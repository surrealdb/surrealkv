use std::io::{Read, Write};
use std::sync::Arc;

use crate::sstable::table::Table;
use crate::{InternalKeyRange, Result};

/// Represents a single level in the LSM tree.
/// Each level contains a sorted collection of SSTables.
#[derive(Clone)]
pub(crate) struct Level {
	/// Vector of tables in this level, sorted by sequence numbers in descending
	/// order
	pub(crate) tables: Vec<Arc<Table>>,
}

impl std::ops::Deref for Level {
	type Target = Vec<Arc<Table>>;

	fn deref(&self) -> &Self::Target {
		&self.tables
	}
}

impl Default for Level {
	fn default() -> Self {
		const DEFAULT_CAPACITY: usize = 10;
		Self {
			tables: Vec::with_capacity(DEFAULT_CAPACITY),
		}
	}
}

impl Level {
	/// Creates a new Level with a specified maximum capacity
	#[allow(unused)]
	pub(crate) fn with_capacity(capacity: usize) -> Self {
		Self {
			tables: Vec::with_capacity(capacity),
		}
	}

	/// Inserts a new table into the level and maintains sorted order
	/// Tables are sorted by sequence numbers in descending order
	/// Using for Level 0 where tables can overlap
	pub(crate) fn insert(&mut self, table: Arc<Table>) {
		let insert_pos = self
			.tables
			.partition_point(|x| x.meta.properties.seqnos.1 > table.meta.properties.seqnos.1);
		self.tables.insert(insert_pos, table);
	}

	/// Inserts a new table sorted by smallest key (ascending)
	/// Tables cannot overlap, enables O(log n) binary search for range queries
	/// Using for Level 1+ where tables have non-overlapping key ranges
	pub(crate) fn insert_sorted_by_key(&mut self, table: Arc<Table>) {
		let insert_pos = self.tables.partition_point(|existing| {
			match (&existing.meta.smallest_point, &table.meta.smallest_point) {
				(Some(existing_smallest), Some(new_smallest)) => {
					existing_smallest.user_key < new_smallest.user_key
				}
				_ => true,
			}
		});
		self.tables.insert(insert_pos, table);
	}

	/// Removes a table by its ID and maintains sorted order
	pub(crate) fn remove(&mut self, table_id: u64) -> bool {
		let len_before = self.tables.len();
		self.tables.retain(|table| table.id != table_id);
		len_before > self.tables.len()
	}

	/// Returns an iterator over tables that overlap with the given range
	pub(crate) fn overlapping_tables<'a>(
		&'a self,
		range: &'a InternalKeyRange,
	) -> impl Iterator<Item = &'a Arc<Table>> + 'a {
		self.tables.iter().filter(move |table| table.overlaps_with_range(range))
	}

	/// Finds the index of the first table that could potentially overlap with
	/// the given range. For Level 1+, tables have non-overlapping key ranges
	/// sorted by their keys. Returns the index of the first table to check, or
	/// tables.len() if all tables are before the range.
	pub(crate) fn find_first_overlapping_table(&self, range: &InternalKeyRange) -> usize {
		// Binary search to find first table that is NOT completely before the range
		self.tables.partition_point(|table| table.is_before_range(range))
	}

	/// Finds the index after the last table that could potentially overlap with
	/// the given range. Returns the exclusive end index for iteration.
	pub(crate) fn find_last_overlapping_table(&self, range: &InternalKeyRange) -> usize {
		// Find the first table that is completely after the range
		self.tables.partition_point(|table| !table.is_after_range(range))
	}

	/// Finds the single table that could contain the given user key in this level.
	/// For Level 1+, tables have non-overlapping sorted key ranges.
	pub(crate) fn find_table_for_user_key(
		&self,
		key: &[u8],
		comparator: &Arc<dyn crate::comparator::Comparator>,
	) -> Option<&Arc<Table>> {
		if self.tables.is_empty() {
			return None;
		}
		let idx = self.tables.partition_point(|table| {
			if let Some(ref largest) = table.meta.largest_point {
				comparator.compare(largest.user_key.as_slice(), key) == std::cmp::Ordering::Less
			} else {
				false
			}
		});
		if idx < self.tables.len() {
			let table = &self.tables[idx];
			if let Some(ref smallest) = table.meta.smallest_point {
				if comparator.compare(key, smallest.user_key.as_slice()) != std::cmp::Ordering::Less {
					return Some(table);
				}
			} else {
				return Some(table);
			}
		}
		None
	}
}

/// Represents all levels in the LSM tree
#[derive(Clone)]
pub(crate) struct Levels(pub(crate) Vec<Arc<Level>>);

impl Levels {
	/// Creates a new Levels structure with specified number of levels and
	/// capacity per level
	#[allow(unused)]
	pub(crate) fn new(level_count: usize, capacity_per_level: usize) -> Self {
		Self((0..level_count).map(|_| Arc::new(Level::with_capacity(capacity_per_level))).collect())
	}

	pub(crate) fn total_tables(&self) -> usize {
		self.0.iter().map(|level| level.tables.len()).sum()
	}

	/// Encodes the levels structure to a writer in a binary format
	/// Format:
	/// - Number of levels (u8)
	/// - For each level:
	///   - Number of tables (u32, BigEndian)
	///   - For each table:
	///     - Table ID (u64, BigEndian)
	pub(crate) fn encode<W: Write>(&self, writer: &mut W) -> Result<()> {
		writer.write_all(&[self.0.len() as u8])?;

		for level in &self.0 {
			writer.write_all(&(level.tables.len() as u32).to_be_bytes())?;

			for table in &level.tables {
				writer.write_all(&table.id.to_be_bytes())?;
			}
		}

		Ok(())
	}

	/// Decodes the levels structure from a reader in a binary format
	/// Format:
	/// - Number of levels (u8)
	/// - For each level:
	///   - Number of tables (u32, BigEndian)
	///   - For each table:
	///     - Table ID (u64, BigEndian)
	///
	/// Returns a vector of vectors containing table IDs for each level
	pub(crate) fn decode<R: Read>(reader: &mut R) -> Result<Vec<Vec<u64>>> {
		let mut len_buf = [0u8; 1];
		reader.read_exact(&mut len_buf)?;
		let level_count = len_buf[0];
		let mut levels = Vec::with_capacity(level_count as usize);

		let mut u32_buf = [0u8; 4];
		let mut u64_buf = [0u8; 8];

		for _ in 0..level_count {
			reader.read_exact(&mut u32_buf)?;
			let table_count = u32::from_be_bytes(u32_buf);
			let mut level = Vec::with_capacity(table_count as usize);

			for _ in 0..table_count {
				reader.read_exact(&mut u64_buf)?;
				let table_id = u64::from_be_bytes(u64_buf);
				level.push(table_id);
			}
			levels.push(level);
		}

		Ok(levels)
	}

	/// Returns a reference to all levels
	pub(crate) fn get_levels(&self) -> &Vec<Arc<Level>> {
		&self.0
	}

	/// Returns a mutable reference to all levels
	pub(crate) fn get_levels_mut(&mut self) -> &mut Vec<Arc<Level>> {
		&mut self.0
	}
}

impl IntoIterator for Levels {
	type IntoIter = std::vec::IntoIter<Arc<Level>>;
	type Item = Arc<Level>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl<'a> IntoIterator for &'a Levels {
	type IntoIter = std::slice::Iter<'a, Arc<Level>>;
	type Item = &'a Arc<Level>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.iter()
	}
}

impl AsRef<Vec<Arc<Level>>> for Levels {
	fn as_ref(&self) -> &Vec<Arc<Level>> {
		&self.0
	}
}
