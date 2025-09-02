use crate::{
	sstable::{meta::KeyRange, table::Table},
	Result,
};
use byteorder::{BigEndian, WriteBytesExt};
use std::{io::Write, sync::Arc};

/// Represents a single level in the LSM tree.
/// Each level contains a sorted collection of SSTables.
#[derive(Clone)]
pub struct Level {
	/// Vector of tables in this level, sorted by sequence numbers in descending order
	pub(crate) tables: Vec<Arc<Table>>,
	/// Maximum capacity before triggering compaction (configurable)
	max_capacity: usize,
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
			max_capacity: DEFAULT_CAPACITY,
		}
	}
}

impl Level {
	/// Creates a new Level with a specified maximum capacity
	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			tables: Vec::with_capacity(capacity),
			max_capacity: capacity,
		}
	}

	/// Inserts a new table into the level and maintains sorted order
	/// Tables are sorted by sequence numbers in descending order
	pub fn insert(&mut self, table: Arc<Table>) {
		let insert_pos = self
			.tables
			.partition_point(|x| x.meta.properties.seqnos.1 > table.meta.properties.seqnos.1);
		self.tables.insert(insert_pos, table);
	}

	/// Removes a table by its ID and maintains sorted order
	pub fn remove(&mut self, table_id: u64) -> bool {
		let len_before = self.tables.len();
		self.tables.retain(|table| table.id != table_id);
		len_before > self.tables.len()
	}

	/// Returns true if the level contains no tables
	pub fn is_empty(&self) -> bool {
		self.tables.is_empty()
	}

	/// Returns true if the level has reached its maximum capacity
	pub fn is_full(&self) -> bool {
		self.tables.len() >= self.max_capacity
	}

	/// Returns an iterator over tables that overlap with the given key range
	pub(crate) fn overlapping_tables<'a>(
		&'a self,
		key_range: &'a KeyRange,
	) -> impl Iterator<Item = &'a Arc<Table>> + 'a {
		self.tables.iter().filter(move |table| table.overlaps_with_range(key_range))
	}
}

/// Represents all levels in the LSM tree
#[derive(Clone)]
pub struct Levels(pub Vec<Arc<Level>>);

impl Levels {
	/// Creates a new Levels structure with specified number of levels and capacity per level
	pub fn new(level_count: usize, capacity_per_level: usize) -> Self {
		Self((0..level_count).map(|_| Arc::new(Level::with_capacity(capacity_per_level))).collect())
	}

	/// Encodes the levels structure to a writer in a binary format
	/// Format:
	/// - Number of levels (u8)
	/// - For each level:
	///   - Number of tables (u32, BigEndian)
	///   - For each table:
	///     - Table ID (u64, BigEndian)
	pub fn encode<W: Write>(&self, writer: &mut W) -> Result<()> {
		writer.write_u8(self.0.len() as u8)?;

		for level in &self.0 {
			writer.write_u32::<BigEndian>(level.tables.len() as u32)?;

			for table in &level.tables {
				writer.write_u64::<BigEndian>(table.id)?;
			}
		}

		Ok(())
	}

	/// Returns a reference to all levels
	pub fn get_levels(&self) -> &Vec<Arc<Level>> {
		&self.0
	}

	/// Returns a mutable reference to all levels
	pub fn get_levels_mut(&mut self) -> &mut Vec<Arc<Level>> {
		&mut self.0
	}

	/// Returns a mutable reference to the first level if it exists
	pub fn get_first_level_mut(&mut self) -> Option<&mut Arc<Level>> {
		self.0.first_mut()
	}

	/// Returns the total number of tables across all levels
	pub fn total_tables(&self) -> usize {
		self.0.iter().map(|level| level.tables.len()).sum()
	}
}

impl IntoIterator for Levels {
	type Item = Arc<Level>;
	type IntoIter = std::vec::IntoIter<Arc<Level>>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl<'a> IntoIterator for &'a Levels {
	type Item = &'a Arc<Level>;
	type IntoIter = std::slice::Iter<'a, Arc<Level>>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.iter()
	}
}

impl AsRef<Vec<Arc<Level>>> for Levels {
	fn as_ref(&self) -> &Vec<Arc<Level>> {
		&self.0
	}
}
