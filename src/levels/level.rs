use crate::sstable::meta::KeyRange;
use crate::sstable::table::Table;
use crate::sstable::InternalKeyTrait;
use crate::Result;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use std::sync::Arc;

/// Represents a single level in the LSM tree.
/// Each level contains a sorted collection of SSTables.
#[derive(Clone)]
pub(crate) struct Level<K: InternalKeyTrait> {
	/// Vector of tables in this level, sorted by sequence numbers in descending
	/// order
	pub(crate) tables: Vec<Arc<Table<K>>>,
}

impl<K: InternalKeyTrait> std::ops::Deref for Level<K> {
	type Target = Vec<Arc<Table<K>>>;

	fn deref(&self) -> &Self::Target {
		&self.tables
	}
}

impl<K: InternalKeyTrait> Default for Level<K> {
	fn default() -> Self {
		const DEFAULT_CAPACITY: usize = 10;
		Self {
			tables: Vec::with_capacity(DEFAULT_CAPACITY),
		}
	}
}

impl<K: InternalKeyTrait> Level<K> {
	/// Creates a new Level with a specified maximum capacity
	#[allow(unused)]
	pub(crate) fn with_capacity(capacity: usize) -> Self {
		Self {
			tables: Vec::with_capacity(capacity),
		}
	}

	/// Inserts a new table into the level and maintains sorted order
	/// Tables are sorted by sequence numbers in descending order
	pub(crate) fn insert(&mut self, table: Arc<Table<K>>) {
		let insert_pos = self
			.tables
			.partition_point(|x| x.meta.properties.seqnos.1 > table.meta.properties.seqnos.1);
		self.tables.insert(insert_pos, table);
	}

	/// Removes a table by its ID and maintains sorted order
	pub(crate) fn remove(&mut self, table_id: u64) -> bool {
		let len_before = self.tables.len();
		self.tables.retain(|table| table.id != table_id);
		len_before > self.tables.len()
	}

	/// Returns an iterator over tables that overlap with the given key range
	pub(crate) fn overlapping_tables<'a>(
		&'a self,
		key_range: &'a KeyRange,
	) -> impl Iterator<Item = &'a Arc<Table<K>>> + 'a {
		self.tables.iter().filter(move |table| table.overlaps_with_range(key_range))
	}
}

/// Represents all levels in the LSM tree
#[derive(Clone)]
pub(crate) struct Levels<K: InternalKeyTrait>(pub(crate) Vec<Arc<Level<K>>>);

impl<K: InternalKeyTrait> Levels<K> {
	/// Creates a new Levels structure with specified number of levels and
	/// capacity per level
	#[allow(unused)]
	pub(crate) fn new(level_count: usize, capacity_per_level: usize) -> Self {
		Self((0..level_count).map(|_| Arc::new(Level::with_capacity(capacity_per_level))).collect())
	}

	/// Encodes the levels structure to a writer in a binary format
	/// Format:
	/// - Number of levels (u8)
	/// - For each level:
	///   - Number of tables (u32, BigEndian)
	///   - For each table:
	///     - Table ID (u64, BigEndian)
	pub(crate) fn encode<W: Write>(&self, writer: &mut W) -> Result<()> {
		writer.write_u8(self.0.len() as u8)?;

		for level in &self.0 {
			writer.write_u32::<BigEndian>(level.tables.len() as u32)?;

			for table in &level.tables {
				writer.write_u64::<BigEndian>(table.id)?;
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
		let level_count = reader.read_u8()?;
		let mut levels = Vec::with_capacity(level_count as usize);

		for _ in 0..level_count {
			let table_count = reader.read_u32::<BigEndian>()?;
			let mut level = Vec::with_capacity(table_count as usize);

			for _ in 0..table_count {
				let table_id = reader.read_u64::<BigEndian>()?;
				level.push(table_id);
			}

			levels.push(level);
		}

		Ok(levels)
	}

	/// Returns a reference to all levels
	pub(crate) fn get_levels(&self) -> &Vec<Arc<Level<K>>> {
		&self.0
	}

	/// Returns a mutable reference to all levels
	pub(crate) fn get_levels_mut(&mut self) -> &mut Vec<Arc<Level<K>>> {
		&mut self.0
	}
}

impl<K: InternalKeyTrait> IntoIterator for Levels<K> {
	type IntoIter = std::vec::IntoIter<Arc<Level<K>>>;
	type Item = Arc<Level<K>>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl<'a, K: InternalKeyTrait> IntoIterator for &'a Levels<K> {
	type IntoIter = std::slice::Iter<'a, Arc<Level<K>>>;
	type Item = &'a Arc<Level<K>>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.iter()
	}
}

impl<K: InternalKeyTrait> AsRef<Vec<Arc<Level<K>>>> for Levels<K> {
	fn as_ref(&self) -> &Vec<Arc<Level<K>>> {
		&self.0
	}
}
