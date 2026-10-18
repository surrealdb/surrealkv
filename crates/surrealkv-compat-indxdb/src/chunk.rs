//! The size of one read of a store.
//!
//! An IndexedDB transaction commits by itself when control leaves for something that is not one of
//! its own requests, so a store that is read for longer than a few awaits is read in pieces, each
//! in a transaction of its own, and each piece continues after the last key of the one before. This
//! is the rule for how much one piece holds. It needs no JavaScript, so it is here for the reader
//! of the browser and for anything that stands in for one.

/// The default most bytes of keys and values in one chunk, a chunk of one entry that is larger
/// than this aside.
pub const DEFAULT_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// The default most entries in one chunk.
pub const DEFAULT_CHUNK_ENTRIES: usize = 10_000;

/// How much one chunk may hold.
///
/// An entry weighs the length of its key and of its value. A chunk holds entries while their weight
/// is at most `max_bytes` and there are at most `max_entries` of them, except that the first entry
/// of a chunk is always taken: an entry that weighs more than `max_bytes` is a chunk of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkLimits {
	pub max_bytes: usize,
	pub max_entries: usize,
}

impl Default for ChunkLimits {
	fn default() -> Self {
		Self {
			max_bytes: DEFAULT_CHUNK_BYTES,
			max_entries: DEFAULT_CHUNK_ENTRIES,
		}
	}
}

impl ChunkLimits {
	pub const fn new(max_bytes: usize, max_entries: usize) -> Self {
		Self {
			max_bytes,
			max_entries,
		}
	}
}

/// One chunk of a store.
#[derive(Debug)]
pub struct Chunk<C> {
	/// The entries, in the order of the store, as bytes.
	pub entries: Vec<(Vec<u8>, Vec<u8>)>,
	/// The weight of the entries: the lengths of their keys and of their values.
	pub bytes: usize,
	/// Where the next chunk begins after: the last entry of this one, in whatever form the reader
	/// can continue from. `None` if there is no entry.
	pub last: Option<C>,
	/// Whether the end of the store was reached while this chunk was read. A chunk that ended
	/// because it was full is not done, even if no entry follows it: the chunk after it is empty
	/// and is.
	pub done: bool,
}

/// What a reader has put in the chunk that it is reading, and what the chunk may still take.
#[derive(Clone, Copy, Debug)]
pub struct ChunkFill {
	limits: ChunkLimits,
	entries: usize,
	bytes: usize,
}

impl ChunkFill {
	pub fn new(limits: ChunkLimits) -> Self {
		Self {
			limits,
			entries: 0,
			bytes: 0,
		}
	}

	/// Whether an entry of `weight` bytes belongs in the chunk. The first one always does. One
	/// that does not is not taken, and begins the next chunk.
	pub fn admits(&self, weight: usize) -> bool {
		self.entries == 0
			|| (self.entries < self.limits.max_entries
				&& self
					.bytes
					.checked_add(weight)
					.is_some_and(|total| total <= self.limits.max_bytes))
	}

	/// Puts an entry of `weight` bytes in the chunk.
	pub fn take(&mut self, weight: usize) {
		self.entries += 1;
		self.bytes = self.bytes.saturating_add(weight);
	}

	/// Whether no entry can follow the ones that were taken: the reader stops without looking at
	/// the next one.
	pub fn is_full(&self) -> bool {
		self.entries >= self.limits.max_entries || self.bytes >= self.limits.max_bytes
	}

	pub fn entries(&self) -> usize {
		self.entries
	}

	pub fn bytes(&self) -> usize {
		self.bytes
	}
}
