pub(crate) mod block;
pub(crate) mod bloom;
pub(crate) mod filter_block;
pub(crate) mod index_block;
pub(crate) mod meta;
pub(crate) mod table;

use std::cmp::Reverse;
use std::sync::Arc;

// FilterPolicy is an algorithm for probabilistically encoding a set of keys.
// It is used to create a filter block that can be stored in a block-based
// file format.
pub trait FilterPolicy: Send + Sync {
    fn name(&self) -> &str;
    fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool;
    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8>;
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InternalKeyKind {
    Delete = 0,
    Set = 1,
    Merge = 2,
    LogData = 3,
    RangeDelete = 4,
    Separator = 5,
    Max = 6,
    Invalid = 7,
}

impl From<u8> for InternalKeyKind {
    fn from(value: u8) -> Self {
        match value {
            0 => InternalKeyKind::Delete,
            1 => InternalKeyKind::Set,
            2 => InternalKeyKind::Merge,
            3 => InternalKeyKind::LogData,
            4 => InternalKeyKind::RangeDelete,
            5 => InternalKeyKind::Separator,
            6 => InternalKeyKind::Max,
            7 => InternalKeyKind::Invalid,
            _ => InternalKeyKind::Invalid, // Default to Invalid for unknown values
        }
    }
}

// This is the maximum valid sequence number that can be stored in the upper 56 bits of a 64-bit integer.
// 1 << 56 shifts the number 1 left by 56 bits, resulting in a binary number with a 1 followed by 56 zeros.
// Subtracting 1 gives a binary number with 56 ones, which is the maximum value for 56 bits.
pub(crate) const INTERNAL_KEY_SEQ_NUM_MAX: u64 = (1 << 56) - 1;
// This is a bit that is set on batch sequence numbers which prevents those entries from being excluded from iteration.
pub(crate) const INTERNAL_KEY_SEQ_NUM_BATCH: u64 = 1 << 55;

#[derive(Debug, Clone, PartialEq, Eq)]
// InternalKey is a key used for the in-memory and on-disk partial DBs that
// make up a pebble DB.
//
// <user-key>.<kind>.<seq-num>
//
// It consists of the user key followed by 8-bytes of metadata:
//   - 1 byte for the type of internal key: delete or set,
//   - 7 bytes for a uint56 sequence number, in big-endian format.
pub struct InternalKey {
    pub(crate) user_key: Arc<[u8]>,
    pub(crate) trailer: u64,
}

impl InternalKey {
    pub fn new(user_key: Vec<u8>, seq_num: u64, kind: InternalKeyKind) -> Self {
        Self {
            user_key: Arc::from(user_key.into_boxed_slice()),
            trailer: (seq_num << 8) | kind as u64,
        }
    }

    // Calculates the size of the InternalKey in bytes.
    pub fn size(&self) -> usize {
        let fixed_size = std::mem::size_of::<u64>() + std::mem::size_of::<Arc<[u8]>>();
        let heap_allocated_size = self.user_key.len();
        fixed_size + heap_allocated_size
    }

    pub fn make_trailer(seq_num: u64, kind: InternalKeyKind) -> u64 {
        (seq_num << 8) | kind as u64
    }

    #[inline]
    pub fn decode(encoded_key: &[u8]) -> Self {
        let n = encoded_key.len() - 8;
        let trailer = u64::from_be_bytes(encoded_key[n..].try_into().unwrap());
        let user_key = Arc::<[u8]>::from(&encoded_key[..n]);
        Self { user_key, trailer }
    }

    pub fn internal_compare(
        a: &Self,
        b: &Self,
        user_cmp: fn(&[u8], &[u8]) -> std::cmp::Ordering,
    ) -> std::cmp::Ordering {
        match user_cmp(&a.user_key, &b.user_key) {
            std::cmp::Ordering::Equal => b.trailer.cmp(&a.trailer),
            other => other,
        }
    }

    #[inline]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = self.user_key.as_ref().to_vec();
        buf.extend_from_slice(&self.trailer.to_be_bytes());
        buf
    }

    pub fn encode_trailer(&self) -> [u8; 8] {
        self.trailer.to_be_bytes()
    }

    // Sets the sequence number component of the key.
    pub fn set_seq_num(&mut self, seq_num: u64) {
        self.trailer = (seq_num << 8) | (self.trailer & 0xff);
    }

    // Returns the sequence number component of the key.
    pub fn seq_num(&self) -> u64 {
        self.trailer >> 8
    }

    // Checks if the key is visible given a snapshot and a batch snapshot.
    pub fn visible(&self, snapshot: u64, batch_snapshot: u64) -> bool {
        let seq_num = self.seq_num();
        Self::visible_internal(seq_num, snapshot, batch_snapshot)
    }

    fn visible_internal(seq_num: u64, snapshot: u64, batch_snapshot: u64) -> bool {
        seq_num < snapshot
            || ((seq_num & INTERNAL_KEY_SEQ_NUM_BATCH) != 0 && seq_num < batch_snapshot)
            || seq_num == INTERNAL_KEY_SEQ_NUM_MAX
    }

    pub fn kind(&self) -> InternalKeyKind {
        let kind_byte = self.trailer as u8; // Extract the last byte
        match kind_byte {
            0 => InternalKeyKind::Delete,
            1 => InternalKeyKind::Set,
            2 => InternalKeyKind::Merge,
            3 => InternalKeyKind::LogData,
            4 => InternalKeyKind::RangeDelete,
            5 => InternalKeyKind::Separator,
            6 => InternalKeyKind::Max,
            _ => InternalKeyKind::Invalid,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        let kind = self.kind();
        if kind == InternalKeyKind::Delete || kind == InternalKeyKind::RangeDelete {
            return true;
        }

        false
    }
}

// Compares two internal keys. For equal user keys, internal keys compare in
// descending sequence number order. For equal user keys and sequence numbers,
// internal keys compare in descending kind order.
// Reverse order is used for trailer (seq_num) comparison.
impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.user_key, Reverse(self.seq_num())).cmp(&(&other.user_key, Reverse(other.seq_num())))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
