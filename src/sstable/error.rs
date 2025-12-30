use std::fmt;

/// SSTable-specific error types
#[derive(Debug, Clone)]
pub enum SSTableError {
	// Block errors
	BlockTooSmall {
		size: usize,
		min_size: usize,
	},
	FailedToDecodeRestartCount {
		block_size: usize,
	},
	InvalidRestartCount {
		count: usize,
		block_size: usize,
	},
	RestartPointExceedsBounds {
		index: usize,
		offset: usize,
		block_size: usize,
	},
	FailedToDecodeRestartPoint {
		index: usize,
		offset: usize,
	},
	FailedToDecodeSharedPrefix {
		offset: usize,
	},
	FailedToDecodeNonSharedKeyLength {
		offset: usize,
	},
	FailedToDecodeValueSize {
		offset: usize,
	},
	OffsetExceedsRestartOffset {
		offset: usize,
		restart_offset: usize,
	},
	IntegerOverflowKeyEnd {
		offset: usize,
	},
	IntegerOverflowValueEnd {
		offset: usize,
	},
	DecodedLengthsExceedBounds {
		offset: usize,
		i: usize,
		non_shared_key: usize,
		value_size: usize,
		key_end: usize,
		value_end: usize,
		restart_offset: usize,
	},
	BlockHasNoRestartPoints,
	EmptyCorruptBlockSeek {
		block_size: usize,
		restart_offset: usize,
	},
	KeyExtendsBeyondBounds {
		offset: usize,
		key_end: usize,
		block_len: usize,
	},
	CorruptedBlockHandle,

	// Table/Footer errors
	FileTooSmall {
		file_size: usize,
		min_size: usize,
	},
	BadMagicNumber {
		magic: Vec<u8>,
	},
	FooterTooShort {
		footer_len: usize,
	},
	InvalidChecksumType {
		checksum_type: u8,
	},
	BadMetaIndexBlockHandle,
	ChecksumVerificationFailed {
		block_offset: u64,
	},

	// BlockHandle decoding errors
	FailedToDecodeBlockHandle {
		value_bytes: Vec<u8>,
		context: String,
	},
	EmptyBlock,
	NoValidBlocksForSeek {
		operation: String, // "first" or "last"
	},

	// Metadata errors
	InvalidHasPointKeysValue {
		value: u8,
	},
	InvalidSmallestPointValue {
		value: u8,
	},
	InvalidLargestPointValue {
		value: u8,
	},

	// Index errors
	EmptyCorruptPartitionedIndex {
		table_id: u64,
	},

	// System errors
	FailedToGetSystemTime {
		source: String,
	},
}

impl fmt::Display for SSTableError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			// Block errors
			SSTableError::BlockTooSmall {
				size,
				min_size,
			} => {
				write!(
					f,
					"Block too small to contain restart count: {} bytes (minimum: {})",
					size, min_size
				)
			}
			SSTableError::FailedToDecodeRestartCount {
				block_size,
			} => {
				write!(f, "Failed to decode restart count from block of size {}", block_size)
			}
			SSTableError::InvalidRestartCount {
				count,
				block_size,
			} => {
				write!(f, "Invalid restart count {} for block of size {}", count, block_size)
			}
			SSTableError::RestartPointExceedsBounds {
				index,
				offset,
				block_size,
			} => {
				write!(
					f,
					"Restart point {} at offset {} exceeds block size {}",
					index, offset, block_size
				)
			}
			SSTableError::FailedToDecodeRestartPoint {
				index,
				offset,
			} => {
				write!(f, "Failed to decode restart point {} at offset {}", index, offset)
			}
			SSTableError::FailedToDecodeSharedPrefix {
				offset,
			} => {
				write!(f, "Failed to decode shared prefix length at offset {}", offset)
			}
			SSTableError::FailedToDecodeNonSharedKeyLength {
				offset,
			} => {
				write!(f, "Failed to decode non-shared key length at offset {}", offset)
			}
			SSTableError::FailedToDecodeValueSize {
				offset,
			} => {
				write!(f, "Failed to decode value size at offset {}", offset)
			}
			SSTableError::OffsetExceedsRestartOffset {
				offset,
				restart_offset,
			} => {
				write!(f, "Offset {} exceeds restart_offset {}", offset, restart_offset)
			}
			SSTableError::IntegerOverflowKeyEnd {
				offset,
			} => {
				write!(f, "Integer overflow calculating key_end at offset {}", offset)
			}
			SSTableError::IntegerOverflowValueEnd {
				offset,
			} => {
				write!(f, "Integer overflow calculating value_end at offset {}", offset)
			}
			SSTableError::DecodedLengthsExceedBounds {
				offset,
				i,
				non_shared_key,
				value_size,
				key_end,
				value_end,
				restart_offset,
			} => {
				write!(f, "Decoded lengths exceed block bounds. offset: {}, i: {}, non_shared_key: {}, value_size: {}, key_end: {}, value_end: {}, restart_offset: {}", 
					offset, i, non_shared_key, value_size, key_end, value_end, restart_offset)
			}
			SSTableError::BlockHasNoRestartPoints => {
				write!(f, "Block has no restart points")
			}
			SSTableError::EmptyCorruptBlockSeek {
				block_size,
				restart_offset,
			} => {
				write!(f, "Attempted seek on empty/corrupt block with no restart points. Block size: {}, restart_offset: {}", block_size, restart_offset)
			}
			SSTableError::KeyExtendsBeyondBounds {
				offset,
				key_end,
				block_len,
			} => {
				write!(
					f,
					"Key extends beyond block bounds. offset: {}, key_end: {}, block_len: {}",
					offset, key_end, block_len
				)
			}
			SSTableError::CorruptedBlockHandle => {
				write!(f, "corrupted block handle")
			}

			// Table/Footer errors
			SSTableError::FileTooSmall {
				file_size,
				min_size,
			} => {
				write!(
					f,
					"invalid table (file size is too small: {} bytes, minimum: {})",
					file_size, min_size
				)
			}
			SSTableError::BadMagicNumber {
				magic,
			} => {
				write!(f, "invalid table (bad magic number: {:x?})", magic)
			}
			SSTableError::FooterTooShort {
				footer_len,
			} => {
				write!(f, "invalid table (footer too short): {}", footer_len)
			}
			SSTableError::InvalidChecksumType {
				checksum_type,
			} => {
				write!(f, "Invalid checksum type: {}", checksum_type)
			}
			SSTableError::BadMetaIndexBlockHandle => {
				write!(f, "invalid table (bad meta_index block handle)")
			}
			SSTableError::ChecksumVerificationFailed {
				block_offset,
			} => {
				write!(f, "checksum verification failed for block at {}", block_offset)
			}

			// BlockHandle decoding errors
			SSTableError::FailedToDecodeBlockHandle {
				value_bytes,
				context,
			} => {
				write!(f, "Couldn't decode corrupt blockhandle {:?}: {}", value_bytes, context)
			}
			SSTableError::EmptyBlock => {
				write!(f, "Empty block")
			}
			SSTableError::NoValidBlocksForSeek {
				operation,
			} => {
				write!(f, "Failed to seek to {} entry: no valid blocks", operation)
			}

			// Metadata errors
			SSTableError::InvalidHasPointKeysValue {
				value,
			} => {
				write!(f, "Invalid has_point_keys value: {}", value)
			}
			SSTableError::InvalidSmallestPointValue {
				value,
			} => {
				write!(f, "Invalid smallest_point value: {}", value)
			}
			SSTableError::InvalidLargestPointValue {
				value,
			} => {
				write!(f, "Invalid largest_point value: {}", value)
			}

			// Index errors
			SSTableError::EmptyCorruptPartitionedIndex {
				table_id,
			} => {
				write!(f, "Attempted lookup on empty/corrupt partitioned index with no blocks. Table ID: {}", table_id)
			}

			// System errors
			SSTableError::FailedToGetSystemTime {
				source,
			} => {
				write!(f, "Failed to get system time: {}", source)
			}
		}
	}
}

impl std::error::Error for SSTableError {}
