use crate::CompressionType;

/// CompressionSelector handles level-aware compression type selection.
#[derive(Debug, Clone)]
pub struct CompressionSelector {
	per_level_compression: Vec<CompressionType>,
}

impl CompressionSelector {
	/// Create a new CompressionSelector with per-level compression settings.
	pub fn new(per_level: Vec<CompressionType>) -> Self {
		Self {
			per_level_compression: per_level,
		}
	}

	/// Select compression type for a given level.
	///
	/// # Arguments
	/// * `level` - The level number (0 = L0, 1 = L1, etc.)
	///
	/// # Returns
	/// The compression type to use for the given level
	pub fn select_compression(&self, level: u8) -> CompressionType {
		if self.per_level_compression.is_empty() {
			return CompressionType::None;
		}

		let idx = level as usize;
		if idx < self.per_level_compression.len() {
			self.per_level_compression[idx]
		} else {
			// Use last configured compression for higher levels
			*self.per_level_compression.last().unwrap_or(&CompressionType::None)
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_empty_compression_defaults_to_none() {
		let selector = CompressionSelector::new(Vec::new());

		assert_eq!(selector.select_compression(0), CompressionType::None);
		assert_eq!(selector.select_compression(1), CompressionType::None);
		assert_eq!(selector.select_compression(5), CompressionType::None);
	}

	#[test]
	fn test_per_level_compression() {
		let selector = CompressionSelector::new(vec![
			CompressionType::None,
			CompressionType::SnappyCompression,
		]);

		// L0 uses first element
		assert_eq!(selector.select_compression(0), CompressionType::None);
		// L1 uses second element
		assert_eq!(selector.select_compression(1), CompressionType::SnappyCompression);
		// Higher levels use last element
		assert_eq!(selector.select_compression(2), CompressionType::SnappyCompression);
		assert_eq!(selector.select_compression(5), CompressionType::SnappyCompression);
	}

	#[test]
	fn test_per_level_with_more_levels() {
		let selector = CompressionSelector::new(vec![
			CompressionType::None,              // L0
			CompressionType::SnappyCompression, // L1
			CompressionType::SnappyCompression, // L2
		]);

		assert_eq!(selector.select_compression(0), CompressionType::None);
		assert_eq!(selector.select_compression(1), CompressionType::SnappyCompression);
		assert_eq!(selector.select_compression(2), CompressionType::SnappyCompression);
		// Higher levels use last configured (Snappy)
		assert_eq!(selector.select_compression(3), CompressionType::SnappyCompression);
		assert_eq!(selector.select_compression(10), CompressionType::SnappyCompression);
	}
}
