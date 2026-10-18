use tempfile::TempDir;

use super::*;

#[test]
fn test_dump_export_and_read_roundtrip() {
	let temp_dir = TempDir::new().unwrap();
	let dump_path = temp_dir.path().join("indxdb_dump.bin");

	let original_entries = vec![
		(b"users:0001".to_vec(), b"Alice".to_vec()),
		(b"users:0002".to_vec(), b"Bob".to_vec()),
		(b"config:app".to_vec(), b"{\"version\": 2}".to_vec()),
	];

	export_dump(&dump_path, &original_entries).expect("Export should succeed");
	assert!(is_indxdb_dump_dir(temp_dir.path()));

	let recovered = read_dump(&dump_path).expect("Read dump should succeed");
	assert_eq!(recovered.len(), 3);
	assert_eq!(recovered, original_entries);
}

#[test]
fn test_dump_bad_magic() {
	let temp_dir = TempDir::new().unwrap();
	let dump_path = temp_dir.path().join("bad_dump.bin");

	std::fs::write(&dump_path, b"NOT_INDXDB_DATA").unwrap();
	let res = read_dump(&dump_path);
	assert!(res.is_err());
}

/// Cuts `weights` into chunks the way a reader does, and says how many entries each holds.
fn chunk_sizes(limits: ChunkLimits, weights: &[usize]) -> Vec<usize> {
	let mut sizes = Vec::new();
	let mut at = 0;
	while at < weights.len() {
		let mut fill = ChunkFill::new(limits);
		while at < weights.len() && fill.admits(weights[at]) {
			fill.take(weights[at]);
			at += 1;
			if fill.is_full() {
				break;
			}
		}
		sizes.push(fill.entries());
	}
	sizes
}

#[test]
fn a_chunk_holds_entries_up_to_its_bytes() {
	let limits = ChunkLimits::new(300, 100);
	// Three entries of one hundred bytes are exactly the limit, and a fourth does not fit.
	assert_eq!(chunk_sizes(limits, &[100; 7]), vec![3, 3, 1]);
	// One byte over: the third entry begins the second chunk.
	assert_eq!(chunk_sizes(limits, &[100, 100, 101]), vec![2, 1]);
	// One byte under.
	assert_eq!(chunk_sizes(limits, &[100, 100, 100]), vec![3]);
	assert_eq!(chunk_sizes(limits, &[100, 100, 99, 1]), vec![4]);
	assert_eq!(chunk_sizes(limits, &[100, 100, 99, 2]), vec![3, 1]);
}

#[test]
fn a_chunk_holds_entries_up_to_its_count() {
	let limits = ChunkLimits::new(usize::MAX, 4);
	assert_eq!(chunk_sizes(limits, &[1; 10]), vec![4, 4, 2]);
	assert_eq!(chunk_sizes(limits, &[1; 4]), vec![4]);
	assert_eq!(chunk_sizes(limits, &[1; 5]), vec![4, 1]);
	// An empty store has no chunk that holds anything.
	assert!(chunk_sizes(limits, &[]).is_empty());
}

#[test]
fn an_entry_larger_than_the_limit_is_a_chunk_of_its_own() {
	let limits = ChunkLimits::new(1000, 100);
	assert_eq!(chunk_sizes(limits, &[10, 5000, 10]), vec![1, 1, 1]);
	assert_eq!(chunk_sizes(limits, &[5000]), vec![1]);
	assert_eq!(chunk_sizes(limits, &[5000, 5000]), vec![1, 1]);
	// One that is exactly the limit fills a chunk, and one that is a byte over cannot join it.
	assert_eq!(chunk_sizes(limits, &[1000, 1]), vec![1, 1]);
	assert_eq!(chunk_sizes(limits, &[1001, 1]), vec![1, 1]);
}

#[test]
fn a_chunk_takes_its_first_entry_whatever_the_limits_say() {
	for limits in [ChunkLimits::new(0, 0), ChunkLimits::new(0, 5), ChunkLimits::new(5, 0)] {
		let mut fill = ChunkFill::new(limits);
		assert!(fill.admits(usize::MAX));
		fill.take(usize::MAX);
		assert!(fill.is_full());
		assert!(!fill.admits(0));
		assert_eq!(fill.entries(), 1);
		assert_eq!(fill.bytes(), usize::MAX);
	}
}

#[test]
fn the_defaults_bound_both_ways() {
	let limits = ChunkLimits::default();
	assert_eq!(limits.max_bytes, DEFAULT_CHUNK_BYTES);
	assert_eq!(limits.max_entries, DEFAULT_CHUNK_ENTRIES);
	let mut fill = ChunkFill::new(limits);
	fill.take(DEFAULT_CHUNK_BYTES - 1);
	assert!(!fill.is_full());
	assert!(fill.admits(1));
	assert!(!fill.admits(2));
}
