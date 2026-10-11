//! Tests that the values of a value log in the block cache are found only by the log that read
//! them.
//!
//! The log numbers its files from 1, so two logs have files of the same ids and, for values of the
//! same sizes, values at the same offsets. A clone of an `Options` shares its block cache, so two
//! trees that are given clones of one `Options` share one.

use std::sync::Arc;

use tempfile::TempDir;

use crate::vlog::VLog;
use crate::{Options, VLogChecksumLevel};

/// A value log in a directory of its own, with the options `base`.
fn log_in(base: &Options) -> (VLog, TempDir, Arc<Options>) {
	let dir = TempDir::new().unwrap();
	let mut opts = base.clone();
	opts.path = dir.path().to_path_buf();
	std::fs::create_dir_all(opts.vlog_dir()).unwrap();
	let opts = Arc::new(opts);
	let log = VLog::new(Arc::clone(&opts)).unwrap();
	(log, dir, opts)
}

#[tokio::test]
async fn value_logs_in_one_cache_each_read_their_own_values() {
	// Two logs that are given clones of the same options have the same file ids and, for values
	// of the same size, the same offsets, and one block cache.
	let base = Options {
		vlog_checksum_verification: VLogChecksumLevel::Full,
		..Default::default()
	};
	let (log_a, _dir_a, opts_a) = log_in(&base);
	let (log_b, _dir_b, opts_b) = log_in(&base);
	assert!(Arc::ptr_eq(&opts_a.block_cache, &opts_b.block_cache));

	let key = b"the-same-key";
	let pointer_a = log_a.append(key, &[b'A'; 300]).unwrap();
	let pointer_b = log_b.append(key, &[b'B'; 300]).unwrap();
	log_a.sync().unwrap();
	log_b.sync().unwrap();
	assert_eq!(
		(pointer_a.file_id, pointer_a.offset, pointer_a.key_size, pointer_a.value_size),
		(pointer_b.file_id, pointer_b.offset, pointer_b.key_size, pointer_b.value_size),
		"the values are not where the test needs them to be"
	);

	assert_ne!(log_a.cache_id(pointer_a.file_id), log_b.cache_id(pointer_b.file_id));
	assert_eq!(log_a.get(&pointer_a).unwrap(), vec![b'A'; 300]);
	assert_eq!(log_b.get(&pointer_b).unwrap(), vec![b'B'; 300]);
	// Read again, from the cache.
	assert_eq!(log_a.get(&pointer_a).unwrap(), vec![b'A'; 300]);
	assert_eq!(log_b.get(&pointer_b).unwrap(), vec![b'B'; 300]);
}
