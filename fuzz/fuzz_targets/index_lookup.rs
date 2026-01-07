#![no_main]
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::block::BlockHandle;
use surrealkv::sstable::index_block::{BlockHandleWithKey, TopLevelIndex};
use surrealkv::sstable::InternalKeyKind;
use surrealkv::Options;

#[path = "mod.rs"]
mod helpers;
use helpers::make_internal_key;

#[derive(Arbitrary, Debug)]
struct FuzzLookupInput {
	partitions: Vec<FuzzPartition>,
	queries: Vec<FuzzQuery>,
}

#[derive(Arbitrary, Debug, Clone)]
struct FuzzPartition {
	separator_key: Vec<u8>,
	seq_num: u64,
}

#[derive(Arbitrary, Debug)]
struct FuzzQuery {
	user_key: Vec<u8>,
	seq_num: u64,
}

fuzz_target!(|data: FuzzLookupInput| {
	// Limit input size
	if data.partitions.len() > 1000 || data.queries.len() > 100 {
		return;
	}

	// Filter out empty separator keys - the implementation skips them
	let mut partitions: Vec<_> =
		data.partitions.into_iter().filter(|p| !p.separator_key.is_empty()).collect();

	if partitions.is_empty() {
		return;
	}

	let user_cmp = Arc::new(BytewiseComparator::default());
	let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp.clone()));

	// Sort partitions by separator key (using same kind we'll use everywhere)
	partitions.sort_by(|a, b| {
		let key_a = make_internal_key(&a.separator_key, a.seq_num, InternalKeyKind::Separator);
		let key_b = make_internal_key(&b.separator_key, b.seq_num, InternalKeyKind::Separator);
		Comparator::compare(internal_cmp.as_ref(), &key_a, &key_b)
	});

	// Deduplicate partitions with same separator
	partitions.dedup_by(|a, b| {
		let key_a = make_internal_key(&a.separator_key, a.seq_num, InternalKeyKind::Separator);
		let key_b = make_internal_key(&b.separator_key, b.seq_num, InternalKeyKind::Separator);
		Comparator::compare(internal_cmp.as_ref(), &key_a, &key_b) == std::cmp::Ordering::Equal
	});

	if partitions.is_empty() {
		return;
	}

	// Create TopLevelIndex with fuzzed partitions
	let opts = Arc::new(Options::default());
	let file: Arc<dyn surrealkv::vfs::File> = Arc::new(Vec::new());

	// Build separator keys once for verification
	let separator_keys: Vec<Vec<u8>> = partitions
		.iter()
		.map(|p| make_internal_key(&p.separator_key, p.seq_num, InternalKeyKind::Separator))
		.collect();

	let blocks: Vec<BlockHandleWithKey> = separator_keys
		.iter()
		.enumerate()
		.map(|(i, sep_key)| BlockHandleWithKey {
			separator_key: sep_key.clone(),
			handle: BlockHandle::new(i * 100, 100),
		})
		.collect();

	let index = TopLevelIndex {
		id: 0,
		opts: opts.clone(),
		blocks,
		file,
	};

	// Test queries with actual invariant verification
	for query in &data.queries {
		// Use same kind as partitions for consistent comparison
		let query_key =
			make_internal_key(&query.user_key, query.seq_num, InternalKeyKind::Separator);
		let result = index.find_block_handle_by_key(&query_key).unwrap();

		match result {
			Some((idx, block)) => {
				// INVARIANT 1: returned index must be valid
				assert!(idx < partitions.len(), "Index {} out of bounds", idx);

				// INVARIANT 2: target <= separator at idx
				let cmp =
					Comparator::compare(internal_cmp.as_ref(), &query_key, &block.separator_key);
				assert!(
					cmp != std::cmp::Ordering::Greater,
					"query_key should be <= separator at idx {}",
					idx
				);

				// INVARIANT 3: all partitions before idx have separator < target
				for i in 0..idx {
					let sep_cmp =
						Comparator::compare(internal_cmp.as_ref(), &separator_keys[i], &query_key);
					assert!(
						sep_cmp == std::cmp::Ordering::Less,
						"separator[{}] should be < query_key",
						i
					);
				}
			}
			None => {
				// INVARIANT 4: target > last separator (query is beyond all partitions)
				let last_sep = &separator_keys[separator_keys.len() - 1];
				let cmp = Comparator::compare(internal_cmp.as_ref(), &query_key, last_sep);
				assert!(
					cmp == std::cmp::Ordering::Greater,
					"query_key should be > last separator when result is None"
				);
			}
		}
	}

	// Verify each separator key finds its own partition
	for (i, sep_key) in separator_keys.iter().enumerate() {
		let result = index.find_block_handle_by_key(sep_key).unwrap();
		assert!(result.is_some(), "Separator key {} should find a partition", i);
		let (idx, _) = result.unwrap();
		assert_eq!(idx, i, "Separator key {} should find partition {}", i, i);
	}
});
