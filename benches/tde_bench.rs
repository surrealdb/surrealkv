//! Throughput of sealing and opening one block, and the per-file cost of starting an encrypted
//! file, for each cipher suite.
//!
//! Run with `cargo bench --bench tde_bench`. The block sizes are those of an SSTable data block
//! and of a small value-log entry. Sealing uses a new, higher position for every block, as a
//! writer does, because a cipher refuses to seal at a position it has passed.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use surrealkv::tde::{
	BlockContext,
	BlockKind,
	CipherSuite,
	DbId,
	EncryptionContext,
	FileKind,
	SoftwareKeyManager,
};

const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];
const BLOCK_SIZES: [usize; 2] = [256, 64 * 1024];

fn context(suite: CipherSuite) -> EncryptionContext {
	let key_manager = Arc::new(SoftwareKeyManager::new([0x42; 32]));
	EncryptionContext::new(key_manager, DbId::from_bytes([0x24; 16]), suite)
}

fn seal(c: &mut Criterion) {
	let mut group = c.benchmark_group("tde/seal");
	for suite in SUITES {
		for size in BLOCK_SIZES {
			let (_, cipher) = context(suite).create_file(FileKind::Sst, 1, b"").unwrap();
			let mut block = vec![0xA5u8; size];
			let mut position = 0u64;
			group.throughput(Throughput::Bytes(size as u64));
			group.bench_function(BenchmarkId::new(format!("{suite:?}"), size), |b| {
				b.iter(|| {
					position += size as u64;
					let context = BlockContext::new(BlockKind::SstData, position, 0);
					cipher.seal_detached(&context, black_box(&mut block)).unwrap()
				})
			});
		}
	}
	group.finish();
}

fn open(c: &mut Criterion) {
	let mut group = c.benchmark_group("tde/open");
	for suite in SUITES {
		for size in BLOCK_SIZES {
			let ctx = context(suite);
			let (header, writer) = ctx.create_file(FileKind::Sst, 1, b"").unwrap();
			let reader = ctx.open_file(&header, FileKind::Sst, 1, b"").unwrap();
			let at = BlockContext::new(BlockKind::SstData, 4096, 0);
			let mut sealed = vec![0xA5u8; size];
			writer.seal_append(&at, &mut sealed).unwrap();

			group.throughput(Throughput::Bytes(size as u64));
			group.bench_function(BenchmarkId::new(format!("{suite:?}"), size), |b| {
				// Opening decrypts in place, so every iteration gets a fresh copy of the block.
				b.iter_batched(
					|| sealed.clone(),
					|mut copy| {
						let opened = reader.open_suffix(&at, black_box(&mut copy)).unwrap();
						black_box(opened.len())
					},
					BatchSize::SmallInput,
				)
			});
		}
	}
	group.finish();
}

fn start_file(c: &mut Criterion) {
	let mut group = c.benchmark_group("tde/start_file");
	for suite in SUITES {
		let ctx = context(suite);
		group.bench_function(BenchmarkId::new("create", format!("{suite:?}")), |b| {
			b.iter(|| ctx.create_file(FileKind::Sst, black_box(1), b"").unwrap())
		});

		let (header, _) = ctx.create_file(FileKind::Sst, 1, b"").unwrap();
		group.bench_function(BenchmarkId::new("open", format!("{suite:?}")), |b| {
			b.iter(|| ctx.open_file(black_box(&header), FileKind::Sst, 1, b"").unwrap())
		});
	}
	group.finish();
}

criterion_group!(benches, seal, open, start_file);
criterion_main!(benches);
