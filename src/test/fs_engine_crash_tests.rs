//! Crash images of a database in a memory pool, at every step of the work that the engine does to
//! its files.
//!
//! The pool is wrapped in a `ProbeFs` that takes a crash image of the memory underneath it before
//! every call that creates, replaces, removes or syncs a file (and after a manifest is replaced or
//! a file removed), in several ways of crashing. A thread of its own opens a database in each image
//! and checks it, while the workload goes on: every commit that was acknowledged (they are
//! sequential and synced) is there, with its value, the commits that are there are a prefix, and
//! each key is there once. The steps are the ones of the rotation of the log, the flush of a
//! memtable (table created, written, synced, listed in a manifest, the log removed), the
//! compaction (its outputs, its manifest, the removal of its inputs) and the roll of the value log.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use test_log::test;

use super::fs_engine_tests::{key_of, options_in, until, until_compacted, value_of, within};
use super::fs_probe::{Call, ProbeFs, Verdict};
use crate::{CrashMode, Durability, FileSystem, Options, TreeBuilder};

/// The way a database that is a prefix of the commits is read back from a crash.
struct Image {
	fs: FileSystem,
	/// The commits that were acknowledged before the image was taken.
	acked: usize,
	/// What the engine was about to do.
	what: String,
}

/// What the thread that checks the images found.
#[derive(Default)]
struct Findings {
	checked: AtomicUsize,
	failures: Mutex<Vec<String>>,
	/// How many images were taken before a call of each kind, for the coverage of the test.
	kinds: Mutex<std::collections::BTreeMap<String, usize>>,
}

/// Opens the database in `image` and checks it: the acknowledged commits are there, and the ones
/// that are there are a prefix of the commits that were made, each once, with its value.
fn check(image: Image, options: &Options, made: usize) -> Result<(), String> {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|e| e.to_string())?;
	runtime.block_on(async {
		let options = Options {
			fs: Arc::clone(&image.fs.0),
			..options.clone()
		};
		let tree = TreeBuilder::with_options(options)
			.build_async()
			.await
			.map_err(|e| format!("{}: the database does not open: {e}", image.what))?;
		let mut found = Vec::new();
		{
			let txn = tree.begin_with_mode(crate::Mode::ReadOnly).map_err(|e| e.to_string())?;
			let mut iter = txn.range(key_of(0), key_of(made + 100)).map_err(|e| e.to_string())?;
			let all = super::collect_transaction_all(&mut iter).map_err(|e| e.to_string())?;
			found.extend(all);
		}
		// Each key once, in order, with its value, and no key that was never written.
		for (at, (key, value)) in found.iter().enumerate() {
			if *key != key_of(at) {
				return Err(format!(
					"{}: the commits that are there are not a prefix: position {at} holds {}",
					image.what,
					String::from_utf8_lossy(key)
				));
			}
			if *value != value_of(at, 150) {
				return Err(format!("{}: key {at} has the wrong value", image.what));
			}
		}
		if found.len() < image.acked {
			return Err(format!(
				"{}: {} commits were acknowledged and {} are there",
				image.what,
				image.acked,
				found.len()
			));
		}
		tree.close().await.map_err(|e| format!("{}: close: {e}", image.what))?;
		Ok(())
	})
}

fn checker(
	rx: Receiver<Image>,
	options: Options,
	made: usize,
	findings: Arc<Findings>,
) -> std::thread::JoinHandle<()> {
	std::thread::spawn(move || {
		for image in rx {
			let what = image.what.clone();
			let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
				check(image, &options, made)
			}));
			findings.checked.fetch_add(1, Ordering::SeqCst);
			let failure = match outcome {
				Ok(Ok(())) => continue,
				Ok(Err(message)) => message,
				Err(_) => format!("{what}: the check panicked"),
			};
			findings.failures.lock().unwrap().push(failure);
		}
	})
}

fn kind_of(call: &Call) -> &'static str {
	match call {
		Call::CreateDirAll(_) => "create_dir_all",
		Call::RemoveFile(p) if p.extension().is_some_and(|e| e == "sst") => "remove table",
		Call::RemoveFile(p) if p.extension().is_some_and(|e| e == "wal") => "remove segment",
		Call::RemoveFile(_) => "remove other",
		Call::Rename(_) => "rename",
		Call::SyncDir(_) => "sync_dir",
		Call::SyncFile(_) => "sync_file",
		Call::OpenAppend(p) if p.extension().is_some_and(|e| e == "vlog") => "open vlog file",
		Call::OpenAppend(_) => "open segment",
		Call::CreateNew(_) => "create_new",
		Call::CreateTruncate(_, crate::vfs::SpareClass::Opportunistic) => "create output",
		Call::CreateTruncate(..) => "create table",
		Call::ReplaceAtomic(_) => "replace manifest",
	}
}

/// Runs a workload of `commits` synced commits over a pool, takes an image before each step that
/// changes a file and in each of `modes`, and has them all checked.
async fn every_step(vlog: bool, commits: usize, modes: &'static [CrashMode]) -> Arc<Findings> {
	let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
	let mut options = options_in(&fs, 8 * 1024);
	options.enable_vlog = vlog;
	options.vlog_value_threshold = 64;
	options.vlog_max_file_size = 8 * 1024;
	let findings = Arc::new(Findings::default());
	let (tx, rx) = sync_channel::<Image>(2);
	let verifier = checker(rx, options.clone(), commits, Arc::clone(&findings));
	let acked = Arc::new(AtomicUsize::new(0));

	let capture = {
		let probe = Arc::downgrade(&probe);
		let tx = tx.clone();
		let acked = Arc::clone(&acked);
		let findings = Arc::clone(&findings);
		move |call: &Call, when: &str| {
			let Some(probe) = probe.upgrade() else {
				return;
			};
			let at = acked.load(Ordering::SeqCst);
			*findings
				.kinds
				.lock()
				.unwrap()
				.entry(format!("{when} {}", kind_of(call)))
				.or_default() += 1;
			for mode in modes {
				let what = format!(
					"{when} {} of {} ({mode:?}, {at} acknowledged)",
					kind_of(call),
					call.path().display()
				);
				let _ = tx.send(Image {
					fs: probe.crash_image(*mode),
					acked: at,
					what,
				});
			}
		}
	};
	let before = capture.clone();
	probe.before(move |call| {
		before(call, "before");
		Verdict::Go
	});
	probe.after(move |call| capture(call, "after"));

	let tree = TreeBuilder::with_options(options).build_async().await.unwrap();
	for i in 0..commits {
		let mut txn = tree.begin().unwrap();
		txn.set_durability(Durability::Immediate);
		txn.set(key_of(i), value_of(i, 150)).unwrap();
		loop {
			match txn.commit().await {
				Ok(()) => break,
				Err(crate::Error::PipelineStall) => {
					tokio::time::sleep(Duration::from_millis(2)).await;
					txn = tree.begin().unwrap();
					txn.set_durability(Durability::Immediate);
					txn.set(key_of(i), value_of(i, 150)).unwrap();
				}
				Err(e) => panic!("commit {i}: {e}"),
			}
		}
		acked.store(i + 1, Ordering::SeqCst);
		// Lets the flush and compaction tasks run between commits, so that their steps are among
		// the ones that are imaged, and the commits are not all done before they begin.
		if i % 8 == 0 {
			tokio::task::yield_now().await;
		}
	}
	until_compacted(&tree).await;
	until("the queue of memtables to be empty", || tree.core.inner.immutable_count() == 0).await;
	tree.close().await.unwrap();
	drop(tree);
	probe.clear_hooks();
	drop(tx);
	// The thread that checks ends when the last image is checked.
	tokio::task::spawn_blocking(move || verifier.join().unwrap()).await.unwrap();
	findings
}

fn report(findings: &Findings) -> String {
	let failures = findings.failures.lock().unwrap();
	format!(
		"{} images checked, {} failed; the first: {:#?}",
		findings.checked.load(Ordering::SeqCst),
		failures.len(),
		failures.iter().take(3).collect::<Vec<_>>()
	)
}

/// Asserts that no image failed, and that the steps of the work that the test names were imaged.
fn assert_covered(findings: &Findings, steps: &[&str]) {
	assert!(findings.failures.lock().unwrap().is_empty(), "{}", report(findings));
	let kinds = findings.kinds.lock().unwrap();
	for step in steps {
		assert!(
			kinds.keys().any(|kind| kind.ends_with(step)),
			"no image was taken at a step of kind {step:?}: {kinds:?}"
		);
	}
	assert!(findings.checked.load(Ordering::SeqCst) >= 60, "{}", report(findings));
}

#[test(tokio::test)]
async fn a_crash_at_every_step_of_the_rotation_flush_manifest_and_compaction_loses_nothing() {
	within("every step", async {
		const MODES: &[CrashMode] =
			&[CrashMode::LoseUnsynced, CrashMode::ZeroTail, CrashMode::Prefix(usize::MAX)];
		let findings = every_step(false, 700, MODES).await;
		assert_covered(
			&findings,
			&[
				"open segment",
				"create table",
				"sync_file",
				"sync_dir",
				"replace manifest",
				"remove segment",
				"create output",
				"remove table",
			],
		);
	})
	.await;
}

#[test(tokio::test)]
async fn a_crash_at_every_step_with_the_value_log_loses_nothing() {
	within("every step, vlog", async {
		const MODES: &[CrashMode] =
			&[CrashMode::LoseUnsynced, CrashMode::ZeroTail, CrashMode::Prefix(usize::MAX)];
		let findings = every_step(true, 700, MODES).await;
		assert_covered(
			&findings,
			&[
				"open vlog file",
				"create table",
				"replace manifest",
				"create output",
				"remove table",
			],
		);
	})
	.await;
}
