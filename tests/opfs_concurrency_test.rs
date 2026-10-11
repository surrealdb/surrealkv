//! Browser tests of concurrent use of the OPFS backend inside one worker, run in a dedicated
//! worker of a real headless Chrome (`WASM_BINDGEN_USE_DEDICATED_WORKER=1 wasm-pack test
//! --headless --chrome --test opfs_concurrency_test`).
//!
//! A worker is single-threaded, so concurrency here means several async tasks (started with
//! `wasm_bindgen_futures::spawn_local`) that take turns at `yield_now` points while they hold
//! files open. Each test records the order in which its tasks ran and asserts that they really
//! interleaved, so that a test cannot pass by running its tasks one after the other.
//!
//! Groups:
//! 1. Several files open at once, written by interleaved tasks.
//! 2. A task appending to a log store while another reads an object store of a different file.
//! 3. Many tasks appending to one log store.
//! 4. Racing opens of one file, and `get_opfs_root` awaited by many tasks at once.

#![cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]

mod opfs_common;

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use opfs_common::*;
use surrealkv::storage::opfs::{get_opfs_root, open_opfs_sync_file, OpfsLogStore, OpfsObjectStore};
use surrealkv::storage::{LogStore, ObjectStore};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// The order in which tasks took their steps: the id of the task, once per step.
type StepLog = Rc<RefCell<Vec<usize>>>;

/// How many times consecutive steps of the log belong to different tasks.
fn task_switches(log: &[usize]) -> usize {
	log.windows(2).filter(|pair| pair[0] != pair[1]).count()
}

// ---------------------------------------------------------------------------------------------
// 1. Several files at once
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn files_written_by_interleaved_tasks_keep_their_own_contents() {
	const FILES: usize = 6;
	const STEPS: usize = 40;
	const SEED: u64 = 0xF11E5;
	let root = root().await;
	let steps: StepLog = Rc::default();

	// All the files are open before any task starts, and the tasks are all started before the
	// first of them runs: a task is only polled when the starting code awaits something.
	let mut names = Vec::new();
	let mut files = Vec::new();
	for id in 0..FILES {
		let name = unique_name(&format!("interleaved-{id}"), "bin");
		files.push(create(&root, &name).await);
		names.push(name);
	}

	// One task per file. Each appends seeded chunks to its own file, yielding after every chunk,
	// and checks every chunk it wrote by reading it straight back, while the others write theirs.
	let mut tasks = Vec::new();
	for (id, file) in files.into_iter().enumerate() {
		let steps = Rc::clone(&steps);
		tasks.push(spawn(async move {
			let mut rng = Rng::new(SEED + id as u64);
			let mut model = Vec::new();
			for step in 0..STEPS {
				let chunk = pattern(rng.next_u64(), 1 + rng.below(3000) as usize);
				let offset = model.len() as u64;
				let written = file.write_at(offset, &chunk).map_err(failed("write_at"))?;
				ensure_eq(written, chunk.len(), || {
					format!("file {id}, step {step}: bytes written")
				})?;
				model.extend_from_slice(&chunk);
				steps.borrow_mut().push(id);
				yield_now().await;
				let back = try_read_exact(&file, offset, chunk.len())?;
				ensure_bytes(&back, &chunk, || {
					format!(
						"file {id}, step {step}: the chunk just written changed while others wrote"
					)
				})?;
				let size = file.size().map_err(failed("size"))?;
				ensure_eq(size, model.len() as u64, || format!("size of file {id}, step {step}"))?;
			}
			file.flush().map_err(failed("flush"))?;
			Ok::<_, String>((file, model))
		}));
	}

	let mut results = Vec::new();
	for (id, task) in tasks.into_iter().enumerate() {
		results.push(finished(&format!("task for file {id}"), task).await);
	}

	// The tasks really took turns: every task had taken its first step before any took its second,
	// and the log switches between tasks on most steps.
	let log: Vec<usize> = steps.borrow().clone();
	assert_eq!(log.len(), FILES * STEPS);
	let mut first_round: Vec<usize> = log[..FILES].to_vec();
	first_round.sort_unstable();
	assert_eq!(first_round, (0..FILES).collect::<Vec<_>>(), "the first round of steps: {log:?}");
	assert!(
		task_switches(&log) >= FILES * STEPS / 2,
		"the tasks ran one after the other: {} switches in {} steps",
		task_switches(&log),
		log.len()
	);

	for (id, ((file, model), name)) in results.into_iter().zip(&names).enumerate() {
		assert_eq!(file.size().unwrap(), model.len() as u64, "final size of file {id}");
		assert!(read_all(&file) == model, "final contents of file {id}, seed {SEED}");
		file.close();
		remove_and_verify(&root, name).await;
	}
}

// ---------------------------------------------------------------------------------------------
// 2. Log store and object store at the same time
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn a_task_appends_to_a_log_store_while_another_reads_an_object_store_of_another_file() {
	const SEED: u64 = 0x10B;
	const APPENDS: usize = 200;
	const READS: usize = 300;
	let root = root().await;
	let steps: StepLog = Rc::default();

	let object_name = unique_name("mixed-object", "sst");
	let object_file = Arc::new(create(&root, &object_name).await);
	let content = pattern(SEED, 128 * 1024);
	object_file.write_at(0, &content).unwrap();
	object_file.flush().unwrap();
	let object = Arc::new(OpfsObjectStore::new(Arc::clone(&object_file)));

	let log_name = unique_name("mixed-log", "log");
	let log_file = Arc::new(create(&root, &log_name).await);
	let log = Arc::new(OpfsLogStore::new(Arc::clone(&log_file)));

	// Task 0 appends records; task 1 reads random ranges, some of them across the end.
	let appender = {
		let (log, steps) = (Arc::clone(&log), Rc::clone(&steps));
		spawn(async move {
			let mut rng = Rng::new(SEED + 1);
			let mut records = Vec::new();
			for i in 0..APPENDS {
				let record = pattern(SEED + 100 + i as u64, 1 + rng.below(200) as usize);
				let end = log.append(&record).await.map_err(failed("append"))?;
				steps.borrow_mut().push(0);
				records.push((end, record));
				if i % 25 == 0 {
					log.sync().await.map_err(failed("sync"))?;
				}
				yield_now().await;
			}
			Ok::<_, String>(records)
		})
	};
	let reader = {
		let (object, steps, content) = (Arc::clone(&object), Rc::clone(&steps), content.clone());
		spawn(async move {
			let mut rng = Rng::new(SEED + 2);
			for i in 0..READS {
				let offset = rng.below(content.len() as u64 + 500) as usize;
				let len = rng.below(4000) as usize;
				let from = offset.min(content.len());
				let to = offset.saturating_add(len).min(content.len());
				let bytes = object.read_at(offset as u64, len).await.map_err(failed("read_at"))?;
				steps.borrow_mut().push(1);
				ensure_bytes(bytes.as_ref(), &content[from..to], || {
					format!("read {i}: {len} bytes at {offset}")
				})?;
				yield_now().await;
			}
			Ok::<_, String>(())
		})
	};
	let records = finished("the appending task", appender).await;
	finished("the reading task", reader).await;

	let log_steps: Vec<usize> = steps.borrow().clone();
	assert_eq!(log_steps.iter().filter(|&&id| id == 0).count(), APPENDS);
	assert_eq!(log_steps.iter().filter(|&&id| id == 1).count(), READS);
	assert!(
		task_switches(&log_steps) >= APPENDS,
		"the two tasks did not interleave: {} switches",
		task_switches(&log_steps)
	);

	// The log holds the records back to back, each ending where its append said.
	let bytes = read_all(&log_file);
	let expected: Vec<u8> = records.iter().flat_map(|(_, r)| r.iter().copied()).collect();
	assert!(bytes == expected, "the log while another file was being read, seed {SEED}");
	for (i, (end, record)) in records.iter().enumerate() {
		assert_eq!(&bytes[*end as usize - record.len()..*end as usize], &record[..], "record {i}");
	}
	assert_eq!(log.size().await.unwrap(), bytes.len() as u64);

	object_file.close();
	log_file.close();
	remove_and_verify(&root, &object_name).await;
	remove_and_verify(&root, &log_name).await;
}

// ---------------------------------------------------------------------------------------------
// 3. Many tasks, one log store
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn many_tasks_appending_to_one_log_store_never_overlap_or_leave_holes() {
	const TASKS: usize = 8;
	const RECORDS: usize = 60;
	const SEED: u64 = 0xA99E;
	let root = root().await;
	let name = unique_name("shared-log", "log");
	let file = Arc::new(create(&root, &name).await);
	let log = Arc::new(OpfsLogStore::new(Arc::clone(&file)));
	let steps: StepLog = Rc::default();

	// A record is `[task, sequence (u16 le), payload length (u8), payload]`, so that the file can
	// be parsed back without help. Every third append is created, then the task yields while
	// holding the not yet polled future, and only then awaits it.
	let mut tasks = Vec::new();
	for id in 0..TASKS {
		let (log, steps) = (Arc::clone(&log), Rc::clone(&steps));
		tasks.push(spawn(async move {
			let mut rng = Rng::new(SEED + id as u64);
			let mut written = Vec::new();
			for seq in 0..RECORDS {
				let payload = pattern(rng.next_u64(), rng.below(120) as usize);
				let mut record = vec![id as u8];
				record.extend_from_slice(&(seq as u16).to_le_bytes());
				record.push(payload.len() as u8);
				record.extend_from_slice(&payload);

				let append = log.append(&record);
				if seq % 3 == 0 {
					yield_now().await;
				}
				let end = append.await.map_err(failed("append"))?;
				steps.borrow_mut().push(id);
				written.push((end, record));
				yield_now().await;
			}
			Ok::<_, String>(written)
		}));
	}
	let mut written = Vec::new();
	for (id, task) in tasks.into_iter().enumerate() {
		written.push(finished(&format!("appending task {id}"), task).await);
	}

	let steps: Vec<usize> = steps.borrow().clone();
	assert_eq!(steps.len(), TASKS * RECORDS);
	assert!(
		task_switches(&steps) >= TASKS * RECORDS / 2,
		"the tasks ran one after the other: {} switches",
		task_switches(&steps)
	);

	// Every returned offset is the end of that record's bytes in the file, and no two overlap.
	let bytes = read_all(&file);
	let total: usize = written.iter().flatten().map(|(_, record)| record.len()).sum();
	assert_eq!(bytes.len(), total, "the log is exactly the records, no hole and no overlap");
	assert_eq!(log.size().await.unwrap(), total as u64);
	let mut ends: Vec<u64> = written.iter().flatten().map(|(end, _)| *end).collect();
	ends.sort_unstable();
	ends.dedup();
	assert_eq!(ends.len(), TASKS * RECORDS, "every append returned its own end offset");
	for (end, record) in written.iter().flatten() {
		assert_eq!(
			&bytes[*end as usize - record.len()..*end as usize],
			&record[..],
			"record ending at {end}"
		);
	}

	// Parsing the file front to back yields every record, and each task's records in its order.
	let mut next_seq = [0usize; TASKS];
	let mut position = 0;
	let mut parsed = 0;
	while position < bytes.len() {
		let (id, seq) = (
			bytes[position] as usize,
			u16::from_le_bytes([bytes[position + 1], bytes[position + 2]]),
		);
		let len = bytes[position + 3] as usize;
		assert_eq!(
			seq as usize, next_seq[id],
			"task {id}'s records are out of order at {position}"
		);
		next_seq[id] += 1;
		position += 4 + len;
		parsed += 1;
	}
	assert_eq!(position, bytes.len(), "the last record ends exactly at the end of the log");
	assert_eq!(parsed, TASKS * RECORDS);

	file.close();
	remove_and_verify(&root, &name).await;
}

// ---------------------------------------------------------------------------------------------
// 4. Racing opens, concurrent root
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn racing_opens_of_one_file_let_exactly_one_handle_through() {
	const RACERS: usize = 6;
	let root = root().await;
	let name = unique_name("race", "bin");

	let mut racers = Vec::new();
	for _ in 0..RACERS {
		let (root, name) = (root.clone(), name.clone());
		racers.push(spawn(async move { open_opfs_sync_file(&root, &name, true).await }));
	}
	let mut winners = Vec::new();
	let mut losers = Vec::new();
	for racer in racers {
		match racer.await {
			Ok(file) => winners.push(file),
			Err(e) => losers.push(text(&e)),
		}
	}

	assert_eq!(winners.len(), 1, "exactly one open must win, {} did", winners.len());
	assert_eq!(losers.len(), RACERS - 1);
	for loser in &losers {
		assert!(loser.contains("NoModificationAllowedError"), "a losing open failed with: {loser}");
	}

	// The winner owns a working file; once it closes, the file is free again.
	let winner = winners.pop().unwrap();
	winner.write_at(0, b"winner").unwrap();
	assert_eq!(read_exact(&winner, 0, 6), b"winner");
	winner.close();
	let again = reopen(&root, &name).await;
	assert_eq!(read_all(&again), b"winner");
	again.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn get_opfs_root_can_be_awaited_by_many_tasks_at_once() {
	const TASKS: usize = 8;
	let stem = unique_name("many-roots", "x");

	let mut tasks = Vec::new();
	for id in 0..TASKS {
		let name = format!("{stem}-{id}.bin");
		tasks.push(spawn(async move {
			let root = get_opfs_root().await.map_err(failed("get_opfs_root"))?;
			let file = open_opfs_sync_file(&root, &name, true).await.map_err(failed("open"))?;
			file.write_at(0, name.as_bytes()).map_err(failed("write_at"))?;
			file.flush().map_err(failed("flush"))?;
			file.close();
			Ok::<_, String>(name)
		}));
	}
	let mut names = Vec::new();
	for (id, task) in tasks.into_iter().enumerate() {
		names.push(finished(&format!("task {id}"), task).await);
	}

	// A root obtained afterwards sees what every task made through its own handle.
	let root = root().await;
	assert_eq!(names.len(), TASKS);
	for name in &names {
		let file = reopen(&root, name).await;
		assert_eq!(read_all(&file), name.as_bytes(), "the file made by the task for {name}");
		file.close();
		remove_and_verify(&root, name).await;
	}
}
