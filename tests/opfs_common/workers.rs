//! Real workers that run the engine, for the browser tests of several tabs
//! (`tests/opfs_two_workers_test.rs`, which includes this file with `#[path]`: it is not part of
//! `opfs_common/mod.rs`, so no other target links it, and the export below stays in this one).
//!
//! # How a worker is made
//!
//! The test binary is a wasm-bindgen module, and `wasm-bindgen-test-runner` serves its glue as
//! `wasm-bindgen-test.js` next to the `worker.js` that runs the tests. The coordinator (the worker
//! that runs the test functions) starts a dedicated *module* worker from a Blob URL whose script
//! is fixed and tiny: it waits for one message, imports the glue by the absolute URL that the
//! coordinator derived from its own `self.location`, instantiates it with the
//! `WebAssembly.Module` that the message carries (`wasm_bindgen::module()`, which the browser
//! shares between workers, so nothing is fetched or compiled again), and calls
//! [`skv_worker_main`]. The glue runs no test on its own: the test harness starts in `worker.js`,
//! by `Context::run`, which the Blob script never calls. What runs in the new worker is the same
//! engine, with a memory of its own, and it talks to the coordinator in text lines
//! (`postMessage`).
//!
//! # The protocol
//!
//! The coordinator sends `"<id> <command> <key=value ...>"`; the worker runs one command at a
//! time, in order, and answers each with
//! `"r\t<id>\t<ok|err>\t<kind>\t<fields>\t<message>"`. `kind` of an error is `locked` when
//! `Error::is_database_locked` holds and `other` otherwise. The worker also sends `ready` once,
//! `ack\t<n>` after every commit that returned while a command asked for acknowledgements (all of
//! the indices below `n` are then committed), `ev\t<name>\t<time>\t<argument>` for what a test
//! needs to see happen (`mounting`, `mounted`, `spinning`, `closing`, `closed`, `sst`,
//! `migrating`, `migrated`, `work`), `log\t<text>`, and, if the engine
//! panics, `panic\t<text>` before the instance aborts. Lines never start with `__wbgtest_`, which
//! the runner's own shim swallows.
//!
//! The browser drops the messages that a worker sends after it was asked to end (`terminate()`),
//! and a worker can go on for some time after that (see the comment before section 4 of
//! `opfs_two_workers_test.rs`). What a test needs to know about a worker that it ends is therefore
//! written by the worker to a file of its own at the root of the
//! origin private file system, next to the pool, which outlives it: the number of indices it
//! committed (`ack_file` of a commit command) and how far its close got (`state_file` of
//! `close_tree`). The coordinator reads the file once the browser has released it.
//!
//! A panic does not unwind on this target, so nothing here relies on `Drop`: every worker is
//! entered in a registry, [`begin_test`] ends the ones that an earlier failed test left, and
//! [`end_test`] fails the test that leaves one running.
//!
//! # What the worker keeps in a database
//!
//! Index `i` is two keys, `<tag>-k-<i>` and `<tag>-a-<i>`, with values that follow from the seed of
//! the [`Db`] and `i`, written in one transaction. A commit of one index is therefore atomic when
//! both keys are there or neither is, and "a prefix of the commits" is "the indices `0..k`".

#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::Duration;

use surrealkv::storage::opfs::OpfsSyncFile;
use surrealkv::{
	migrate_from_indexeddb,
	migrate_from_indexeddb_with_tuning,
	Durability,
	Error,
	FileSystem,
	ImportTuning,
	IndexedDbMigration,
	LSMIterator,
	Mode,
	Options,
	Tree,
	TreeBuilder,
};
use surrealkv_compat_indxdb::ChunkLimits;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_test::console_log;
use web_sys::{DedicatedWorkerGlobalScope, MessageEvent, WorkerOptions, WorkerType};

use crate::opfs_common::{pattern, sleep_ms, unique_name, yield_now};

/// The name that the runner gives the glue of the test binary. It is looked at once, and a
/// worker that cannot import it says so in its error.
const GLUE_FILE: &str = "wasm-bindgen-test.js";

/// How long a new worker may take to say that it is ready.
const SPAWN_LIMIT_MS: f64 = 60_000.0;

/// How long any one command may take unless a test says otherwise.
pub const CALL_LIMIT_MS: f64 = 120_000.0;

/// The name of the file that a copy from IndexedDB keeps at the root of the pool while it runs.
/// (`IMPORTING_FROM_INDEXEDDB` in the engine, which keeps the constant to itself.)
pub const MIGRATION_MARKER: &str = "IMPORTING_FROM_INDEXEDDB";

fn now() -> f64 {
	js_sys::Date::now()
}

// ---------------------------------------------------------------------------------------------
// What the coordinator and the workers share: the database that a test uses
// ---------------------------------------------------------------------------------------------

/// A database of a test and the options every worker opens it with.
#[derive(Clone, Debug)]
pub struct Db {
	/// The name in the origin private file system.
	pub name: String,
	/// The value log on (values of 600 bytes, above its threshold) or off (values of 160 bytes).
	pub vlog: bool,
	/// The memtable in bytes; `0` is the default of the engine. Small makes flushes and
	/// compactions frequent.
	pub mem: usize,
	/// Which keys are this database's.
	pub tag: String,
	/// What the values are made from.
	pub seed: u64,
}

impl Db {
	/// A database that no earlier run used.
	pub fn new(label: &str, vlog: bool) -> Self {
		let name = unique_name(label, "db");
		let seed = name.bytes().fold(0xCBF2_9CE4_8422_2325u64, |h, b| {
			(h ^ u64::from(b)).wrapping_mul(0x0100_0000_01B3)
		});
		Self {
			name,
			vlog,
			mem: 0,
			tag: "t".to_string(),
			seed,
		}
	}

	pub fn with_mem(mut self, mem: usize) -> Self {
		self.mem = mem;
		self
	}

	pub fn with_tag(mut self, tag: &str) -> Self {
		self.tag = tag.to_string();
		self
	}

	pub fn key(&self, i: u64) -> Vec<u8> {
		format!("{}-k-{i:08}", self.tag).into_bytes()
	}

	pub fn aux(&self, i: u64) -> Vec<u8> {
		format!("{}-a-{i:08}", self.tag).into_bytes()
	}

	/// The value of index `i`: random content that differs from every other, so that a value that
	/// belongs to another key or another database shows as different bytes.
	pub fn value(&self, i: u64) -> Vec<u8> {
		let len = if self.vlog {
			600
		} else {
			160
		};
		pattern(self.seed ^ (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15), len)
	}

	pub fn aux_value(&self, i: u64) -> Vec<u8> {
		format!("aux-{}-{i}", self.seed).into_bytes()
	}

	/// Which index and which of the two keys `key` is, if it is one of this database's.
	fn parse(&self, key: &[u8]) -> Option<(bool, u64)> {
		let text = std::str::from_utf8(key).ok()?;
		for (is_key, infix) in [(true, "k"), (false, "a")] {
			let prefix = format!("{}-{infix}-", self.tag);
			if let Some(rest) = text.strip_prefix(&prefix) {
				let i: u64 = rest.parse().ok()?;
				if text == format!("{prefix}{i:08}") {
					return Some((is_key, i));
				}
			}
		}
		None
	}

	/// What the worker needs to know of the database, as arguments of a command.
	pub fn args(&self) -> String {
		format!(
			"db={} vlog={} mem={} tag={} seed={}",
			self.name, self.vlog as u8, self.mem, self.tag, self.seed
		)
	}

	fn from_args(args: &Args) -> Self {
		Self {
			name: args.text("db"),
			vlog: args.flag("vlog"),
			mem: args.number("mem") as usize,
			tag: args.text("tag"),
			seed: args.number("seed"),
		}
	}

	/// The options of the database, without a file system.
	pub fn options(&self) -> Options {
		let mut options = Options::default()
			.with_path("db".into())
			.with_enable_vlog(self.vlog)
			.with_vlog_value_threshold(256);
		if self.mem != 0 {
			options = options.with_max_memtable_size(self.mem);
			options.level0_max_files = 4;
		}
		options
	}
}

/// A 64-bit hash of rows, in the order given: what a test compares a database with a source by,
/// without moving megabytes between workers.
pub fn digest_rows<'a>(rows: impl IntoIterator<Item = (&'a [u8], &'a [u8])>) -> u64 {
	let mut hash = 0xCBF2_9CE4_8422_2325u64;
	let mut eat = |bytes: &[u8]| {
		for &b in bytes {
			hash = (hash ^ u64::from(b)).wrapping_mul(0x0100_0000_01B3);
		}
	};
	for (key, value) in rows {
		eat(&(key.len() as u32).to_le_bytes());
		eat(key);
		eat(&(value.len() as u32).to_le_bytes());
		eat(value);
	}
	hash
}

// ---------------------------------------------------------------------------------------------
// The coordinator
// ---------------------------------------------------------------------------------------------

/// How long a mount waits for a database that another worker holds.
#[derive(Clone, Copy, Debug)]
pub enum Wait {
	/// `FileSystem::opfs`.
	Default,
	/// `FileSystem::opfs_with_lock_wait`.
	Ms(u64),
}

/// What a worker did that a test looks at.
#[derive(Clone, Debug)]
pub struct Event {
	pub name: String,
	/// The clock of the worker, which is the one of the coordinator (`Date.now()`).
	pub at: f64,
	pub arg: String,
	/// The clock of the coordinator when the event arrived.
	pub seen_at: f64,
}

/// What a worker that commits in a loop does between two commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Yielding {
	/// Nothing: the tasks of the engine do not run between commits.
	Never = 0,
	/// Lets the tasks of the engine run, and every few commits the event loop of the worker too,
	/// so a message to the worker, a timer and the end of a terminated worker all get their turn.
	/// A worker that was asked to end stops at the next of these.
	EventLoop = 1,
	/// Lets the tasks of the engine run but never returns to the event loop, so the whole loop is
	/// one task of the worker. Chrome ends a worker when its task is over, and forcibly after
	/// about two seconds if it is not, wherever the code is at that moment: this is the loop
	/// that it ends in the middle of the work of the engine.
	Tasks = 2,
}

/// A command that commits indices.
#[derive(Clone, Debug)]
pub struct CommitSpec {
	pub from: u64,
	pub count: u64,
	/// Indices in one transaction.
	pub per: u64,
	/// `Durability::Immediate` if true, `Eventual` if not.
	pub durable: bool,
	/// Send `ack` after every commit.
	pub ack: bool,
	/// What the worker does between commits.
	pub yielding: Yielding,
	/// Send the number of tables of the database after every this many commits (`0` never).
	pub sst_every: u64,
	/// Write the number of indices that are committed, as eight bytes, to this file at the root of
	/// the origin private file system after every commit: what a worker that was ended has
	/// acknowledged, which no message can say, since the messages of an ended worker are dropped.
	pub ack_file: Option<String>,
}

impl CommitSpec {
	pub fn new(from: u64, count: u64, durable: bool) -> Self {
		Self {
			from,
			count,
			per: 1,
			durable,
			ack: false,
			yielding: Yielding::Never,
			sst_every: 0,
			ack_file: None,
		}
	}

	pub fn per(mut self, per: u64) -> Self {
		self.per = per;
		self
	}

	pub fn ack(mut self) -> Self {
		self.ack = true;
		self
	}

	pub fn yielding(mut self, yielding: Yielding) -> Self {
		self.yielding = yielding;
		self
	}

	pub fn sst_every(mut self, every: u64) -> Self {
		self.sst_every = every;
		self
	}

	pub fn ack_file(mut self, name: &str) -> Self {
		self.ack_file = Some(name.to_string());
		self
	}

	fn args(&self) -> String {
		format!(
			"from={} count={} per={} durable={} ack={} yield={} sst_every={} ack_file={}",
			self.from,
			self.count,
			self.per,
			self.durable as u8,
			self.ack as u8,
			self.yielding as u8,
			self.sst_every,
			self.ack_file.as_deref().unwrap_or("")
		)
	}
}

/// The answer to one command.
#[derive(Clone, Debug)]
pub struct Reply {
	pub ok: bool,
	/// `locked` or `other` for an error.
	pub kind: String,
	fields: Vec<(String, String)>,
	pub message: String,
	/// The clock of the coordinator when the reply arrived.
	pub seen_at: f64,
}

impl Reply {
	pub fn text(&self, key: &str) -> &str {
		self.fields
			.iter()
			.find(|(k, _)| k == key)
			.map(|(_, v)| v.as_str())
			.unwrap_or_else(|| panic!("the reply has no field {key:?}: {self:?}"))
	}

	pub fn number(&self, key: &str) -> i64 {
		self.text(key).parse().unwrap_or_else(|_| panic!("field {key:?} is not a number: {self:?}"))
	}

	pub fn has(&self, key: &str) -> bool {
		self.fields.iter().any(|(k, _)| k == key)
	}

	pub fn is_locked(&self) -> bool {
		!self.ok && self.kind == "locked"
	}

	/// The reply, failing the test with the worker's message if it is an error.
	pub fn unwrap(self, what: &str) -> Self {
		if !self.ok {
			panic!("{what}: the worker answered with an error ({}): {}", self.kind, self.message);
		}
		self
	}
}

enum Msg {
	Ready,
	Reply(u64, Reply),
	Panic(String),
	Fatal(String),
}

/// What the message handlers of a worker and the test both see.
struct Shared {
	label: String,
	worker: web_sys::Worker,
	acked: Cell<u64>,
	events: RefCell<Vec<Event>>,
	kill_at_ack: Cell<Option<u64>>,
	terminated_at: Cell<Option<f64>>,
}

thread_local! {
	static REGISTRY: RefCell<Vec<Rc<Shared>>> = const { RefCell::new(Vec::new()) };
	static SCRIPT: RefCell<Option<String>> = const { RefCell::new(None) };
}

impl Shared {
	/// Ends the worker. The browser drops what it had not yet sent, and releases its handles when
	/// it gets to it. Safe to call from a message handler.
	fn end(&self) {
		if self.terminated_at.get().is_none() {
			self.worker.terminate();
			self.terminated_at.set(Some(now()));
		}
		let me: *const Shared = self;
		REGISTRY
			.with(|registry| registry.borrow_mut().retain(|w| !std::ptr::eq(Rc::as_ptr(w), me)));
	}
}

/// A worker that runs the engine, as the coordinator sees it.
pub struct Child {
	shared: Rc<Shared>,
	rx: tokio::sync::Mutex<UnboundedReceiver<Msg>>,
	next_id: Cell<u64>,
}

/// The absolute URL of the glue of the test binary.
fn glue_url() -> String {
	let global = js_sys::global();
	let location =
		js_sys::Reflect::get(&global, &"location".into()).expect("a worker has a location");
	let href = js_sys::Reflect::get(&location, &"href".into())
		.ok()
		.and_then(|v| v.as_string())
		.expect("the location of the worker has an href");
	web_sys::Url::new_with_base(GLUE_FILE, &href)
		.unwrap_or_else(|e| panic!("no URL for {GLUE_FILE} next to {href}: {e:?}"))
		.href()
}

/// The Blob URL of the script of a worker, made once. The script is the same for every worker.
fn script_url() -> String {
	SCRIPT.with(|slot| {
		if let Some(url) = slot.borrow().as_ref() {
			return url.clone();
		}
		let script = format!(
			"self.addEventListener('message', async (e) => {{\n\
			 \ttry {{\n\
			 \t\tconst glue = await import({glue:?});\n\
			 \t\tawait glue.default({{ module_or_path: e.data }});\n\
			 \t\tglue.skv_worker_main();\n\
			 \t}} catch (err) {{\n\
			 \t\tpostMessage('fatal\\t' + String((err && err.stack) || err));\n\
			 \t}}\n\
			 }}, {{ once: true }});\n",
			glue = glue_url()
		);
		let parts = js_sys::Array::of1(&JsValue::from_str(&script));
		let bag = web_sys::BlobPropertyBag::new();
		bag.set_type("text/javascript");
		let blob = web_sys::Blob::new_with_str_sequence_and_options(&parts, &bag)
			.expect("a Blob for the script of the workers");
		let url = web_sys::Url::create_object_url_with_blob(&blob).expect("a URL for the Blob");
		*slot.borrow_mut() = Some(url.clone());
		url
	})
}

fn parse_fields(text: &str) -> Vec<(String, String)> {
	text.split(' ')
		.filter(|pair| !pair.is_empty())
		.filter_map(|pair| pair.split_once('='))
		.map(|(k, v)| (k.to_string(), v.to_string()))
		.collect()
}

/// Ends every worker that an earlier test left running (a test that failed does not get to end its
/// own: the panic does not unwind), and says so. Call it first in every test.
pub fn begin_test(name: &str) {
	let left: Vec<Rc<Shared>> = REGISTRY.with(|registry| registry.borrow().clone());
	for worker in left {
		console_log!(
			"{name}: ending the worker {} that an earlier test left running",
			worker.label
		);
		worker.end();
	}
	console_log!("== {name}");
}

/// Fails the test if a worker is still running. Call it last in every test, after the workers
/// were ended and the database was removed.
pub fn end_test(name: &str) {
	let left: Vec<String> =
		REGISTRY.with(|registry| registry.borrow().iter().map(|w| w.label.clone()).collect());
	assert!(left.is_empty(), "{name}: the workers {left:?} are still running");
}

impl Child {
	/// Starts a worker, and waits until it runs the engine.
	pub async fn spawn(label: &str) -> Self {
		let options = WorkerOptions::new();
		options.set_type(WorkerType::Module);
		let worker = web_sys::Worker::new_with_options(&script_url(), &options)
			.unwrap_or_else(|e| panic!("could not start the worker {label}: {e:?}"));
		let shared = Rc::new(Shared {
			label: label.to_string(),
			worker: worker.clone(),
			acked: Cell::new(0),
			events: RefCell::default(),
			kill_at_ack: Cell::new(None),
			terminated_at: Cell::new(None),
		});
		REGISTRY.with(|registry| registry.borrow_mut().push(Rc::clone(&shared)));
		let (tx, rx) = unbounded_channel::<Msg>();

		let weak = Rc::downgrade(&shared);
		let sender = tx.clone();
		let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
			if let Some(shared) = weak.upgrade() {
				handle_line(&shared, &sender, event.data().as_string());
			}
		});
		worker.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
		// Kept for as long as the page lives: a few bytes per worker.
		on_message.forget();

		let on_error = Closure::<dyn FnMut(web_sys::ErrorEvent)>::new({
			let tx = tx.clone();
			let label = label.to_string();
			move |event: web_sys::ErrorEvent| {
				let _ = tx.send(Msg::Fatal(format!(
					"the worker {label} raised an error: {} ({}:{})",
					event.message(),
					event.filename(),
					event.lineno()
				)));
			}
		});
		worker.set_onerror(Some(on_error.as_ref().unchecked_ref()));
		on_error.forget();
		let on_message_error = Closure::<dyn FnMut(web_sys::Event)>::new({
			let label = label.to_string();
			move |_event: web_sys::Event| {
				let _ = tx.send(Msg::Fatal(format!(
					"a message to or from the worker {label} could not be read"
				)));
			}
		});
		worker
			.add_event_listener_with_callback(
				"messageerror",
				on_message_error.as_ref().unchecked_ref(),
			)
			.expect("listen for messageerror");
		on_message_error.forget();

		let child = Self {
			shared,
			rx: tokio::sync::Mutex::new(rx),
			next_id: Cell::new(1),
		};
		worker
			.post_message(&wasm_bindgen::module())
			.unwrap_or_else(|e| panic!("could not send the module to the worker {label}: {e:?}"));
		match child.next_message(SPAWN_LIMIT_MS, "the worker to start").await {
			Msg::Ready => child,
			Msg::Fatal(text) | Msg::Panic(text) => {
				panic!("the worker {label} did not start: {text}")
			}
			Msg::Reply(..) => panic!("the worker {label} answered before it was ready"),
		}
	}

	pub fn label(&self) -> &str {
		&self.shared.label
	}

	/// The next message, or a failure of the test if none comes within `limit_ms`.
	async fn next_message(&self, limit_ms: f64, what: &str) -> Msg {
		let mut rx = self.rx.lock().await;
		tokio::select! {
			biased;
			msg = rx.recv() => msg.unwrap_or_else(|| {
				panic!("the channel of the worker {} closed while waiting for {what}", self.shared.label)
			}),
			_ = sleep_ms(limit_ms) => panic!(
				"the worker {} did not answer within {limit_ms} ms while waiting for {what}",
				self.shared.label
			),
		}
	}

	/// Sends a command without waiting for its answer. Answers come in the order of the commands.
	pub fn send(&self, command: &str) -> u64 {
		let id = self.next_id.get();
		self.next_id.set(id + 1);
		self.shared
			.worker
			.post_message(&JsValue::from_str(&format!("{id} {command}")))
			.unwrap_or_else(|e| {
				panic!("could not send {command:?} to {}: {e:?}", self.shared.label)
			});
		id
	}

	/// Waits for the answer to the command `id`.
	pub async fn wait(&self, id: u64, limit_ms: f64, what: &str) -> Reply {
		loop {
			match self.next_message(limit_ms, what).await {
				Msg::Reply(got, reply) if got == id => return reply,
				Msg::Reply(got, _) => panic!(
					"{}: the answer to command {got} came while waiting for {id} ({what})",
					self.shared.label
				),
				Msg::Ready => {}
				Msg::Panic(text) => {
					panic!(
						"the engine panicked in the worker {} ({what}): {text}",
						self.shared.label
					)
				}
				Msg::Fatal(text) => {
					panic!("the worker {} failed ({what}): {text}", self.shared.label)
				}
			}
		}
	}

	/// Sends a command and waits for its answer.
	pub async fn call(&self, command: &str, what: &str) -> Reply {
		let id = self.send(command);
		self.wait(id, CALL_LIMIT_MS, what).await
	}

	// -- the commands --

	pub fn send_mount(&self, db: &Db, wait: Wait) -> u64 {
		let wait = match wait {
			Wait::Default => "default".to_string(),
			Wait::Ms(ms) => ms.to_string(),
		};
		self.send(&format!("mount {} wait={wait}", db.args()))
	}

	pub async fn mount(&self, db: &Db, wait: Wait) -> Reply {
		let id = self.send_mount(db, wait);
		let limit = match wait {
			Wait::Default => CALL_LIMIT_MS,
			Wait::Ms(ms) => ms as f64 + CALL_LIMIT_MS,
		};
		self.wait(id, limit, "a mount").await
	}

	pub async fn open(&self) -> Reply {
		self.call("open", "open the database").await
	}

	/// Mounts with the default wait and opens, failing the test if either fails.
	pub async fn mount_and_open(&self, db: &Db) -> (Reply, Reply) {
		let mounted = self.mount(db, Wait::Default).await.unwrap("mount");
		let opened = self.open().await.unwrap("open");
		(mounted, opened)
	}

	pub fn send_commit(&self, spec: &CommitSpec) -> u64 {
		self.send(&format!("commit {}", spec.args()))
	}

	pub async fn commit(&self, spec: &CommitSpec) -> Reply {
		let id = self.send_commit(spec);
		self.wait(id, CALL_LIMIT_MS, "a commit").await
	}

	/// Reads the indices `from..from + count` with `get` and checks both keys and their values.
	pub async fn verify(&self, from: u64, count: u64) -> Reply {
		self.call(&format!("verify from={from} count={count}"), "verify the keys").await
	}

	/// Scans the whole database and classifies what is in it (see the command `probe`).
	pub async fn probe(&self) -> Probe {
		let reply = self.call("probe", "scan the database").await.unwrap("probe");
		Probe {
			rows: reply.number("rows") as u64,
			indices: reply.number("indices") as u64,
			prefix: reply.number("prefix") as u64,
			max: reply.number("max"),
			holes: reply.number("holes") as u64,
			unpaired: reply.number("unpaired") as u64,
			bad: reply.number("bad") as u64,
			foreign: reply.number("foreign") as u64,
		}
	}

	pub async fn close_tree(&self) -> Reply {
		self.call("close_tree", "close the tree").await
	}

	/// Sends `close_tree` that begins with a busy wait of `spin_ms` in the same task, and writes
	/// how far it got to the file `state_file` at the root of the origin private file system: a
	/// byte that is 0 before the close begins, 1 while it runs and 2 when it has returned.
	pub fn send_close_tree_after(&self, spin_ms: u64, state_file: &str) -> u64 {
		self.send(&format!("close_tree spin={spin_ms} state_file={state_file}"))
	}

	/// Sends a task that keeps the worker busy for `ms` without returning to its event loop.
	pub fn send_spin(&self, ms: u64) -> u64 {
		self.send(&format!("spin ms={ms}"))
	}

	/// Closes the tree and the file system, which releases the lock.
	pub async fn close(&self) -> Reply {
		self.call("close", "close the tree and the file system").await
	}

	/// Closes what is open, answers, and ends the worker.
	pub async fn exit(&self) -> Reply {
		let reply = self.call("exit", "exit").await;
		self.terminate();
		reply
	}

	/// The worker is ended at once, as when its tab is closed. What it was doing stops where it
	/// is, and the browser releases its handles in its own time.
	pub fn terminate(&self) {
		self.shared.end();
	}

	/// Ends the worker from the message handler as soon as the acknowledgement of the commit that
	/// made `n` indices durable arrives, with no delay in between.
	pub fn terminate_at_ack(&self, n: u64) {
		self.shared.kill_at_ack.set(Some(n));
	}

	pub fn is_terminated(&self) -> bool {
		self.shared.terminated_at.get().is_some()
	}

	/// When the worker was ended, on the clock of the coordinator.
	pub fn terminated_at(&self) -> Option<f64> {
		self.shared.terminated_at.get()
	}

	/// How many indices the worker said it had committed (the last `ack` that arrived).
	pub fn acked(&self) -> u64 {
		self.shared.acked.get()
	}

	pub fn events(&self, name: &str) -> Vec<Event> {
		self.shared.events.borrow().iter().filter(|e| e.name == name).cloned().collect()
	}

	/// Waits until `condition` holds, polling the state that the handlers keep.
	pub async fn wait_until(
		&self,
		what: &str,
		limit_ms: f64,
		mut condition: impl FnMut(&Self) -> bool,
	) {
		let started = now();
		while !condition(self) {
			assert!(
				now() - started < limit_ms,
				"worker {}: {what} did not happen within {limit_ms} ms",
				self.shared.label
			);
			sleep_ms(1.0).await;
		}
	}
}

/// What `probe` found in a database.
#[derive(Clone, Copy, Debug)]
pub struct Probe {
	/// Entries in the database.
	pub rows: u64,
	/// Indices that have either key.
	pub indices: u64,
	/// The length of the run `0..prefix` in which both keys are there.
	pub prefix: u64,
	/// The highest index that has either key, `-1` if none.
	pub max: i64,
	/// Indices below `max` that have neither key.
	pub holes: u64,
	/// Indices that have one key and not the other: a torn commit.
	pub unpaired: u64,
	/// Entries with the wrong value.
	pub bad: u64,
	/// Entries that are not keys of this database.
	pub foreign: u64,
}

impl Probe {
	/// Whether the database holds exactly the indices `0..prefix`, each complete and right.
	pub fn is_a_prefix(&self) -> bool {
		self.holes == 0
			&& self.unpaired == 0
			&& self.bad == 0
			&& self.foreign == 0
			&& self.rows == 2 * self.prefix
			&& self.max + 1 == self.prefix as i64
	}
}

/// Handles one line from a worker, on the thread of the coordinator.
fn handle_line(shared: &Shared, tx: &UnboundedSender<Msg>, line: Option<String>) {
	let Some(line) = line else {
		let _ = tx.send(Msg::Fatal("a message that is not text".to_string()));
		return;
	};
	let mut parts = line.splitn(2, '\t');
	let head = parts.next().unwrap_or("");
	let rest = parts.next().unwrap_or("");
	match head {
		"ready" => {
			let _ = tx.send(Msg::Ready);
		}
		"ack" => {
			let n: u64 = rest.parse().unwrap_or(0);
			shared.acked.set(n);
			if shared.kill_at_ack.get().is_some_and(|at| n >= at) {
				shared.kill_at_ack.set(None);
				shared.end();
			}
		}
		"ev" => {
			let mut f = rest.splitn(3, '\t');
			let name = f.next().unwrap_or("").to_string();
			let at = f.next().and_then(|t| t.parse().ok()).unwrap_or(0.0);
			let arg = f.next().unwrap_or("").to_string();
			shared.events.borrow_mut().push(Event {
				name,
				at,
				arg,
				seen_at: now(),
			});
		}
		"log" => console_log!("  [{}] {rest}", shared.label),
		"panic" => {
			let _ = tx.send(Msg::Panic(rest.to_string()));
		}
		"fatal" => {
			let _ = tx.send(Msg::Fatal(rest.to_string()));
		}
		"r" => {
			let mut f = rest.splitn(5, '\t');
			let id: u64 = f.next().and_then(|s| s.parse().ok()).unwrap_or(0);
			let ok = f.next() == Some("ok");
			let kind = f.next().unwrap_or("").to_string();
			let fields = parse_fields(f.next().unwrap_or(""));
			let message = f.next().unwrap_or("").to_string();
			let _ = tx.send(Msg::Reply(
				id,
				Reply {
					ok,
					kind,
					fields,
					message,
					seen_at: now(),
				},
			));
		}
		other => {
			let _ = tx.send(Msg::Fatal(format!("an unknown message {other:?}: {rest}")));
		}
	}
}

// ---------------------------------------------------------------------------------------------
// The worker
// ---------------------------------------------------------------------------------------------

fn post(line: &str) {
	let scope: DedicatedWorkerGlobalScope = js_sys::global().unchecked_into();
	let _ = scope.post_message(&JsValue::from_str(line));
}

fn event(name: &str, arg: &str) {
	post(&format!("ev\t{name}\t{}\t{arg}", now()));
}

fn log(text: &str) {
	post(&format!("log\t{text}"));
}

/// The arguments of a command, `key=value` separated by spaces.
struct Args(Vec<(String, String)>);

impl Args {
	fn text(&self, key: &str) -> String {
		self.get(key).unwrap_or_default().to_string()
	}

	fn get(&self, key: &str) -> Option<&str> {
		self.0.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
	}

	fn number(&self, key: &str) -> u64 {
		self.get(key).and_then(|v| v.parse().ok()).unwrap_or(0)
	}

	fn flag(&self, key: &str) -> bool {
		self.number(key) != 0
	}
}

type Fields = Vec<(&'static str, String)>;

/// Why a command failed.
struct Failure {
	kind: &'static str,
	message: String,
	fields: Fields,
}

fn other(what: &str, error: impl std::fmt::Display) -> Failure {
	Failure {
		kind: "other",
		message: format!("{what}: {error}"),
		fields: Vec::new(),
	}
}

fn engine(what: &str, error: Error) -> Failure {
	Failure {
		kind: if error.is_database_locked() {
			"locked"
		} else {
			"other"
		},
		message: format!("{what}: {error}"),
		fields: Vec::new(),
	}
}

fn engine_after(what: &str, error: Error, started: f64) -> Failure {
	let mut failure = engine(what, error);
	failure.fields.push(("ms", format!("{:.0}", now() - started)));
	failure
}

#[derive(Default)]
struct Session {
	db: Option<Db>,
	fs: Option<FileSystem>,
	tree: Option<Tree>,
}

impl Session {
	fn db(&self) -> Result<&Db, Failure> {
		self.db.as_ref().ok_or_else(|| other("no database", "mount comes first"))
	}

	fn fs(&self) -> Result<&FileSystem, Failure> {
		self.fs.as_ref().ok_or_else(|| other("no file system", "mount comes first"))
	}

	fn tree(&self) -> Result<&Tree, Failure> {
		self.tree.as_ref().ok_or_else(|| other("no tree", "open comes first"))
	}

	fn tables(&self) -> usize {
		self.fs.as_ref().map_or(0, |fs| fs.list_for_test("db/sstables").len())
	}
}

/// The entry of the worker, called by the Blob script after the module is instantiated: listens
/// for commands and runs them one at a time.
#[wasm_bindgen]
pub fn skv_worker_main() {
	std::panic::set_hook(Box::new(|info| {
		post(&format!("panic\t{}", info.to_string().replace('\t', " ")))
	}));
	let scope: DedicatedWorkerGlobalScope = js_sys::global().unchecked_into();
	let (tx, mut rx) = unbounded_channel::<String>();
	let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
		if let Some(line) = event.data().as_string() {
			let _ = tx.send(line);
		}
	});
	scope.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
	on_message.forget();
	wasm_bindgen_futures::spawn_local(async move {
		let mut session = Session::default();
		post("ready");
		while let Some(line) = rx.recv().await {
			let mut words = line.split(' ');
			let id = words.next().unwrap_or("0").to_string();
			let command = words.next().unwrap_or("").to_string();
			let args = Args(
				words
					.filter_map(|w| w.split_once('='))
					.map(|(k, v)| (k.to_string(), v.to_string()))
					.collect(),
			);
			let outcome = run(&mut session, &command, &args).await;
			let fields = |fields: &Fields| {
				fields.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(" ")
			};
			match outcome {
				Ok(f) => post(&format!("r\t{id}\tok\t\t{}\t", fields(&f))),
				Err(e) => post(&format!(
					"r\t{id}\terr\t{}\t{}\t{}",
					e.kind,
					fields(&e.fields),
					e.message.replace(['\t', '\n'], " ")
				)),
			}
		}
	});
}

async fn run(s: &mut Session, command: &str, a: &Args) -> Result<Fields, Failure> {
	match command {
		"mount" => cmd_mount(s, a).await,
		"open" => cmd_open(s).await,
		"commit" => cmd_commit(s, a).await,
		"verify" => cmd_verify(s, a).await,
		"probe" => cmd_probe(s),
		"digest" => cmd_digest(s),
		"close_tree" => cmd_close_tree(s, a).await,
		"close" => cmd_close(s).await,
		"exit" => {
			let reply = cmd_close(s).await;
			s.tree = None;
			s.fs = None;
			reply.or_else(|e| Ok(vec![("close_error", e.message)]))
		}
		"destroy" => {
			FileSystem::destroy_opfs(&a.text("db")).await.map_err(|e| engine("destroy", e))?;
			Ok(vec![])
		}
		"files" => cmd_files(s),
		"marker" => cmd_marker(s),
		"migrate" => cmd_migrate(a).await,
		"work" => cmd_work(s, a).await,
		"spin" => {
			spin(a.number("ms"));
			Ok(vec![])
		}
		other => Err(other_failure(format!("unknown command {other:?}"))),
	}
}

fn other_failure(message: String) -> Failure {
	Failure {
		kind: "other",
		message,
		fields: Vec::new(),
	}
}

async fn cmd_mount(s: &mut Session, a: &Args) -> Result<Fields, Failure> {
	let db = Db::from_args(a);
	let started = now();
	event("mounting", "");
	let mounted = match a.get("wait") {
		Some("default") | None => FileSystem::opfs(&db.name).await,
		Some(ms) => {
			let ms: u64 = ms.parse().map_err(|e| other("wait", e))?;
			FileSystem::opfs_with_lock_wait(&db.name, Duration::from_millis(ms)).await
		}
	};
	let fs = mounted.map_err(|e| engine_after("mount", e, started))?;
	// What the mount found in the files: after a worker was ended in the middle of its work, this
	// is where a half-made file shows.
	let found = fs.mount_report().unwrap_or_default();
	let fields = vec![
		("ms", format!("{:.0}", now() - started)),
		("files", found.files.to_string()),
		("spares", found.spares.to_string()),
		("losers", found.losers_cleaned.to_string()),
		("orphans", found.orphans_kept.len().to_string()),
		("cut_back", found.cut_back.to_string()),
		("blank", found.blank_initialised.to_string()),
	];
	s.db = Some(db);
	s.fs = Some(fs);
	event("mounted", "");
	Ok(fields)
}

async fn cmd_open(s: &mut Session) -> Result<Fields, Failure> {
	let db = s.db()?.clone();
	let fs = s.fs()?.clone();
	let started = now();
	let options = db.options().with_file_system(fs.clone());
	let tree = TreeBuilder::with_options(options)
		.build_async()
		.await
		.map_err(|e| engine_after("open", e, started))?;
	let report = tree.recovery_report();
	let fields = vec![
		("ms", format!("{:.0}", now() - started)),
		("segments", report.segments_replayed.to_string()),
		("replayed", report.records_replayed.to_string()),
		("dropped", report.records_dropped.to_string()),
		("trimmed", report.bytes_trimmed.to_string()),
		("quarantined", report.quarantined_files.len().to_string()),
		("tables", fs.list_for_test("db/sstables").len().to_string()),
	];
	s.tree = Some(tree);
	Ok(fields)
}

async fn cmd_commit(s: &Session, a: &Args) -> Result<Fields, Failure> {
	let db = s.db()?;
	let tree = s.tree()?;
	let (from, count) = (a.number("from"), a.number("count"));
	let per = a.number("per").max(1);
	let durability = if a.flag("durable") {
		Durability::Immediate
	} else {
		Durability::Eventual
	};
	let (ack, yielding, sst_every) = (a.flag("ack"), a.number("yield"), a.number("sst_every"));
	let ack_file = match a.text("ack_file").as_str() {
		"" => None,
		name => {
			let root = crate::opfs_common::root().await;
			Some(crate::opfs_common::create(&root, name).await)
		}
	};
	let end = from + count;
	let (mut next, mut commits) = (from, 0u64);
	while next < end {
		let group_end = (next + per).min(end);
		let mut txn = tree.begin().map_err(|e| engine("begin", e))?;
		txn.set_durability(durability);
		for i in next..group_end {
			txn.set(db.key(i), db.value(i)).map_err(|e| engine("set", e))?;
			txn.set(db.aux(i), db.aux_value(i)).map_err(|e| engine("set", e))?;
		}
		txn.commit().await.map_err(|e| engine("commit", e))?;
		next = group_end;
		commits += 1;
		// The file first: a worker that is stopped between the two has made it known where it can
		// be read, and the message is only for the coordinator that is still listening.
		if let Some(file) = &ack_file {
			file.write_at(0, &next.to_le_bytes())
				.map_err(|e| other("write the ack file", format!("{e:?}")))?;
		}
		if ack {
			post(&format!("ack\t{next}"));
		}
		if sst_every != 0 && commits % sst_every == 0 {
			event("sst", &s.tables().to_string());
		}
		if yielding != 0 {
			yield_now().await;
			if yielding == 1 && commits % 8 == 0 {
				sleep_ms(0.0).await;
			}
		}
	}
	Ok(vec![("end", next.to_string()), ("commits", commits.to_string())])
}

async fn cmd_verify(s: &Session, a: &Args) -> Result<Fields, Failure> {
	let db = s.db()?;
	let tree = s.tree()?;
	let (from, count) = (a.number("from"), a.number("count"));
	let txn = tree.begin_with_mode(Mode::ReadOnly).map_err(|e| engine("begin", e))?;
	for i in from..from + count {
		let (key, aux) = (db.key(i), db.aux(i));
		let (got_key, got_aux) = if i % 7 == 0 {
			(
				txn.get_async(key).await.map_err(|e| engine("get_async", e))?,
				txn.get_async(aux).await.map_err(|e| engine("get_async", e))?,
			)
		} else {
			(
				txn.get(key).map_err(|e| engine("get", e))?,
				txn.get(aux).map_err(|e| engine("get", e))?,
			)
		};
		if got_key.as_deref() != Some(&db.value(i)[..]) {
			return Err(other_failure(format!(
				"index {i}: the key is {}",
				got_key.map_or("missing".to_string(), |v| format!(
					"there with {} wrong bytes",
					v.len()
				))
			)));
		}
		if got_aux.as_deref() != Some(&db.aux_value(i)[..]) {
			return Err(other_failure(format!(
				"index {i}: the second key of the commit is not right"
			)));
		}
	}
	Ok(vec![("verified", count.to_string())])
}

/// Scans every entry and classifies it.
fn cmd_probe(s: &Session) -> Result<Fields, Failure> {
	let db = s.db()?;
	let tree = s.tree()?;
	let txn = tree.begin_with_mode(Mode::ReadOnly).map_err(|e| engine("begin", e))?;
	let mut iter = txn.iter().map_err(|e| engine("iter", e))?;
	let (mut keys, mut auxes) = (BTreeSet::new(), BTreeSet::new());
	let (mut rows, mut bad, mut foreign) = (0u64, 0u64, 0u64);
	let mut valid = iter.seek_first().map_err(|e| engine("seek_first", e))?;
	while valid {
		let key = iter.key().user_key().to_vec();
		let value = iter.value().map_err(|e| engine("value", e))?;
		rows += 1;
		match db.parse(&key) {
			Some((true, i)) => {
				if value != db.value(i) {
					bad += 1;
				}
				keys.insert(i);
			}
			Some((false, i)) => {
				if value != db.aux_value(i) {
					bad += 1;
				}
				auxes.insert(i);
			}
			None => foreign += 1,
		}
		valid = iter.next().map_err(|e| engine("next", e))?;
	}
	let both: BTreeSet<u64> = keys.intersection(&auxes).copied().collect();
	let either: BTreeSet<u64> = keys.union(&auxes).copied().collect();
	let unpaired = (either.len() - both.len()) as u64;
	let prefix = (0u64..).take_while(|i| both.contains(i)).count() as u64;
	let max = either.last().map_or(-1, |m| *m as i64);
	let holes = if max < 0 {
		0
	} else {
		(max as u64 + 1) - either.len() as u64
	};
	Ok(vec![
		("rows", rows.to_string()),
		("indices", either.len().to_string()),
		("prefix", prefix.to_string()),
		("max", max.to_string()),
		("holes", holes.to_string()),
		("unpaired", unpaired.to_string()),
		("bad", bad.to_string()),
		("foreign", foreign.to_string()),
	])
}

/// Scans every entry and hashes them (see [`digest_rows`]).
fn cmd_digest(s: &Session) -> Result<Fields, Failure> {
	let tree = s.tree()?;
	let txn = tree.begin_with_mode(Mode::ReadOnly).map_err(|e| engine("begin", e))?;
	let mut iter = txn.iter().map_err(|e| engine("iter", e))?;
	let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
	let mut valid = iter.seek_first().map_err(|e| engine("seek_first", e))?;
	while valid {
		rows.push((iter.key().user_key().to_vec(), iter.value().map_err(|e| engine("value", e))?));
		valid = iter.next().map_err(|e| engine("next", e))?;
	}
	let digest = digest_rows(rows.iter().map(|(k, v)| (k.as_slice(), v.as_slice())));
	Ok(vec![("rows", rows.len().to_string()), ("digest", format!("{digest:016x}"))])
}

/// Does nothing for `ms` without returning to the event loop: one long task of the worker.
fn spin(ms: u64) {
	event("spinning", &ms.to_string());
	let started = now();
	while now() - started < ms as f64 {}
}

async fn cmd_close_tree(s: &mut Session, a: &Args) -> Result<Fields, Failure> {
	let tree = s.tree.take().ok_or_else(|| other("no tree", "open comes first"))?;
	// Where the worker says how far it got, for the coordinator to read when the worker is gone: a
	// byte that is 0 before the close, 1 while it runs and 2 once it returned.
	let state = match a.text("state_file").as_str() {
		"" => None,
		name => {
			let root = crate::opfs_common::root().await;
			let file = crate::opfs_common::create(&root, name).await;
			set_state(&file, 0)?;
			Some(file)
		}
	};
	// The wait is part of the same task as the close, so that a worker that is ended during it is
	// ended, by Chrome, in the middle of the close if the wait is as long as it needs.
	spin(a.number("spin"));
	let started = now();
	if let Some(file) = &state {
		set_state(file, 1)?;
	}
	event("closing", "");
	let closed = tree.close().await;
	if let Some(file) = &state {
		set_state(file, 2)?;
	}
	event("closed", "");
	closed.map_err(|e| engine("close the tree", e))?;
	Ok(vec![("ms", format!("{:.0}", now() - started))])
}

fn set_state(file: &OpfsSyncFile, state: u8) -> Result<(), Failure> {
	file.write_at(0, &[state])
		.map(|_| ())
		.map_err(|e| other("write the state file", format!("{e:?}")))
}

async fn cmd_close(s: &mut Session) -> Result<Fields, Failure> {
	let started = now();
	if s.tree.is_some() {
		cmd_close_tree(s, &Args(Vec::new())).await?;
	}
	if let Some(fs) = s.fs.take() {
		fs.close().await.map_err(|e| engine("close the file system", e))?;
	}
	Ok(vec![("ms", format!("{:.0}", now() - started))])
}

fn cmd_files(s: &Session) -> Result<Fields, Failure> {
	let fs = s.fs()?;
	Ok(vec![
		("tables", fs.list_for_test("db/sstables").len().to_string()),
		(
			"segments",
			fs.list_for_test("db/wal")
				.into_iter()
				.filter(|n| n.ends_with(".wal"))
				.count()
				.to_string(),
		),
		("root", fs.list_for_test("").join(",")),
	])
}

fn cmd_marker(s: &Session) -> Result<Fields, Failure> {
	let fs = s.fs()?;
	Ok(vec![
		("present", (fs.file_len_for_test(MIGRATION_MARKER).is_some() as u8).to_string()),
		("root", fs.list_for_test("").join(",")),
	])
}

/// `migrate_from_indexeddb`, with the plain options or with small pieces (`small=1`) that make the
/// copy last longer.
async fn cmd_migrate(a: &Args) -> Result<Fields, Failure> {
	let (idb, opfs) = (a.text("idb"), a.text("db"));
	let db = Db::from_args(a);
	let started = now();
	event("migrating", "");
	let result = if a.flag("small") {
		let tuning = ImportTuning {
			read: ChunkLimits::new(24 * 1024, 40),
			write: ChunkLimits::new(24 * 1024, 20),
			..ImportTuning::default()
		};
		let (result, report) =
			migrate_from_indexeddb_with_tuning(&idb, &opfs, db.options(), tuning).await;
		log(&format!("{} chunks read, {} transactions", report.chunks, report.transactions));
		result
	} else {
		migrate_from_indexeddb(&idb, &opfs, db.options()).await
	};
	let ms = format!("{:.0}", now() - started);
	match result {
		Ok(IndexedDbMigration::Imported {
			entries,
			bytes,
		}) => {
			event("migrated", "");
			Ok(vec![
				("result", "imported".to_string()),
				("entries", entries.to_string()),
				("bytes", bytes.to_string()),
				("ms", ms),
			])
		}
		Ok(IndexedDbMigration::Existing) => {
			event("migrated", "");
			Ok(vec![("result", "existing".to_string()), ("ms", ms)])
		}
		Ok(IndexedDbMigration::NoSource) => {
			Ok(vec![("result", "nosource".to_string()), ("ms", ms)])
		}
		Ok(_) => Err(other_failure("a result that this test does not know".to_string())),
		Err(e) => Err(engine_after("migrate", e, started)),
	}
}

/// Commits and reads for `secs` seconds, one after the other, and checks every read. Reports the
/// first and the last moment of the work on the clock of the browser.
async fn cmd_work(s: &Session, a: &Args) -> Result<Fields, Failure> {
	let db = s.db()?;
	let tree = s.tree()?;
	let limit_ms = a.number("secs") as f64 * 1000.0;
	let started = now();
	let (mut next, mut commits, mut reads) = (0u64, 0u64, 0u64);
	let mut rng = crate::opfs_common::Rng::new(db.seed);
	while now() - started < limit_ms {
		let mut txn = tree.begin().map_err(|e| engine("begin", e))?;
		if commits % 5 == 0 {
			txn.set_durability(Durability::Immediate);
		}
		for i in next..next + 4 {
			txn.set(db.key(i), db.value(i)).map_err(|e| engine("set", e))?;
			txn.set(db.aux(i), db.aux_value(i)).map_err(|e| engine("set", e))?;
		}
		txn.commit().await.map_err(|e| engine("commit", e))?;
		next += 4;
		commits += 1;
		// Reads of earlier commits, which the writes of this one must not disturb.
		let reader = tree.begin_with_mode(Mode::ReadOnly).map_err(|e| engine("begin", e))?;
		for _ in 0..8 {
			let i = rng.below(next);
			let got = reader.get(db.key(i)).map_err(|e| engine("get", e))?;
			if got.as_deref() != Some(&db.value(i)[..]) {
				return Err(other_failure(format!("a read of index {i} did not give its value")));
			}
			reads += 1;
		}
		if commits % 20 == 0 {
			event("work", &format!("{next}"));
		}
		yield_now().await;
		if commits % 4 == 0 {
			sleep_ms(0.0).await;
		}
	}
	Ok(vec![
		("first", format!("{started:.0}")),
		("last", format!("{:.0}", now())),
		("end", next.to_string()),
		("commits", commits.to_string()),
		("reads", reads.to_string()),
	])
}
