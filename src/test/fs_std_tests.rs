//! The file system seam over `std::fs`: the contract that every backend runs, the provided
//! methods of the trait on their own, what `StdFs` keeps exactly as the engine has it today, the
//! refusals of a file system with nothing mounted, and the plumbing of the options.

use std::io;
use std::path::Path;
use std::sync::Arc;

use tempfile::TempDir;

use crate::error::Error;
use crate::lockfile::LockFile;
use crate::vfs::{
	self,
	contract,
	CreateOpts,
	DirEntry,
	EntryKind,
	Fs,
	FsFile,
	FsKind,
	FsWriter,
	IntoFsFile,
	StdFs,
	UnavailableFs,
};
use crate::wal::{Options as WalOptions, WalContext};
use crate::{FileSystem, Options, Result, TreeBuilder};

/// `StdFs` with every method that the trait can provide left to the trait, and a kind of the
/// caller's choice. The contract runs over it too, which tests the provided methods, and a pool's
/// kind lets the options be tested before a pool exists.
#[derive(Debug)]
struct Provided(FsKind);

impl Fs for Provided {
	fn kind(&self) -> FsKind {
		self.0
	}

	fn entry(&self, path: &Path) -> io::Result<Option<EntryKind>> {
		StdFs.entry(path)
	}

	fn create_dir_all(&self, path: &Path) -> io::Result<()> {
		StdFs.create_dir_all(path)
	}

	fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntry>> {
		StdFs.read_dir(path)
	}

	fn remove_file(&self, path: &Path) -> io::Result<()> {
		StdFs.remove_file(path)
	}

	fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
		StdFs.remove_dir_all(path)
	}

	fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
		StdFs.rename(from, to)
	}

	fn sync_dir(&self, path: &Path) -> io::Result<()> {
		StdFs.sync_dir(path)
	}

	fn open_read(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		StdFs.open_read(path)
	}

	fn open_append(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		StdFs.open_append(path, opts)
	}

	fn create_new(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		StdFs.create_new(path, opts)
	}

	fn create_truncate(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		StdFs.create_truncate(path, opts)
	}

	fn replace_atomic(&self, path: &Path, content: &[u8]) -> Result<()> {
		StdFs.replace_atomic(path, content)
	}

	fn lock_file(&self, dir: &Path) -> LockFile {
		StdFs.lock_file(dir)
	}
}

#[test]
fn std_fs_honours_the_contract() {
	let dir = TempDir::new().unwrap();
	assert_eq!(StdFs.kind(), FsKind::Std);
	contract::run_all(&StdFs, dir.path());
}

#[test]
fn the_provided_methods_honour_the_contract() {
	let dir = TempDir::new().unwrap();
	contract::run_all(&Provided(FsKind::Std), dir.path());
}

#[test]
fn std_fs_answers_as_path_does_where_an_error_is_not_absence() {
	let dir = TempDir::new().unwrap();
	let file = dir.path().join("f");
	std::fs::write(&file, b"x").unwrap();
	// A path below a file cannot be told: `try_exists` fails, `exists` reads false.
	let below = file.join("child");
	for fs in [&StdFs as &dyn Fs, &Provided(FsKind::Std)] {
		assert_eq!(fs.try_exists(&below).is_err(), below.try_exists().is_err());
		assert_eq!(fs.exists(&below), below.exists());
		assert_eq!(fs.is_dir(&below), below.is_dir());
		assert_eq!(fs.is_file(&below), below.is_file());
		assert_eq!(fs.entry(&below).is_err(), below.try_exists().is_err());
		assert_eq!(fs.exists(&file), file.exists());
		assert_eq!(fs.is_file(dir.path()), dir.path().is_file());
	}
	#[cfg(unix)]
	assert!(StdFs.try_exists(&below).is_err(), "ENOTDIR is not absence");
}

#[test]
fn only_sync_file_feeds_the_ledger_of_synced_files() {
	let dir = TempDir::new().unwrap();
	let (through_file, through_path) = (dir.path().join("a"), dir.path().join("b"));
	let file = StdFs.create_truncate(&through_file, CreateOpts::default()).unwrap();
	file.append(b"x").unwrap();
	file.sync().unwrap();
	assert!(!vfs::sync_tracker::was_synced(&through_file), "FsFile::sync records nothing");

	StdFs.create_truncate(&through_path, CreateOpts::default()).unwrap().append(b"x").unwrap();
	StdFs.sync_file(&through_path).unwrap();
	assert!(vfs::sync_tracker::was_synced(&through_path), "sync_file is vfs::fsync_file");

	// The ledger of synced directories is shared by every test, so this looks for this one.
	let canonical = dir.path().canonicalize().unwrap();
	StdFs.sync_dir(dir.path()).unwrap();
	assert!(crate::lsm::SYNCED_DIRECTORIES.lock().contains(&canonical));
}

#[test]
fn a_file_that_was_not_opened_by_std_fs_is_cut_back_without_a_hole() {
	let dir = TempDir::new().unwrap();
	// What `IntoFsFile` is for: the tests and the write-ahead log hand over a `std::fs::File`.
	for append in [false, true] {
		let path = dir.path().join(format!("f{append}"));
		let mut options = std::fs::OpenOptions::new();
		options.read(true).write(true).create(true);
		if append {
			options.append(true);
		}
		let file = options.open(&path).unwrap().into_fs_file();
		file.append(b"hello world").unwrap();
		file.set_len(5).unwrap();
		file.append(b"!").unwrap();
		assert_eq!(std::fs::read(&path).unwrap(), b"hello!", "append: {append}");
	}
}

// A read open is read-only on Unix; on Windows `open_for_sync` opens it for writing too.
#[cfg(unix)]
#[test]
fn a_file_opened_to_read_is_not_written() {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("f");
	std::fs::write(&path, b"abcd").unwrap();
	let file = StdFs.open_read(&path).unwrap();
	assert!(file.append(b"no").is_err());
	assert_eq!(std::fs::read(&path).unwrap(), b"abcd");
	let mut writer = FsWriter(file);
	assert!(io::Write::write(&mut writer, b"x").is_err());
}

#[cfg(unix)]
#[test]
fn creations_apply_the_mode_they_are_given() {
	use std::os::unix::fs::PermissionsExt;

	let dir = TempDir::new().unwrap();
	let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
	// The WAL's modes, as `open_wal_file` and `create_new_wal_file` give them.
	let (a, b, c) = (dir.path().join("a"), dir.path().join("b"), dir.path().join("c"));
	StdFs.open_append(&a, CreateOpts::new(Some(0o600))).unwrap();
	StdFs.create_new(&b, CreateOpts::new(Some(0o640))).unwrap();
	StdFs.create_truncate(&c, CreateOpts::new(Some(0o600))).unwrap();
	assert_eq!((mode(&a), mode(&b), mode(&c)), (0o600, 0o640, 0o600));
}

#[test]
fn a_file_coerces_to_the_read_seam_and_keeps_its_length() {
	let dir = TempDir::new().unwrap();
	let file: Arc<dyn FsFile> =
		StdFs.create_truncate(&dir.path().join("f"), CreateOpts::default()).unwrap();
	file.append(b"0123").unwrap();
	let read: Arc<dyn vfs::File> = file;
	assert_eq!(read.size().unwrap(), 4);
	let mut buf = [0u8; 2];
	read.read_exact_at(1, &mut buf).unwrap();
	assert_eq!(&buf, b"12");
}

#[test]
fn spares_are_never_wanted_and_a_std_fs_never_runs_out() {
	assert!(StdFs.ensure_spares(usize::MAX).is_none());
	assert_eq!(StdFs.pool_exhaustions(), 0);
	assert!(StdFs.check_available().is_ok());
}

// ---------------------------------------------------------------------------------------------
// A file system with nothing mounted
// ---------------------------------------------------------------------------------------------

#[test]
fn an_unavailable_file_system_refuses_everything_with_a_typed_error() {
	vfs::unavailable::assert_refuses_everything(
		&UnavailableFs::no_file_system(),
		"no file system is mounted",
	);
	vfs::unavailable::assert_refuses_everything(&UnavailableFs::new("custom reason"), "custom");
}

#[tokio::test]
async fn building_over_an_unavailable_file_system_fails_before_it_creates_anything() {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("db");
	let fs = FileSystem(Arc::new(UnavailableFs::no_file_system()));
	let options = Options::default().with_path(path.clone()).with_file_system(fs);
	let built = TreeBuilder::with_options(options).build_async().await;
	match built {
		Err(Error::Io(e)) => {
			assert_eq!(e.kind(), io::ErrorKind::Unsupported);
			assert!(e.to_string().contains("no file system is mounted"), "{e}");
		}
		Err(other) => panic!("a typed refusal was expected, got {other:?}"),
		Ok(_) => panic!("a tree was built over nothing"),
	}
	assert!(!path.exists(), "nothing was created");
}

/// The sync entry points are refused too: the options validate (a default browser build has
/// nothing mounted and its options are still valid), and it is the build that says there is no
/// file system, before the first `exists` can answer `false`.
#[test]
fn the_sync_build_over_nothing_is_refused_and_creates_nothing() {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("db");
	let nothing = || FileSystem(Arc::new(UnavailableFs::no_file_system()));
	Options::default().with_file_system(nothing()).validate().expect("valid options");

	let refused = |built: Result<()>| match built {
		Err(Error::Io(e)) => {
			assert_eq!(e.kind(), io::ErrorKind::Unsupported);
			assert!(e.to_string().contains("no file system is mounted"), "{e}");
		}
		other => panic!("a typed refusal was expected, got {other:?}"),
	};
	refused(
		TreeBuilder::new().with_path(path.clone()).with_file_system(nothing()).build().map(|_| ()),
	);
	refused(
		TreeBuilder::new()
			.with_path(path.clone())
			.with_file_system(nothing())
			.build_with_options()
			.map(|_| ()),
	);
	// `FileSystem::memory()` is a stub until the pool exists, and refuses in the same way: it must
	// not fall through to the files of the operating system.
	let built = TreeBuilder::new()
		.with_path(path.clone())
		.with_file_system(FileSystem::memory())
		.build()
		.map(|_| ());
	match built {
		Err(Error::Io(e)) => assert!(e.to_string().contains("not implemented yet"), "{e}"),
		other => panic!("{other:?}"),
	}
	assert!(!path.exists(), "nothing was created");
}

#[tokio::test]
async fn the_memory_file_system_says_it_is_not_there_yet() {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("db");
	let built = TreeBuilder::new()
		.with_path(path.clone())
		.with_file_system(FileSystem::memory())
		.build_async()
		.await;
	match built {
		Err(Error::Io(e)) => {
			assert_eq!(e.kind(), io::ErrorKind::Unsupported);
			assert!(e.to_string().contains("not implemented yet"), "{e}");
		}
		other => panic!("{:?}", other.map(|_| ())),
	}
	assert!(!path.exists());
}

// ---------------------------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------------------------

#[test]
fn the_default_file_system_is_std_on_native() {
	assert_eq!(Options::default().fs.kind(), FsKind::Std);
	assert_eq!(vfs::default_fs().kind(), FsKind::Std);
	assert_eq!(FileSystem::std().0.kind(), FsKind::Std);
	assert_eq!(WalContext::default().fs.kind(), FsKind::Std);
	assert_eq!(WalOptions::default().context.fs.kind(), FsKind::Std);
	// One value is shared by the defaults, so that none of them allocates one.
	assert!(Arc::ptr_eq(&vfs::default_fs(), &vfs::default_fs()));
}

#[tokio::test]
async fn with_file_system_sets_the_file_system_and_the_log_takes_it_from_the_options() {
	let fs = FileSystem(Arc::new(Provided(FsKind::Std)));
	let options = Options::default().with_file_system(fs.clone());
	assert!(Arc::ptr_eq(&options.fs, &fs.0));
	let context = WalContext::for_options(&options).unwrap();
	assert!(Arc::ptr_eq(&context.fs, &fs.0), "the log is in the file system of the tree");
	let wal = WalOptions::for_tree(&options).unwrap();
	assert!(Arc::ptr_eq(&wal.context.fs, &fs.0));
	let dir = TempDir::new().unwrap();
	let (tree, built_with) = TreeBuilder::new()
		.with_path(dir.path().join("db"))
		.with_file_system(fs.clone())
		.build_with_options_async()
		.await
		.expect("a tree over a file system that is std underneath");
	assert!(Arc::ptr_eq(&built_with.fs, &fs.0), "the builder passes the file system on");
	tree.close().await.unwrap();
	assert!(format!("{fs:?}").contains("Provided"));
}

#[tokio::test]
async fn a_pool_is_refused_until_the_engine_is_ported_to_it() {
	let complete = std::hint::black_box(vfs::POOL_FS_PORT_COMPLETE);
	assert!(!complete, "this test goes when the constant is set");
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("db");
	let pool = || FileSystem(Arc::new(Provided(FsKind::Pool)));
	let options = Options::default().with_path(path.clone()).with_file_system(pool());
	assert!(matches!(options.validate(), Err(Error::InvalidArgument(_))));
	let built = TreeBuilder::with_options(options).build_async().await;
	assert!(matches!(built, Err(Error::InvalidArgument(_))), "{:?}", built.map(|_| ()));
	let built = TreeBuilder::new().with_path(path.clone()).with_file_system(pool()).build();
	assert!(matches!(built, Err(Error::InvalidArgument(_))), "{:?}", built.map(|_| ()));
	assert!(!path.exists(), "nothing was created");
	// The other kinds are valid options: an unavailable file system is refused by the build,
	// where it says why.
	for kind in [FsKind::Std, FsKind::Unavailable] {
		let fs = FileSystem(Arc::new(Provided(kind)));
		Options::default().with_file_system(fs).validate().unwrap();
	}
}

#[tokio::test]
async fn the_async_build_is_the_sync_build_on_native() {
	let dir = TempDir::new().unwrap();
	let tree = TreeBuilder::new().with_path(dir.path().join("db")).build_async().await.unwrap();
	tree.close().await.unwrap();

	let (tree, options) = TreeBuilder::new()
		.with_path(dir.path().join("db"))
		.build_with_options_async()
		.await
		.expect("and again, with its options");
	assert_eq!(options.fs.kind(), FsKind::Std);
	tree.close().await.unwrap();
}

/// The futures of both async builders are `Send` on native, so that a multi-threaded runtime can
/// run them: this is a compile-time test, and `build_async` must stay `Send` as the file system
/// mount and the retry loop of a later change are added to it. On a browser nothing is required to
/// be `Send`.
#[test]
fn build_async_is_send() {
	fn assert_send<T: Send>(_: &T) {}
	assert_send(&TreeBuilder::new().build_async());
	assert_send(&TreeBuilder::new().build_with_options_async());
	// And so are the pieces a build is made of.
	fn assert_send_sync<T: Send + Sync + ?Sized>() {}
	assert_send_sync::<dyn Fs>();
	assert_send_sync::<dyn FsFile>();
	assert_send_sync::<FileSystem>();
	assert_send_sync::<LockFile>();
	assert_send(&None::<vfs::SpareWait>);
}
