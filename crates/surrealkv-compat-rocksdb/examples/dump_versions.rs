//! Prints every version of a RocksDB directory as `key ts kind len` lines, for debugging.
use std::path::PathBuf;

use surrealkv_compat_rocksdb::{Payload, RocksDb, VersionKind};

fn main() {
	let dir = PathBuf::from(std::env::args().nth(1).expect("a database directory"));
	let db = RocksDb::open(&dir).expect("open");
	println!(
		"# tables={} wal_files={} cutoff={:?} last_sequence={}",
		db.table_count(),
		db.wal_files.len(),
		db.manifest.history_cutoff(),
		db.manifest.last_sequence
	);
	for v in db.versions().expect("versions") {
		let v = v.expect("version");
		let len = match &v.payload {
			Payload::Inline(b) => b.len() as i64,
			Payload::Blob(r) => r.size as i64,
			Payload::None => -1,
		};
		let hex: String = v.user_key.iter().map(|b| format!("{b:02x}")).collect();
		println!(
			"{hex}\t{}\t{}\t{:?}\t{len}",
			v.timestamp.unwrap_or(0),
			v.sequence,
			v.kind == VersionKind::Delete
		);
	}
}
