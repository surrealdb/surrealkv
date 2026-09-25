//! Diagnostics, inspection, and integrity scrubbing utilities for SurrealKV.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::levels::LevelManifest;
use crate::sstable::index_block::IndexIterator;
use crate::sstable::table::{IndexType, Table, TABLE_FULL_FOOTER_LENGTH};
use crate::vfs::File;
use crate::{LSMIterator, Options};

/// General database storage inspection summary.
#[derive(Debug, Clone)]
pub struct DbInfo {
	pub path: PathBuf,
	pub manifest_version: u16,
	pub next_table_id: u64,
	pub log_number: u64,
	pub last_sequence: u64,
	pub total_tables: usize,
	pub total_sst_bytes: u64,
	pub total_wal_bytes: u64,
	pub total_vlog_bytes: u64,
	pub levels: Vec<LevelSummary>,
	pub is_locked: bool,
	pub lock_pid: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct LevelSummary {
	pub level: usize,
	pub table_count: usize,
	pub total_bytes: u64,
}

/// Detailed manifest state.
#[derive(Debug, Clone)]
pub struct ManifestInfo {
	pub version: u16,
	pub next_table_id: u64,
	pub log_number: u64,
	pub last_sequence: u64,
	pub levels: Vec<LevelDetail>,
	pub snapshots: Vec<SnapshotSummary>,
}

#[derive(Debug, Clone)]
pub struct LevelDetail {
	pub level: usize,
	pub tables: Vec<TableSummary>,
}

#[derive(Debug, Clone)]
pub struct TableSummary {
	pub id: u64,
	pub file_size: u64,
	pub smallest_key: Vec<u8>,
	pub largest_key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct SnapshotSummary {
	pub id: u64,
	pub sequence: u64,
	pub timestamp: u64,
}

/// Detailed SSTable inspection information.
#[derive(Debug, Clone)]
pub struct SstInfo {
	pub path: PathBuf,
	pub file_size: u64,
	pub format_version: u8,
	pub checksum_type: u8,
	pub total_entries: u64,
	pub raw_key_size: u64,
	pub raw_value_size: u64,
	pub data_blocks_count: u64,
	pub index_blocks_count: u64,
	pub filter_size: u64,
	pub created_at: u128,
	pub keys: Option<Vec<SstKeyEntry>>,
	pub verified_blocks: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct SstKeyEntry {
	pub user_key: Vec<u8>,
	pub seq_num: u64,
	pub kind: String,
	pub value_preview: Vec<u8>,
}

/// WAL segments summary.
#[derive(Debug, Clone)]
pub struct WalInfo {
	pub segment_files: Vec<WalSegmentSummary>,
	pub total_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct WalSegmentSummary {
	pub file_name: String,
	pub file_size: u64,
}

/// Comprehensive database integrity scrub report.
#[derive(Debug, Clone, Default)]
pub struct ScrubReport {
	pub total_tables: usize,
	pub verified_tables: usize,
	pub total_blocks: usize,
	pub corrupt_blocks: usize,
	pub errors: Vec<String>,
}

/// Inspects a database directory and returns overall storage health.
pub fn inspect_db<P: AsRef<Path>>(db_path: P) -> Result<DbInfo> {
	let path = db_path.as_ref().to_path_buf();
	let opts = Arc::new(Options {
		path: path.clone(),
		..Default::default()
	});

	// Check lock status
	let lock_path = opts.path.join("LOCK");
	#[cfg_attr(target_arch = "wasm32", allow(unused_mut))]
	let mut is_locked = false;
	let mut lock_pid = None;

	if lock_path.exists() {
		if let Ok(content) = fs::read_to_string(&lock_path) {
			if let Ok(pid) = content.trim().parse::<u32>() {
				lock_pid = Some(pid);
			}
		}
		// On non-wasm32, check if lock is actively held
		#[cfg(not(target_arch = "wasm32"))]
		{
			if let Ok(f) = fs::File::open(&lock_path) {
				if crate::lockfile::try_lock_exclusive(&f).is_err() {
					is_locked = true;
				} else {
					let _ = crate::lockfile::unlock(&f);
				}
			}
		}
	}

	// Calculate directory sizes
	let mut total_sst_bytes = 0u64;
	let mut total_wal_bytes = 0u64;
	let mut total_vlog_bytes = 0u64;

	if let Ok(entries) = fs::read_dir(opts.sstable_dir()) {
		for e in entries.flatten() {
			if e.path().extension().is_some_and(|ext| ext == "sst") {
				if let Ok(meta) = e.metadata() {
					total_sst_bytes += meta.len();
				}
			}
		}
	}

	if let Ok(entries) = fs::read_dir(opts.wal_dir()) {
		for e in entries.flatten() {
			if let Ok(meta) = e.metadata() {
				total_wal_bytes += meta.len();
			}
		}
	}

	if let Ok(entries) = fs::read_dir(opts.vlog_dir()) {
		for e in entries.flatten() {
			if let Ok(meta) = e.metadata() {
				total_vlog_bytes += meta.len();
			}
		}
	}

	// Read manifest
	let manifest_path = opts.manifest_file_path(0);
	if !manifest_path.exists() {
		return Ok(DbInfo {
			path,
			manifest_version: 0,
			next_table_id: 0,
			log_number: 0,
			last_sequence: 0,
			total_tables: 0,
			total_sst_bytes,
			total_wal_bytes,
			total_vlog_bytes,
			levels: Vec::new(),
			is_locked,
			lock_pid,
		});
	}

	let manifest = LevelManifest::load_from_file(&manifest_path, opts)?;
	let mut total_tables = 0;
	let mut levels = Vec::new();

	for (lvl_idx, lvl) in manifest.levels.get_levels().iter().enumerate() {
		let table_count = lvl.tables.len();
		total_tables += table_count;
		let total_bytes: u64 = lvl.tables.iter().map(|t| t.file_size).sum();
		levels.push(LevelSummary {
			level: lvl_idx,
			table_count,
			total_bytes,
		});
	}

	Ok(DbInfo {
		path,
		manifest_version: manifest.manifest_format_version,
		next_table_id: manifest.next_table_id.load(std::sync::atomic::Ordering::Relaxed),
		log_number: manifest.log_number,
		last_sequence: manifest.last_sequence,
		total_tables,
		total_sst_bytes,
		total_wal_bytes,
		total_vlog_bytes,
		levels,
		is_locked,
		lock_pid,
	})
}

/// Dumps detailed manifest state.
pub fn dump_manifest<P: AsRef<Path>>(path: P) -> Result<ManifestInfo> {
	let p = path.as_ref();
	let db_dir = if p.is_dir() {
		p.to_path_buf()
	} else {
		p.parent().and_then(|parent| parent.parent()).unwrap_or(p).to_path_buf()
	};

	let opts = Arc::new(Options {
		path: db_dir,
		..Default::default()
	});

	let manifest_path = opts.manifest_file_path(0);
	let manifest = LevelManifest::load_from_file(&manifest_path, opts)?;

	let mut levels = Vec::new();
	for (lvl_idx, lvl) in manifest.levels.get_levels().iter().enumerate() {
		let mut tables = Vec::new();
		for t in &lvl.tables {
			let smallest =
				t.meta.smallest_point.as_ref().map(|k| k.user_key.clone()).unwrap_or_default();
			let largest =
				t.meta.largest_point.as_ref().map(|k| k.user_key.clone()).unwrap_or_default();
			tables.push(TableSummary {
				id: t.id,
				file_size: t.file_size,
				smallest_key: smallest,
				largest_key: largest,
			});
		}
		levels.push(LevelDetail {
			level: lvl_idx,
			tables,
		});
	}

	let snapshots = manifest
		.snapshots
		.iter()
		.map(|s| SnapshotSummary {
			id: 0,
			sequence: s.seq_num,
			timestamp: s.created_at as u64,
		})
		.collect();

	Ok(ManifestInfo {
		version: manifest.manifest_format_version,
		next_table_id: manifest.next_table_id.load(std::sync::atomic::Ordering::Relaxed),
		log_number: manifest.log_number,
		last_sequence: manifest.last_sequence,
		levels,
		snapshots,
	})
}

/// Dumps deep inspection information from an SSTable.
pub fn dump_sst<P: AsRef<Path>>(
	path: P,
	dump_keys: bool,
	verify_checksum: bool,
) -> Result<SstInfo> {
	let path = path.as_ref().to_path_buf();
	let sys_file = fs::File::open(&path)?;
	let file_size = sys_file.metadata()?.len();

	if file_size < TABLE_FULL_FOOTER_LENGTH as u64 {
		return Err(Error::Corruption("SSTable file is too small for footer".into()));
	}

	let file: Arc<dyn File> = Arc::new(sys_file);
	let opts = Arc::new(Options {
		path: path.parent().and_then(|p| p.parent()).unwrap_or(&path).to_path_buf(),
		..Default::default()
	});

	let table = Table::new(1, opts, Arc::clone(&file), file_size)?;

	let props = &table.meta.properties;
	let total_entries = props.num_entries;
	let raw_key_size = props.raw_key_size;
	let raw_value_size = props.raw_value_size;
	let data_blocks_count = props.num_data_blocks;
	let index_blocks_count = props.index_partitions;
	let filter_size = props.filter_size;
	let created_at = props.created_at;

	let mut verified_count = 0;
	let mut keys = if dump_keys {
		Some(Vec::new())
	} else {
		None
	};

	let IndexType::Partitioned(ref partitioned) = table.index_block;
	let mut index_iter = IndexIterator::new(partitioned);
	index_iter.seek_to_first()?;

	while index_iter.valid() {
		let handle = index_iter.block_handle()?;
		let block = table.read_block(&handle)?;
		verified_count += 1;

		if let Some(ref mut k_list) = keys {
			let mut b_iter = block.iter()?;
			b_iter.seek_first()?;

			while b_iter.valid() {
				let ikey = b_iter.key();
				let val = b_iter.value_encoded()?;
				k_list.push(SstKeyEntry {
					user_key: ikey.user_key().to_vec(),
					seq_num: ikey.seq_num(),
					kind: format!("{:?}", ikey.kind()),
					value_preview: val[..val.len().min(32)].to_vec(),
				});
				b_iter.next()?;
			}
		}

		index_iter.next()?;
	}

	Ok(SstInfo {
		path,
		file_size,
		format_version: props.table_format as u8,
		checksum_type: 1, // CRC32c
		total_entries,
		raw_key_size,
		raw_value_size,
		data_blocks_count,
		index_blocks_count,
		filter_size,
		created_at,
		keys,
		verified_blocks: if verify_checksum {
			Some(verified_count)
		} else {
			None
		},
	})
}

/// Dumps summary of WAL segments.
pub fn dump_wal<P: AsRef<Path>>(path: P) -> Result<WalInfo> {
	let p = path.as_ref();
	let wal_dir = if p.is_dir() && p.join("wal").exists() {
		p.join("wal")
	} else if p.is_dir() {
		p.to_path_buf()
	} else {
		p.parent().unwrap_or(p).to_path_buf()
	};

	let mut segment_files = Vec::new();
	let mut total_bytes = 0;

	if let Ok(entries) = fs::read_dir(wal_dir) {
		for e in entries.flatten() {
			let name = e.file_name().to_string_lossy().to_string();
			if name.ends_with(".wal") {
				if let Ok(meta) = e.metadata() {
					let len = meta.len();
					total_bytes += len;
					segment_files.push(WalSegmentSummary {
						file_name: name,
						file_size: len,
					});
				}
			}
		}
	}

	segment_files.sort_by(|a, b| a.file_name.cmp(&b.file_name));
	Ok(WalInfo {
		segment_files,
		total_bytes,
	})
}

/// Comprehensively verifies CRC32 checksums of all active SSTables in a database.
pub fn scrub_db<P: AsRef<Path>>(db_path: P) -> Result<ScrubReport> {
	let path = db_path.as_ref();
	let opts = Options {
		path: path.to_path_buf(),
		..Default::default()
	};
	let tables_dir = opts.sstable_dir();
	let mut report = ScrubReport::default();

	let entries = fs::read_dir(tables_dir)?;
	for entry in entries.flatten() {
		let p = entry.path();
		if p.extension().is_some_and(|ext| ext == "sst") {
			report.total_tables += 1;
			match dump_sst(&p, false, true) {
				Ok(info) => {
					report.verified_tables += 1;
					if let Some(blocks) = info.verified_blocks {
						report.total_blocks += blocks;
					}
				}
				Err(e) => {
					report.corrupt_blocks += 1;
					report.errors.push(format!("{}: {e}", p.display()));
				}
			}
		}
	}

	Ok(report)
}
