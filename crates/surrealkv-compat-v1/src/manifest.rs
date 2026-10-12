//! The manifest of an LSMV1 database: a frozen copy of the read side of v0.21.4's
//! `levels/mod.rs`, which is the same in v0.20.0 to v0.21.4. Everything is big endian.
//!
//! ```text
//! version (u16, 1) | next table id (u64) | log number (u64) | last sequence number (u64)
//! number of levels (u8) | per level: number of tables (u32), then the table ids (u64 each)
//! number of snapshots (u32) | per snapshot: length (u32), then seq (u64) and created_at (u128)
//! ```
//!
//! The snapshots were the points that `Tree::snapshot` had registered; the crate does not return
//! them, but checks they are all there.

use crate::{Error, Manifest, Result};

const MANIFEST_VERSION: u16 = 1;

struct Cursor<'a> {
	bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
	fn take(&mut self, n: usize) -> Result<&'a [u8]> {
		if self.bytes.len() < n {
			return Err(Error::Corrupted(format!(
				"the manifest ends where {n} more bytes were expected"
			)));
		}
		let (head, rest) = self.bytes.split_at(n);
		self.bytes = rest;
		Ok(head)
	}

	fn u8(&mut self) -> Result<u8> {
		Ok(self.take(1)?[0])
	}

	fn u16(&mut self) -> Result<u16> {
		Ok(u16::from_be_bytes(self.take(2)?.try_into().expect("2 bytes")))
	}

	fn u32(&mut self) -> Result<u32> {
		Ok(u32::from_be_bytes(self.take(4)?.try_into().expect("4 bytes")))
	}

	fn u64(&mut self) -> Result<u64> {
		Ok(u64::from_be_bytes(self.take(8)?.try_into().expect("8 bytes")))
	}
}

pub(crate) fn read_manifest(bytes: &[u8]) -> Result<Manifest> {
	let mut cursor = Cursor {
		bytes,
	};
	let version = cursor.u16()?;
	if version != MANIFEST_VERSION {
		return Err(Error::Unsupported(format!(
			"the manifest has version {version}, not the {MANIFEST_VERSION} of LSMV1"
		)));
	}
	let next_table_id = cursor.u64()?;
	let log_number = cursor.u64()?;
	let last_sequence = cursor.u64()?;

	let levels = cursor.u8()?;
	let mut level_data = Vec::with_capacity(usize::from(levels));
	for _ in 0..levels {
		let tables = cursor.u32()? as usize;
		// The ids are in the manifest, so a count that is more than it holds is damage, and
		// there is no allocating for it.
		let ids = cursor.take(tables.checked_mul(8).ok_or_else(|| {
			Error::Corrupted("the manifest lists more tables than can be".into())
		})?)?;
		level_data.push(
			(0..tables)
				.map(|i| u64::from_be_bytes(ids[i * 8..i * 8 + 8].try_into().expect("8 bytes")))
				.collect(),
		);
	}

	let snapshots = cursor.u32()?;
	for _ in 0..snapshots {
		let len = cursor.u32()? as usize;
		cursor.take(len)?;
	}

	Ok(Manifest {
		version,
		level_data,
		next_table_id,
		last_sequence,
		log_number,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	pub(crate) fn encode(levels: &[&[u64]], snapshots: usize) -> Vec<u8> {
		let mut out = Vec::new();
		out.extend_from_slice(&1u16.to_be_bytes());
		out.extend_from_slice(&9u64.to_be_bytes());
		out.extend_from_slice(&4u64.to_be_bytes());
		out.extend_from_slice(&77u64.to_be_bytes());
		out.push(levels.len() as u8);
		for level in levels {
			out.extend_from_slice(&(level.len() as u32).to_be_bytes());
			for id in *level {
				out.extend_from_slice(&id.to_be_bytes());
			}
		}
		out.extend_from_slice(&(snapshots as u32).to_be_bytes());
		for _ in 0..snapshots {
			out.extend_from_slice(&24u32.to_be_bytes());
			out.extend_from_slice(&[0; 24]);
		}
		out
	}

	#[test]
	fn reads_the_levels_and_the_header() {
		let bytes = encode(&[&[3, 1], &[], &[2]], 2);
		let manifest = read_manifest(&bytes).unwrap();
		assert_eq!(
			manifest,
			Manifest {
				version: 1,
				level_data: vec![vec![3, 1], vec![], vec![2]],
				next_table_id: 9,
				last_sequence: 77,
				log_number: 4,
			}
		);
	}

	#[test]
	fn refuses_other_versions_and_cut_manifests() {
		let bytes = encode(&[&[1, 2, 3]], 1);
		for cut in 0..bytes.len() {
			assert!(read_manifest(&bytes[..cut]).is_err(), "cut at {cut}");
		}
		let mut v2 = bytes.clone();
		v2[1] = 2;
		assert!(matches!(read_manifest(&v2), Err(Error::Unsupported(_))));
		// A table count that the manifest cannot hold does not allocate.
		let mut huge = bytes;
		huge[27..31].copy_from_slice(&u32::MAX.to_be_bytes());
		assert!(read_manifest(&huge).is_err());
	}
}
