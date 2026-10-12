//! The footer of a table: the last 50 bytes of the file, a frozen copy of v0.21.4's.
//!
//! ```text
//! 0   1   format (1 = LSMV1)
//! 1   1   checksum type (1 = CRC-32)
//! 2   ..  handle of the meta-index block (two varints)
//! ..  ..  handle of the top-level index block (two varints)
//! 42  8   magic 0x57fb808b247547db
//! ```
//!
//! The current format (LSMV2) keeps the layout of the footer and the magic and says 2 in the
//! format byte, which is what tells the two apart without reading a block. It also uses the
//! checksum type 2 for tables whose blocks are sealed, which this crate cannot read.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use crate::block::Handle;
use crate::{Error, Result};

pub(crate) const FOOTER_LEN: usize = 50;
const MAGIC: [u8; 8] = [0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb];

const NO_HANDLE: Handle = Handle {
	offset: 0,
	size: 0,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Footer {
	pub format: u8,
	pub checksum: u8,
	/// Only meaningful when the format and the checksum type are LSMV1's: no other footer has its
	/// handles decoded.
	pub meta_index: Handle,
	pub index: Handle,
}

impl Footer {
	/// Whether the table is of the format this crate reads, as far as the footer says.
	pub(crate) fn is_lsmv1(&self) -> bool {
		self.format == 1 && self.checksum == 1
	}
}

/// Reads the footer of the table in `file`, `len` bytes long.
pub(crate) fn read_footer(file: &mut File, len: u64) -> Result<Footer> {
	if len < FOOTER_LEN as u64 {
		return Err(Error::Corrupted(format!("a table of {len} bytes is smaller than its footer")));
	}
	let mut buf = [0u8; FOOTER_LEN];
	file.seek(SeekFrom::Start(len - FOOTER_LEN as u64))?;
	file.read_exact(&mut buf)?;
	decode(&buf)
}

fn decode(buf: &[u8; FOOTER_LEN]) -> Result<Footer> {
	if buf[FOOTER_LEN - 8..] != MAGIC {
		return Err(Error::Corrupted("the footer of the table has the wrong magic".into()));
	}
	let format = buf[0];
	let checksum = buf[1];
	if format != 1 || checksum != 1 {
		return Ok(Footer {
			format,
			checksum,
			meta_index: NO_HANDLE,
			index: NO_HANDLE,
		});
	}
	let bad = || Error::Corrupted("the footer holds a bad block handle".into());
	let (meta_index, n) = Handle::decode(&buf[2..FOOTER_LEN - 8]).ok_or_else(bad)?;
	let (index, _) = Handle::decode(&buf[2 + n..FOOTER_LEN - 8]).ok_or_else(bad)?;
	Ok(Footer {
		format,
		checksum,
		meta_index,
		index,
	})
}
