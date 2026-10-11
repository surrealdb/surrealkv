//! # Sealed tables
//!
//! What turns the layout of `block_io` into an encrypted table: the sealer that a file's blocks
//! go through, and the starter that makes a file's header and sealer when its first block is
//! written.
//!
//! ## A sealed table
//!
//! ```text
//! +-------------------+----------------+-----+----------------+----------+
//! | header (92 bytes) | sealed block 1 | ... | sealed block N | footer   |
//! +-------------------+----------------+-----+----------------+----------+
//! ```
//!
//! The header is the crypto header of the file, written in clear: the suite, the key id, the
//! identity of the database, the kind and id of the file, and the salt that the key of the file
//! was derived with. Every block of every kind is compressed and then sealed, and what a block
//! is bound to is the file, what kind of block it is, where it is in the file and the compression
//! byte of its trailer, so a block that is moved, replayed or read as another kind does not open.
//! Only the header and the footer are clear, and neither holds a key or anything of the data.
//!
//! The position of a block is the byte offset of its sealed bytes in the file. A cipher derives its
//! nonce from it and refuses to seal at a position that is not after the last one, so the writer
//! of a file has its own sealer and a position is sealed at most once under a key.

use std::sync::Arc;

use crate::error::Result;
use crate::sstable::block_io::{BlockContext, BlockKind, BlockSealer, FileStart, FileStarter};
use crate::tde::{self, EncryptionContext, FileCipher, FileKind, HEADER_LEN, TAG_LEN};

/// The kind of block that a cipher binds a block of `kind` to.
///
/// Written out for each kind instead of converted by number, so that a kind that is added to one
/// side is a compile error here and not a silent change of what is bound.
const fn bound_kind(kind: BlockKind) -> tde::BlockKind {
	match kind {
		BlockKind::Data => tde::BlockKind::SstData,
		BlockKind::IndexPartition => tde::BlockKind::SstIndexPartition,
		BlockKind::TopLevelIndex => tde::BlockKind::SstTopLevelIndex,
		BlockKind::FilterPartition => tde::BlockKind::SstFilterPartition,
		BlockKind::FilterIndex => tde::BlockKind::SstFilterIndex,
		BlockKind::MetaIndex => tde::BlockKind::SstMetaIndex,
	}
}

fn bound(context: &BlockContext) -> tde::BlockContext {
	tde::BlockContext::new(bound_kind(context.kind), context.offset, context.compression)
}

/// Seals and opens the blocks of one SSTable with the cipher of that file.
///
/// The one that writes is made with the file, from a key that was generated for it, and is held
/// by one sink and no other. The ones that read are made from the header of an existing file and
/// cannot seal.
pub(crate) struct SstSealer {
	cipher: FileCipher,
}

impl SstSealer {
	pub(crate) fn new(cipher: FileCipher) -> Self {
		Self {
			cipher,
		}
	}
}

impl BlockSealer for SstSealer {
	fn overhead(&self) -> usize {
		TAG_LEN
	}

	fn seal(&self, context: &BlockContext, block: &mut Vec<u8>) -> Result<()> {
		// Room for the tag, so that appending it does not copy the block again.
		block.reserve(TAG_LEN);
		self.cipher.seal_append(&bound(context), block)?;
		Ok(())
	}

	fn open(&self, context: &BlockContext, block: &mut Vec<u8>) -> Result<()> {
		match self.cipher.open_suffix(&bound(context), block.as_mut_slice()) {
			Ok(plain) => {
				let len = plain.len();
				block.truncate(len);
				Ok(())
			}
			Err(e) => {
				// Nothing that was read is left for a caller that does not look at the error.
				block.clear();
				Err(e.into())
			}
		}
	}
}

/// Starts the file of table `table_id` of an encrypted database: a fresh salt, a key of its own,
/// and the header that says so.
///
/// Every table writer has one and it is used once, whatever the id is: an id can come back after a
/// crash or a failed compaction, and the file that is written for it then is another file with
/// another salt and another key.
pub(crate) struct SstStarter {
	context: Arc<EncryptionContext>,
	table_id: u64,
}

impl SstStarter {
	pub(crate) fn new(context: Arc<EncryptionContext>, table_id: u64) -> Self {
		Self {
			context,
			table_id,
		}
	}
}

impl FileStarter for SstStarter {
	fn header_len(&self) -> usize {
		HEADER_LEN
	}

	fn start(self: Box<Self>) -> Result<FileStart> {
		// The header has no fields of its own to cover: the table keeps nothing of its own in it.
		let (header, cipher) = self.context.create_file(FileKind::Sst, self.table_id, &[])?;
		Ok(FileStart {
			header: header.encode().to_vec(),
			sealer: Arc::new(SstSealer::new(cipher)),
		})
	}
}
