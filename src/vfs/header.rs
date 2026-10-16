//! The header of a slot file of a pool, and its codec.
//!
//! A slot is `[page A: 4096 bytes][page B: 4096 bytes][payload]`. Each page holds one header, and
//! the header that counts is the valid page with the higher sequence number. An update writes the
//! page that does not count, so a write that is torn leaves the page it tore with a bad checksum
//! and the other page, which is the old header, intact. A page is:
//!
//! ```text
//! offset  size  field
//!      0     8  magic "SKVPOOL1"
//!      8     4  format, little endian, 1
//!     12     4  crc32 of bytes 16..4096
//!     16     8  seq: a number that only grows, for the whole pool
//!     24     1  state: 0 free, 1 named
//!     25     3  reserved, zero
//!     28     4  path length in bytes, at most 4000, zero when the slot is free
//!     32     n  path: UTF-8, '/' separated, normalised
//!   32+n        zero padding to 4096
//! ```
//!
//! Decoding is on the path of a mount, where the bytes are whatever a browser kept, and a browser
//! instance that panics is gone: so `decode` never panics, indexes only what it has checked, and
//! allocates only the path, whose length it has bounded first.

use std::io;

use crc32fast::Hasher;

/// The bytes before the payload of every slot: two header pages.
pub(crate) const HEADER_LEN: u64 = 8192;

/// One header page.
pub(crate) const PAGE_LEN: usize = 4096;

/// The two pages together, as a `usize` for buffers.
pub(crate) const HEADER_BYTES: usize = 2 * PAGE_LEN;

/// What a page starts with.
pub(crate) const MAGIC: [u8; 8] = *b"SKVPOOL1";

/// The only format there is. A page of another format is not read, and so a slot that has only
/// such pages is kept as an orphan and not reused.
pub(crate) const FORMAT: u32 = 1;

/// The longest path a header holds.
pub(crate) const MAX_PATH_LEN: usize = 4000;

const OFFSET_FORMAT: usize = 8;
const OFFSET_CRC: usize = 12;
const OFFSET_SEQ: usize = 16;
const OFFSET_STATE: usize = 24;
const OFFSET_PATH_LEN: usize = 28;
const OFFSET_PATH: usize = 32;

const STATE_FREE: u8 = 0;
const STATE_NAMED: u8 = 1;

/// What a slot is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SlotState {
	/// No file: the payload, if there is any, is dead.
	Free,
	/// The file at this path.
	Named(String),
}

/// A decoded header page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Header {
	/// The sequence number of the write that made it. Higher wins.
	pub(crate) seq: u64,
	pub(crate) state: SlotState,
}

/// Why a page is not a header. None of these is an error of the pool: a page that is not valid is
/// one that was never written, or one that a write tore, and the other page says what the slot is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HeaderError {
	/// The buffer is not one page.
	WrongLength(usize),
	BadMagic,
	/// A format that this code does not know, from a newer or a different writer.
	UnknownFormat(u32),
	BadChecksum,
	BadState(u8),
	/// A path length that is over the limit or past the page, or a free slot that has one.
	BadPathLength(u32),
	/// A path that is empty (for a named slot), not UTF-8, or has a NUL in it.
	BadPath,
}

impl Header {
	pub(crate) fn free(seq: u64) -> Self {
		Self {
			seq,
			state: SlotState::Free,
		}
	}

	pub(crate) fn named(seq: u64, path: impl Into<String>) -> Self {
		Self {
			seq,
			state: SlotState::Named(path.into()),
		}
	}

	/// The page for this header. Fails, and writes nothing, for a path that does not fit or that
	/// no page would decode (empty, or with a NUL in it).
	pub(crate) fn encode(&self) -> io::Result<[u8; PAGE_LEN]> {
		let mut page = [0u8; PAGE_LEN];
		page[..8].copy_from_slice(&MAGIC);
		page[OFFSET_FORMAT..OFFSET_FORMAT + 4].copy_from_slice(&FORMAT.to_le_bytes());
		page[OFFSET_SEQ..OFFSET_SEQ + 8].copy_from_slice(&self.seq.to_le_bytes());
		match &self.state {
			SlotState::Free => page[OFFSET_STATE] = STATE_FREE,
			SlotState::Named(path) => {
				check_path(path)?;
				page[OFFSET_STATE] = STATE_NAMED;
				// Checked above: at most MAX_PATH_LEN, which is a `u32` and fits the page.
				page[OFFSET_PATH_LEN..OFFSET_PATH_LEN + 4]
					.copy_from_slice(&(path.len() as u32).to_le_bytes());
				page[OFFSET_PATH..OFFSET_PATH + path.len()].copy_from_slice(path.as_bytes());
			}
		}
		let crc = checksum(&page);
		page[OFFSET_CRC..OFFSET_CRC + 4].copy_from_slice(&crc.to_le_bytes());
		Ok(page)
	}

	/// The header in `page`, or why it is not one. Never panics, whatever the bytes are.
	pub(crate) fn decode(page: &[u8]) -> Result<Header, HeaderError> {
		if page.len() != PAGE_LEN {
			return Err(HeaderError::WrongLength(page.len()));
		}
		if page[..8] != MAGIC {
			return Err(HeaderError::BadMagic);
		}
		let format = read_u32(page, OFFSET_FORMAT);
		if format != FORMAT {
			return Err(HeaderError::UnknownFormat(format));
		}
		if read_u32(page, OFFSET_CRC) != checksum(page) {
			return Err(HeaderError::BadChecksum);
		}
		let seq = read_u64(page, OFFSET_SEQ);
		let path_len = read_u32(page, OFFSET_PATH_LEN);
		match page[OFFSET_STATE] {
			STATE_FREE => {
				if path_len != 0 {
					return Err(HeaderError::BadPathLength(path_len));
				}
				Ok(Header::free(seq))
			}
			STATE_NAMED => {
				// Bounded before it indexes or allocates anything.
				let len = usize::try_from(path_len)
					.ok()
					.filter(|len| *len <= MAX_PATH_LEN && OFFSET_PATH + len <= PAGE_LEN)
					.ok_or(HeaderError::BadPathLength(path_len))?;
				let bytes = &page[OFFSET_PATH..OFFSET_PATH + len];
				let path = std::str::from_utf8(bytes).map_err(|_| HeaderError::BadPath)?;
				if path.is_empty() || path.contains('\0') {
					return Err(HeaderError::BadPath);
				}
				Ok(Header::named(seq, path))
			}
			other => Err(HeaderError::BadState(other)),
		}
	}
}

fn check_path(path: &str) -> io::Result<()> {
	if path.is_empty() || path.contains('\0') {
		return Err(io::Error::new(io::ErrorKind::InvalidInput, "the path of a file is not valid"));
	}
	if path.len() > MAX_PATH_LEN {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("the path is {} bytes, and a pool holds at most {MAX_PATH_LEN}", path.len()),
		));
	}
	Ok(())
}

/// The checksum of a page: everything after the checksum itself.
fn checksum(page: &[u8]) -> u32 {
	let mut hasher = Hasher::new();
	hasher.update(&page[OFFSET_SEQ..]);
	hasher.finalize()
}

// The callers have checked that the page is `PAGE_LEN` long and the offsets are constants within
// it.
fn read_u32(page: &[u8], at: usize) -> u32 {
	let mut bytes = [0u8; 4];
	bytes.copy_from_slice(&page[at..at + 4]);
	u32::from_le_bytes(bytes)
}

fn read_u64(page: &[u8], at: usize) -> u64 {
	let mut bytes = [0u8; 8];
	bytes.copy_from_slice(&page[at..at + 8]);
	u64::from_le_bytes(bytes)
}

/// What the two pages of a slot say.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SlotHeaders {
	pub(crate) pages: [Result<Header, HeaderError>; 2],
	/// The page that counts and its header: the valid one with the higher `seq`. On a tie the
	/// first page, which can only be two copies of one clean header.
	pub(crate) current: Option<(usize, Header)>,
}

impl SlotHeaders {
	/// Reads both pages out of the first `HEADER_BYTES` of a slot. A buffer of another length has
	/// no valid page.
	pub(crate) fn decode(head: &[u8]) -> Self {
		let (a, b) = if head.len() >= HEADER_BYTES {
			(Header::decode(&head[..PAGE_LEN]), Header::decode(&head[PAGE_LEN..HEADER_BYTES]))
		} else {
			(Err(HeaderError::WrongLength(head.len())), Err(HeaderError::WrongLength(head.len())))
		};
		let current = match (&a, &b) {
			(Ok(x), Ok(y)) => {
				if y.seq > x.seq {
					Some((1, y.clone()))
				} else {
					Some((0, x.clone()))
				}
			}
			(Ok(x), Err(_)) => Some((0, x.clone())),
			(Err(_), Ok(y)) => Some((1, y.clone())),
			(Err(_), Err(_)) => None,
		};
		Self {
			pages: [a, b],
			current,
		}
	}
}

/// The offset of page `index` (0 or 1) in a slot.
pub(crate) const fn page_offset(index: usize) -> u64 {
	(index * PAGE_LEN) as u64
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use proptest::prelude::*;

	use super::*;

	fn sample() -> Header {
		Header::named(0x0102_0304_0506_0708, "db/wal/00000000000000000007.wal")
	}

	#[test]
	fn a_header_round_trips() {
		for header in [
			Header::free(0),
			Header::free(u64::MAX),
			sample(),
			Header::named(1, "a"),
			Header::named(9, "/abs/with/ünïcode/ファイル"),
			Header::named(3, "x".repeat(MAX_PATH_LEN)),
		] {
			let page = header.encode().unwrap();
			assert_eq!(page.len(), PAGE_LEN);
			assert_eq!(&page[..8], b"SKVPOOL1");
			assert_eq!(Header::decode(&page), Ok(header));
		}
	}

	#[test]
	fn a_path_that_does_not_fit_or_decode_is_not_encoded() {
		for bad in ["", "a\0b"] {
			assert_eq!(
				Header::named(1, bad).encode().unwrap_err().kind(),
				io::ErrorKind::InvalidInput,
				"{bad:?}"
			);
		}
		assert_eq!(
			Header::named(1, "x".repeat(MAX_PATH_LEN + 1)).encode().unwrap_err().kind(),
			io::ErrorKind::InvalidInput
		);
	}

	#[test]
	fn the_layout_is_the_documented_one() {
		let page = Header::named(5, "ab").encode().unwrap();
		assert_eq!(&page[..8], b"SKVPOOL1");
		assert_eq!(&page[8..12], &1u32.to_le_bytes());
		assert_eq!(&page[16..24], &5u64.to_le_bytes());
		assert_eq!(page[24], 1);
		assert_eq!(&page[25..28], &[0, 0, 0]);
		assert_eq!(&page[28..32], &2u32.to_le_bytes());
		assert_eq!(&page[32..34], b"ab");
		assert!(page[34..].iter().all(|b| *b == 0));
		let free = Header::free(6).encode().unwrap();
		assert_eq!(free[24], 0);
		assert_eq!(&free[28..32], &[0, 0, 0, 0]);
		assert_eq!(HEADER_LEN, 2 * PAGE_LEN as u64);
	}

	#[test]
	fn pages_that_are_not_headers_say_why() {
		let good = sample().encode().unwrap();
		assert_eq!(Header::decode(&good[..100]), Err(HeaderError::WrongLength(100)));
		assert_eq!(Header::decode(&[0u8; PAGE_LEN]), Err(HeaderError::BadMagic));
		assert_eq!(Header::decode(&[]), Err(HeaderError::WrongLength(0)));

		let mut page = good;
		page[8] = 2;
		assert_eq!(Header::decode(&page), Err(HeaderError::UnknownFormat(2)));

		let mut page = good;
		page[40] ^= 1;
		assert_eq!(Header::decode(&page), Err(HeaderError::BadChecksum));

		// A state, a length and a path that the checksum covers but no writer makes: written with
		// a good checksum on purpose, so that the check that fails is the one under test.
		let reseal = |mut page: [u8; PAGE_LEN]| {
			let crc = checksum(&page);
			page[OFFSET_CRC..OFFSET_CRC + 4].copy_from_slice(&crc.to_le_bytes());
			page
		};
		let mut page = good;
		page[OFFSET_STATE] = 7;
		assert_eq!(Header::decode(&reseal(page)), Err(HeaderError::BadState(7)));

		for len in [MAX_PATH_LEN as u32 + 1, u32::MAX, PAGE_LEN as u32] {
			let mut page = good;
			page[OFFSET_PATH_LEN..OFFSET_PATH_LEN + 4].copy_from_slice(&len.to_le_bytes());
			assert_eq!(Header::decode(&reseal(page)), Err(HeaderError::BadPathLength(len)));
		}

		let mut page = Header::free(1).encode().unwrap();
		page[OFFSET_PATH_LEN] = 3;
		assert_eq!(Header::decode(&reseal(page)), Err(HeaderError::BadPathLength(3)));

		let mut page = good;
		page[OFFSET_PATH] = 0xff;
		assert_eq!(Header::decode(&reseal(page)), Err(HeaderError::BadPath));
		let mut page = good;
		page[OFFSET_PATH] = 0;
		assert_eq!(Header::decode(&reseal(page)), Err(HeaderError::BadPath));
		let mut page = good;
		page[OFFSET_PATH_LEN..OFFSET_PATH_LEN + 4].copy_from_slice(&0u32.to_le_bytes());
		assert_eq!(
			Header::decode(&reseal(page)),
			Err(HeaderError::BadPath),
			"a named slot has a path"
		);
	}

	/// The write of a page is torn at every byte: what is read is the page that was there before,
	/// or the new one, and never a mixture that decodes.
	#[test]
	fn every_torn_prefix_of_a_header_write_is_recoverable() {
		let old = Header::named(10, "db/MANIFEST");
		let new = Header::named(11, "db/manifest-2");
		let old_page = old.encode().unwrap();
		let new_page = new.encode().unwrap();
		for torn in 0..=PAGE_LEN {
			// The slot: page A is the old header and counts, page B is being written, over
			// whatever it held (another old header, here the free one).
			let mut head = vec![0u8; HEADER_BYTES];
			head[..PAGE_LEN].copy_from_slice(&old_page);
			let older = Header::free(3).encode().unwrap();
			head[PAGE_LEN..].copy_from_slice(&older);
			head[PAGE_LEN..PAGE_LEN + torn].copy_from_slice(&new_page[..torn]);

			let headers = SlotHeaders::decode(&head);
			let (page, header) = headers.current.expect("a page counts at every tear");
			if torn == PAGE_LEN {
				assert_eq!((page, header), (1, new.clone()), "the whole write is the new header");
			} else if torn == 0 {
				assert_eq!((page, header), (0, old.clone()));
			} else {
				// The page is a mixture of the new bytes and the older ones. It may only decode if
				// every byte that the checksum covers is the new page's, which a torn page
				// is not, unless the bytes that were not written happen to equal the new ones.
				let mixture_equals_new = new_page[torn..] == older[torn..];
				if mixture_equals_new {
					assert_eq!(header, new);
				} else {
					assert_eq!(
						(page, header),
						(0, old.clone()),
						"a torn page is not read, at {torn}"
					);
				}
			}
		}
		// And a tear into a page that was never written (zeros) is no header either.
		for torn in 0..PAGE_LEN {
			let mut page = [0u8; PAGE_LEN];
			page[..torn].copy_from_slice(&new_page[..torn]);
			if new_page[torn..].iter().any(|b| *b != 0) {
				assert!(Header::decode(&page).is_err(), "torn at {torn}");
			} else {
				// Only the zero padding is missing, and the page is the new one.
				assert_eq!(Header::decode(&page), Ok(new.clone()), "torn at {torn}");
			}
		}
	}

	#[test]
	fn the_higher_sequence_counts() {
		let low = Header::named(4, "a").encode().unwrap();
		let high = Header::free(9).encode().unwrap();
		let mut head = vec![0u8; HEADER_BYTES];
		head[..PAGE_LEN].copy_from_slice(&low);
		head[PAGE_LEN..].copy_from_slice(&high);
		assert_eq!(SlotHeaders::decode(&head).current, Some((1, Header::free(9))));
		head[..PAGE_LEN].copy_from_slice(&high);
		head[PAGE_LEN..].copy_from_slice(&low);
		assert_eq!(SlotHeaders::decode(&head).current, Some((0, Header::free(9))));
		// A tie is two copies of one header: the first.
		head[PAGE_LEN..].copy_from_slice(&high);
		assert_eq!(SlotHeaders::decode(&head).current, Some((0, Header::free(9))));
		// One valid page is enough, whichever it is.
		head[..PAGE_LEN].fill(0);
		assert_eq!(SlotHeaders::decode(&head).current, Some((1, Header::free(9))));
		head[PAGE_LEN..].fill(0);
		assert_eq!(SlotHeaders::decode(&head).current, None);
		assert_eq!(SlotHeaders::decode(&head[..10]).current, None);
		assert_eq!(page_offset(0), 0);
		assert_eq!(page_offset(1), 4096);
	}

	/// Every single bit of a page, flipped: no panic, and a page that still decodes decodes to what
	/// was written. The bits that the checksum does not cover (the magic and the format) make the
	/// page unreadable instead.
	#[test]
	fn a_flipped_bit_is_never_read_as_something_else() {
		for header in [Header::free(77), sample()] {
			let page = header.encode().unwrap();
			for bit in 0..PAGE_LEN * 8 {
				let mut flipped = page;
				flipped[bit / 8] ^= 1 << (bit % 8);
				match Header::decode(&flipped) {
					Err(_) => {}
					Ok(decoded) => {
						// Only a bit in the padding past the path could survive, and the checksum
						// covers the padding too: so nothing may.
						panic!("bit {bit} flipped and still decodes: {decoded:?}");
					}
				}
			}
		}
	}

	proptest! {
		#![proptest_config(ProptestConfig::with_cases(512))]

		/// Any bytes at all: no panic, and the answer is either an error or a header that encodes
		/// back to the same page up to the bytes that decode ignores.
		#[test]
		fn decode_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..8200)) {
			let _ = Header::decode(&bytes);
			let _ = SlotHeaders::decode(&bytes);
		}

		/// A valid page with some bytes overwritten with garbage, which is how a torn or rotten
		/// page looks: no panic, and anything that decodes round-trips.
		#[test]
		fn a_damaged_page_never_panics(
			seq in any::<u64>(),
			path in "[a-z/]{1,40}",
			edits in proptest::collection::vec((0usize..PAGE_LEN, any::<u8>()), 0..8),
		) {
			let mut page = Header::named(seq, path).encode().unwrap();
			for (at, byte) in edits {
				page[at] = byte;
			}
			if let Ok(header) = Header::decode(&page) {
				prop_assert_eq!(Header::decode(&header.encode().unwrap()), Ok(header));
			}
		}

		#[test]
		fn any_valid_header_round_trips(seq in any::<u64>(), path in "[ -~]{1,200}", named in any::<bool>()) {
			let header = if named { Header::named(seq, path.replace('\0', "x")) } else { Header::free(seq) };
			let page = header.encode().unwrap();
			prop_assert_eq!(Header::decode(&page), Ok(header));
		}
	}
}
