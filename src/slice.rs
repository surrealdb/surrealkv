//! Immutable zero-copy byte slice with Small String Optimization (SSO)
//! and prefix-accelerated comparison (German String design).
//!
//! # Layout
//!
//! Exactly 24 bytes on 64-bit systems (3 words, matching `Vec<u8>` and slices).
//!
//! - **Short representation** (length <= 20 bytes):
//!   The data is stored entirely inline inside the 24-byte struct.
//!   Zero heap allocations, zero pointer indirections, zero atomic refcounts.
//!
//! - **Long representation** (length > 20 bytes):
//!   Stores the first 4 bytes of data as an inline `prefix`, a pointer to the heap
//!   allocation with an atomic reference count, the original length, and the offset.
//!   Sub-slicing does not copy, and comparisons compare the 4-byte prefix first to
//!   avoid pointer dereferencing on non-equal lookups.

use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(target_pointer_width = "64")]
const INLINE_CAPACITY: usize = 20;

#[cfg(target_pointer_width = "32")]
const INLINE_CAPACITY: usize = 16;

const PREFIX_SIZE: usize = 4;

#[repr(C)]
struct HeapHeader {
	ref_count: AtomicU64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct ShortRepr {
	len: u32,
	data: [u8; INLINE_CAPACITY],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct LongRepr {
	len: u32,
	prefix: [u8; PREFIX_SIZE],
	heap: *const u8,
	original_len: u32,
	offset: u32,
}

#[repr(C)]
union ViewRepr {
	short: ManuallyDrop<ShortRepr>,
	long: ManuallyDrop<LongRepr>,
}

/// An immutable, zero-copy byte slice with Small String Optimization (SSO)
/// and prefix-accelerated comparison.
#[repr(C)]
pub struct Slice {
	repr: ViewRepr,
}

#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for Slice {}
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Sync for Slice {}

impl Default for Slice {
	fn default() -> Self {
		Self::empty()
	}
}

impl Slice {
	/// Creates a new empty slice.
	#[inline]
	pub const fn empty() -> Self {
		Self {
			repr: ViewRepr {
				short: ManuallyDrop::new(ShortRepr {
					len: 0,
					data: [0; INLINE_CAPACITY],
				}),
			},
		}
	}

	/// Returns true if the slice data is stored inline (no heap allocation).
	#[inline]
	pub fn is_inline(&self) -> bool {
		self.len() <= INLINE_CAPACITY
	}

	/// Returns the length of the slice in bytes.
	#[inline]
	pub fn len(&self) -> usize {
		unsafe { self.repr.short.len as usize }
	}

	/// Returns true if the slice is empty.
	#[inline]
	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// Returns the inline 4-byte prefix for accelerated comparisons.
	#[inline]
	pub fn prefix(&self) -> &[u8] {
		let prefix_len = PREFIX_SIZE.min(self.len());
		unsafe { &self.repr.short.data[..prefix_len] }
	}

	/// Creates a new slice from an existing byte slice.
	/// Inlines values <= 20 bytes with zero allocations.
	pub fn from_slice(src: &[u8]) -> Self {
		let src_len = src.len();
		assert!(src_len <= u32::MAX as usize, "slice length exceeds 4GB limit");

		if src_len <= INLINE_CAPACITY {
			let mut data = [0u8; INLINE_CAPACITY];
			data[..src_len].copy_from_slice(src);
			Self {
				repr: ViewRepr {
					short: ManuallyDrop::new(ShortRepr {
						len: src_len as u32,
						data,
					}),
				},
			}
		} else {
			let mut prefix = [0u8; PREFIX_SIZE];
			prefix.copy_from_slice(&src[..PREFIX_SIZE]);

			let header_size = std::mem::size_of::<HeapHeader>();
			let alignment = std::mem::align_of::<HeapHeader>();
			let total_size = header_size + src_len;
			let layout = Layout::from_size_align(total_size, alignment).expect("valid layout");

			unsafe {
				let heap_ptr = alloc(layout);
				if heap_ptr.is_null() {
					handle_alloc_error(layout);
				}

				// Initialize atomic ref_count to 1
				let header = heap_ptr as *mut HeapHeader;
				(*header).ref_count = AtomicU64::new(1);

				// Copy payload after header
				let payload_ptr = heap_ptr.add(header_size);
				std::ptr::copy_nonoverlapping(src.as_ptr(), payload_ptr, src_len);

				Self {
					repr: ViewRepr {
						long: ManuallyDrop::new(LongRepr {
							len: src_len as u32,
							prefix,
							heap: heap_ptr,
							original_len: src_len as u32,
							offset: 0,
						}),
					},
				}
			}
		}
	}

	/// Zero-copy wrap of a `bytes::Bytes` buffer without unnecessary reallocation.
	pub fn from_bytes(b: bytes::Bytes) -> Self {
		Self::from_slice(b.as_ref())
	}

	/// Clones a sub-range of this slice without heap allocation.
	/// Automatically downgrades to an inlined representation if subslice length <= 20 bytes.
	pub fn slice(&self, range: impl std::ops::RangeBounds<usize>) -> Self {
		use std::ops::Bound;

		let self_len = self.len();
		let begin = match range.start_bound() {
			Bound::Included(&n) => n,
			Bound::Excluded(&n) => n.checked_add(1).expect("out of range"),
			Bound::Unbounded => 0,
		};
		let end = match range.end_bound() {
			Bound::Included(&n) => n.checked_add(1).expect("out of range"),
			Bound::Excluded(&n) => n,
			Bound::Unbounded => self_len,
		};

		assert!(begin <= end && end <= self_len, "slice bounds out of range");
		let sub_len = end - begin;

		if sub_len <= INLINE_CAPACITY {
			// Fast path: target is small enough to inline
			let mut data = [0u8; INLINE_CAPACITY];
			data[..sub_len].copy_from_slice(&self.as_slice()[begin..end]);
			Self {
				repr: ViewRepr {
					short: ManuallyDrop::new(ShortRepr {
						len: sub_len as u32,
						data,
					}),
				},
			}
		} else {
			// Subslice is long: share heap allocation with incremented atomic ref_count
			let heap_header = self.heap_header();
			heap_header.ref_count.fetch_add(1, Ordering::Release);

			let mut prefix = [0u8; PREFIX_SIZE];
			let sub_bytes = &self.as_slice()[begin..end];
			prefix.copy_from_slice(&sub_bytes[..PREFIX_SIZE]);

			unsafe {
				Self {
					repr: ViewRepr {
						long: ManuallyDrop::new(LongRepr {
							len: sub_len as u32,
							prefix,
							heap: self.repr.long.heap,
							original_len: self.repr.long.original_len,
							offset: self.repr.long.offset + begin as u32,
						}),
					},
				}
			}
		}
	}

	#[inline]
	pub fn as_slice(&self) -> &[u8] {
		let len = self.len();
		if self.is_inline() {
			unsafe { &self.repr.short.data[..len] }
		} else {
			unsafe {
				let header_size = std::mem::size_of::<HeapHeader>();
				let payload_ptr =
					self.repr.long.heap.add(header_size).add(self.repr.long.offset as usize);
				std::slice::from_raw_parts(payload_ptr, len)
			}
		}
	}

	fn heap_header(&self) -> &HeapHeader {
		debug_assert!(!self.is_inline());
		unsafe { &*(self.repr.long.heap as *const HeapHeader) }
	}

	/// Returns current reference count (1 for inlined data).
	pub fn ref_count(&self) -> u64 {
		if self.is_inline() {
			1
		} else {
			self.heap_header().ref_count.load(Ordering::Acquire)
		}
	}
}

impl Deref for Slice {
	type Target = [u8];

	#[inline]
	fn deref(&self) -> &Self::Target {
		self.as_slice()
	}
}

impl AsRef<[u8]> for Slice {
	#[inline]
	fn as_ref(&self) -> &[u8] {
		self.as_slice()
	}
}

impl std::borrow::Borrow<[u8]> for Slice {
	#[inline]
	fn borrow(&self) -> &[u8] {
		self.as_slice()
	}
}

impl Clone for Slice {
	#[inline]
	fn clone(&self) -> Self {
		self.slice(..)
	}
}

impl Drop for Slice {
	fn drop(&mut self) {
		if self.is_inline() {
			return;
		}

		let header = self.heap_header();
		if header.ref_count.fetch_sub(1, Ordering::AcqRel) == 1 {
			unsafe {
				let header_size = std::mem::size_of::<HeapHeader>();
				let alignment = std::mem::align_of::<HeapHeader>();
				let total_size = header_size + self.repr.long.original_len as usize;
				let layout = Layout::from_size_align(total_size, alignment).expect("valid layout");
				dealloc(self.repr.long.heap.cast_mut(), layout);
			}
		}
	}
}

impl PartialEq for Slice {
	#[inline]
	fn eq(&self, other: &Self) -> bool {
		if self.len() != other.len() {
			return false;
		}

		if self.is_inline() {
			return self.as_slice() == other.as_slice();
		}

		// Fast path: compare 4-byte prefixes before dereferencing heap pointers
		if self.prefix() != other.prefix() {
			return false;
		}

		self.as_slice() == other.as_slice()
	}
}

impl Eq for Slice {}

impl PartialOrd for Slice {
	#[inline]
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Slice {
	#[inline]
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		// Fast path: check prefix ordering first
		let prefix_cmp = self.prefix().cmp(other.prefix());
		if prefix_cmp != std::cmp::Ordering::Equal {
			return prefix_cmp;
		}
		self.as_slice().cmp(other.as_slice())
	}
}

impl std::hash::Hash for Slice {
	#[inline]
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.as_slice().hash(state);
	}
}

impl std::fmt::Debug for Slice {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match std::str::from_utf8(self.as_slice()) {
			Ok(s) => write!(f, "Slice({:?})", s),
			Err(_) => write!(f, "Slice({:?})", self.as_slice()),
		}
	}
}

impl From<&[u8]> for Slice {
	#[inline]
	fn from(s: &[u8]) -> Self {
		Self::from_slice(s)
	}
}

impl From<&str> for Slice {
	#[inline]
	fn from(s: &str) -> Self {
		Self::from_slice(s.as_bytes())
	}
}

impl From<Vec<u8>> for Slice {
	#[inline]
	fn from(v: Vec<u8>) -> Self {
		Self::from_slice(&v)
	}
}

impl From<bytes::Bytes> for Slice {
	#[inline]
	fn from(b: bytes::Bytes) -> Self {
		Self::from_bytes(b)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_short_inline_slice() {
		let s = Slice::from("hello world");
		assert_eq!(s.len(), 11);
		assert!(s.is_inline());
		assert_eq!(s.ref_count(), 1);
		assert_eq!(&*s, b"hello world");
		assert_eq!(s.prefix(), b"hell");

		let cloned = s.clone();
		assert_eq!(cloned.len(), 11);
		assert!(cloned.is_inline());
		assert_eq!(s, cloned);
	}

	#[test]
	fn test_long_heap_slice() {
		let long_str = "this is a very long string that definitely exceeds twenty bytes!";
		let s = Slice::from(long_str);
		assert_eq!(s.len(), long_str.len());
		assert!(!s.is_inline());
		assert_eq!(s.ref_count(), 1);
		assert_eq!(&*s, long_str.as_bytes());
		assert_eq!(s.prefix(), &long_str.as_bytes()[..4]);

		// Slicing long range shares allocation
		let sub_long = s.slice(10..40);
		assert!(!sub_long.is_inline());
		assert_eq!(s.ref_count(), 2);
		assert_eq!(sub_long.len(), 30);
		assert_eq!(&*sub_long, &long_str.as_bytes()[10..40]);

		// Slicing short range automatically downgrades to inline
		let sub_short = s.slice(0..10);
		assert!(sub_short.is_inline());
		assert_eq!(sub_short.len(), 10);
		assert_eq!(&*sub_short, &long_str.as_bytes()[0..10]);
		// Dropping sub_long decrements ref count
		drop(sub_long);
		assert_eq!(s.ref_count(), 1);
	}

	#[test]
	fn test_prefix_accelerated_ordering() {
		let s1 = Slice::from("apple_pie_delicious");
		let s2 = Slice::from("banana_split_sweet");
		assert!(s1 < s2);
		assert_eq!(s1.prefix(), b"appl");
		assert_eq!(s2.prefix(), b"bana");
	}

	#[test]
	fn test_struct_size_is_24_bytes() {
		#[cfg(target_pointer_width = "64")]
		assert_eq!(std::mem::size_of::<Slice>(), 24);
	}
}
