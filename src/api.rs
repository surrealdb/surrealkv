//! Public identifiers, receipts, and structured errors for the branch-native engine.

use std::fmt;

macro_rules! id16 {
	($name:ident) => {
		#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
		pub struct $name(pub [u8; 16]);

		impl $name {
			pub const fn from_u128(value: u128) -> Self {
				Self(value.to_be_bytes())
			}
		}
	};
}

id16!(BranchId);

impl BranchId {
	/// Reserved identity of the always-existing default branch ("main").
	/// The default branch must be addressable before any durable catalog
	/// exists (fresh stores, recovery, replay fencing), so its id is a
	/// well-known constant rather than a minted identifier.
	/// `BatchOwner::DEFAULT` and the open-time catalog check both derive
	/// from this single definition; user branches never receive it.
	pub(crate) const DEFAULT: Self = Self([0; 16]);
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchGeneration(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
	InvalidArgument,
	NotFound,
	AlreadyExists,
	Conflict,
	Fenced,
	CapabilityMismatch,
	Corruption,
	ResourceExhausted,
	Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KernelError {
	pub code: ErrorCode,
	pub message: String,
}

impl KernelError {
	pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
		Self {
			code,
			message: message.into(),
		}
	}
}

impl fmt::Display for KernelError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:?}: {}", self.code, self.message)
	}
}

impl std::error::Error for KernelError {}

pub type KernelResult<T> = std::result::Result<T, KernelError>;
