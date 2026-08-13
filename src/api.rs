//! Public identifiers, receipts, and structured errors for the branch-native engine.

use std::fmt;

macro_rules! id16 {
	($name:ident) => {
		#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
		pub struct $name(pub [u8; 16]);

		impl $name {
			#[cfg_attr(not(test), allow(dead_code))]
			pub const fn from_u128(value: u128) -> Self {
				Self(value.to_be_bytes())
			}
		}
	};
}

macro_rules! id32 {
	($name:ident) => {
		#[cfg_attr(not(test), allow(dead_code))]
		#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
		pub struct $name(pub [u8; 32]);
	};
}

id16!(DatabaseId);
id16!(BranchId);
id16!(OperationId);
id16!(SessionId);
id32!(TableId);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchGeneration(pub u64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitVersion(pub u64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitTimestamp(pub u64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AuthorityFence(pub u64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MonotonicTime(pub(crate) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DurabilityClass {
	Ephemeral,
	CrashDurable,
}

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
