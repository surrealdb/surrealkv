//! Additive public surface for the branch-native engine during integration.

pub use crate::{
	AuthorityFence as BranchAuthorityFence, BranchGeneration, BranchId, CommitTimestamp,
	CommitVersion, DatabaseId, ErrorCode as DatabaseErrorCode, KernelError as DatabaseError,
	KernelResult as DatabaseResult, ReadSelector as BranchReadSelector, WriteOperation,
};
