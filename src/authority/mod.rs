//! FK1 durable authority: numbered immutable metadata lineages.
//!
//! Design: docs/FK_AUTHORITY_FORK_DESIGN.md (FINAL v4). The catalog lineage is
//! the sole authority for branch existence/generation/anchors/TTLs; per-branch
//! state lineages carry only owned durable facts; the root lineage carries
//! global recovery facts. All files are `magic + version + body + crc32`,
//! published by conditional create (hard link), never rewritten in place.

pub(crate) mod format;
pub(crate) mod publish;
pub(crate) mod store;

#[cfg(test)]
mod format_tests;
#[cfg(test)]
mod golden_tests;
#[cfg(test)]
mod publish_tests;
