/// SSTable identifier. All SSTs (from flush or compaction) use globally unique ULIDs.
pub type SstId = ulid::Ulid;
