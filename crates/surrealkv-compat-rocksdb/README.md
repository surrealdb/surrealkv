# surrealkv-compat-rocksdb

Pure-Rust reader of the RocksDB databases that SurrealDB writes, for migrating them to SurrealKV
(RocksDB 8 to 11).

## Features

- **No RocksDB**: no `librocksdb` linkage and no C++ of RocksDB's. The table and log formats are read in Rust (Zstandard decompression uses the `zstd` crate, which builds the C library).
- **What RocksDB's own recovery reads**: the live tables that the MANIFEST (found through CURRENT)
  lists, the blob files their values live in, and the write-ahead logs that no table covers,
  including the size records of user-defined timestamps.
- **Table formats**: BlockBasedTable format versions 2 through 7 (both footers; crc32c, xxhash,
  xxhash64 and xxh3 checksums; delta-encoded index values; two-level partitioned indexes).
- **Compression**: Zstandard, Snappy, LZ4 and uncompressed blocks.
- **SurrealDB timestamps**: with the timestamp comparator that SurrealDB uses in versioned mode
  (the 8-byte little-endian suffix, newest first), every stored version is returned with its
  timestamp, ordered by key, then newest timestamp, then newest write.
- **Refuses what it does not read**: other comparators, other column families, range deletes, merge
  operands, single deletes, tables without their timestamps, compression dictionaries and zlib,
  bzip2 and xpress compression are refused with `Error::Unsupported`, and not read wrongly.
- **Locking**: `lock_directory` takes RocksDB's own lock on a database directory, so that a
  database that is open in RocksDB is not read.
