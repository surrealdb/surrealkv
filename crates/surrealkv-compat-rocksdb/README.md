# surrealkv-compat-rocksdb

Pure-Rust reader and migration engine for RocksDB BlockBasedTable SSTables (v2 through v7).

## Features

- **100% Pure Rust**: Zero C++ code, zero `librocksdb` linkage, zero CMake / Clang dependencies.
- **Modern RocksDB Format Support**: Automatically parses BlockBasedTable footers across format versions 2 through 7.
- **Compression**: Decompresses Zstandard (`zstd`), Snappy (`snap`), LZ4 (`lz4_flex`), and uncompressed blocks.
- **SurrealDB UDT Awareness**: Automatically detects and strips 8-byte User-Defined Timestamp suffixes from SurrealDB RocksDB keys.
- **K-Way Merge Resolution**: Merges multiple SSTables in a RocksDB directory, yielding only the latest version of each key and discarding tombstones.
