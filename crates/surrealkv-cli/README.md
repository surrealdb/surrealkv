# surrealkv-cli

`skv` is the command-line tool for [SurrealKV](https://github.com/surrealdb/surrealkv): it inspects databases, dumps manifests and SSTables, scrubs checksums, reads encrypted databases, and migrates RocksDB and SurrealKV (up to v0.21.4) databases into a new SurrealKV database.

```bash
cargo install surrealkv-cli

skv inspect path/to/db
skv scrub path/to/db
skv migrate path/to/rocksdb_dir path/to/new_surrealkv_db
```

See the [SurrealKV README](https://github.com/surrealdb/surrealkv#command-line-tool-skv) for every command.
