# surrealkv

surrealkv is a versioned, low-level, persistent, embedded key-value database implemented in Rust. This ACID-compliant database offers the following features:

- **Transaction Support**: SurrealKV supports rich transactions, allowing multiple items to be inserted, updated, or deleted simultaneously. These transactions are applied atomically, ensuring atomicity and consistency in line with ACID principles. This makes updates invisible until they are committed.

- **Isolation**: SurrealKV provides two levels of isolation - Snapshot Isolation and Serializable Snapshot Isolation - to prevent concurrent transactions from interfering with each other.

- **Durability**: SurrealKV ensures durability by persisting data on disk, protecting against data loss in the event of a system failure.

- **Multi-Version Concurrency Control (MVCC)**: SurrealKV employs immutable versioned adaptive radix tries via [vart](https://github.com/surrealdb/vart). This allows for any number of concurrent readers and writers to operate without blocking each other.

For more information on the underlying immutable versioned adaptive radix tries used, visit [vart](https://github.com/surrealdb/vart).


[![License](https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square)](https://github.com/surrealdb/surrealkv)

## Features

- [x] **In-memory Index**
- [x] **Embeddable**
- [x] **ACID Semantics** 
- [x] **Transaction Support:** 
- [x] **Built-in Item Versioning API**
- [x] **Multi-Version Concurrency Control (MVCC) support**
- [x] **Multiple Concurrent Readers and Writers**
- [x] **Persistence through an append-only File**
- [x] **Compaction**
