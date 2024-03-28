# surrealkv

surrealkv is a versioned, low-level, embedded key-value database implemented in Rust. This ACID-compliant database offers the following features:

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
- [ ] **Built-in Item Versioning API**
- [x] **Multi-Version Concurrency Control (MVCC) support**
- [x] **Multiple Concurrent Readers and Writers**
- [x] **Persistence through an append-only File**

## Important Notice

This project is actively evolving, and as such, there might be changes to the file format, APIs, and feature set in future releases until reaching stability. Developers are encouraged to stay informed about updates and review future release notes for any breaking changes.

Feel free to contribute, provide feedback, or report issues to help shape the future of surrealkv. Thank you for your interest and involvement in this project!
