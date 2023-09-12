# surrealkv

surrealkv is a versioned, low-level, embedded, key-value database in rust. It persists to disk, is ACID compliant, and supports multiple readers and writers.

[![](https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square)](https://github.com/surrealdb/surrealkv) 



Features
========

- In-memory database
- Embeddable
- ACID semantics with rich transaction support with rollbacks
- Built-in item versioning
- Multi-version concurrency control
- Multiple concurrent readers and writers
- Durable append-only file format for persistence (with WAL support)
