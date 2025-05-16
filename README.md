# rust-kv
A minimal multi-threaded key-value store in Rust based on the [Bitcask](https://en.wikipedia.org/wiki/Bitcask) design.
- WAL based key-value storage with an in-memory index of all keys to their location on disk.
- Basic compaction logic on writes.
- Multi-threaded storage engine.
- Fast and easy last-state recovery.

Ref - [TP 201: Practical Networked Applications in Rust](https://github.com/pingcap/talent-plan/blob/master/courses/rust/projects/project-2/README.md).
