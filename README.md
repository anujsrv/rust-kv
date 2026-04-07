# rust-kv
A minimal multi-threaded key-value store in Rust based on the [Bitcask](https://en.wikipedia.org/wiki/Bitcask) design.
- WAL based key-value storage with an in-memory index of all keys to their location on disk.
- Basic compaction logic on writes.
- Multi-threaded storage engine.
- Fast and easy last-state recovery.

# Architecture:

This section covers every major component: the on-disk log format, the WAL lifecycle, the in-memory index, compaction, multi-threading, client-server networking, and how Rust's type system is leveraged throughout.

---

## Table of Contents

1. [High-Level Overview](#1-high-level-overview)
2. [Module Structure](#2-module-structure)
3. [Data Model and Entry Format](#3-data-model-and-entry-format)
4. [Write-Ahead Log (WAL)](#4-write-ahead-log-wal)
5. [In-Memory Index](#5-in-memory-index)
6. [Store: The Core Engine](#6-store-the-core-engine)
7. [Read Path](#7-read-path)
8. [Write Path](#8-write-path)
9. [Remove Path](#9-remove-path)
10. [Compaction](#10-compaction)
11. [Startup and Recovery](#11-startup-and-recovery)
12. [Multi-Threading Model](#12-multi-threading-model)
13. [Thread Pool](#13-thread-pool)
14. [Client-Server Networking](#14-client-server-networking)
15. [Generics and Trait System](#15-generics-and-trait-system)
16. [Error Handling](#16-error-handling)
17. [Key Design Trade-offs and Limitations](#17-key-design-trade-offs-and-limitations)

---

## 1. High-Level Overview

```
  kvs-client (CLI)
       |
       | TCP (JSON over socket)
       v
  kvs-server (TCP Listener)
       |
       | ThreadPool: dispatches each connection to a worker thread
       v
  KvsServer<K, V, E: KvsEngine>
       |
       | calls get/set/remove
       v
  KvStore<K, V>          <-- thin facade
       |
       v
  Store<K, V>            <-- the real engine
    |           |
    |           v
    |    Arc<Mutex<Writer>>  -- serialized append to active .log file
    |
    v
  Arc<SkipMap<K, EntryOffset>>  -- lock-free in-memory index
    |
    v
  RefCell<HashMap<u32, Reader>>  -- per-clone file descriptor pool
```

All mutations are appended to an active log file on disk (WAL). An in-memory `SkipMap` maps every live key to its byte range inside a specific log file. Reads seek directly to that offset — no scan required. When stale data accumulates past a threshold, compaction rewrites only live entries into a new file.

---

## 2. Module Structure

```
src/
  lib.rs              -- public re-exports
  entry.rs            -- Entry<K,V> enum + EntryOffset struct
  error.rs            -- unified Error enum + Result alias
  resource.rs         -- Request<K,V> and Response<V> (network protocol)
  client.rs           -- KvsClient (TCP client)
  server.rs           -- KvsServer<K,V,E> (TCP server + dispatch)
  threadpool.rs       -- hand-rolled ThreadPool
  engines/
    mod.rs            -- KvsEngine<K,V> trait
    kvs.rs            -- KvStore<K,V>: implements KvsEngine via Store
    store.rs          -- Store<K,V>, Writer, Reader, compaction logic

src/bin/
  kvs-server.rs       -- CLI entry point for the server
  kvs-client.rs       -- CLI entry point for the client
  kvs.rs              -- single-process CLI (non-networked mode)
```

---

## 3. Data Model and Entry Format

### Entry enum (`src/entry.rs`)

Every operation written to disk is represented as a tagged JSON object:

```rust
pub enum Entry<K, V> {
    Set { key: K, val: V },
    Rm  { key: K },
}
```

Serialized with `serde_json`, a `Set` entry looks like:

```json
{"Set":{"key":"foo","val":"bar"}}
```

And a tombstone `Rm` looks like:

```json
{"Rm":{"key":"foo"}}
```

Entries are written as raw bytes, one after another, with **no length prefix or separator**. The `serde_json` streaming deserializer (`Deserializer::into_iter`) is used at read time to parse back-to-back JSON values from a byte stream, relying on the self-delimiting nature of JSON.

### EntryOffset struct

```rust
pub struct EntryOffset {
    pub file_id: u32,   // which .log file holds this entry
    pub start:   u64,   // byte offset of the first byte of the entry
    pub end:     u64,   // byte offset one past the last byte
}
```

This is the value stored in the in-memory index. Given a key, the engine can seek directly to `start` in `file_id` and read exactly `end - start` bytes without scanning any surrounding data.

---

## 4. Write-Ahead Log (WAL)

### File naming

Log files live in the store's directory and are named `{file_id}.log` where `file_id` is a monotonically increasing `u32`:

```
<dir>/
  1.log
  2.log
  3.log   <-- active (writer's current file)
```

At any point in time there is exactly **one active (writable) file** and zero or more **inactive (read-only) files**.

### Append-only writes

The `Writer` struct holds a `BufWriter<fs::File>` opened in append mode:

```rust
fs::OpenOptions::new()
    .create(true)
    .write(true)
    .append(true)
    .open(&file)
```

The `append` flag ensures the OS atomically positions the write cursor at the end of the file before each write, which prevents data corruption even if multiple threads share an OS file handle. Here, however, access to the `Writer` is further serialized via `Arc<Mutex<Writer>>`.

### Writer internals

```rust
pub struct Writer {
    pub file_id:      u32,
    pub writer:       BufWriter<fs::File>,
    pub pos:          u64,   // logical byte offset (tracks how many bytes written to this file)
    pub uncompacted:  u64,   // total bytes of stale/overwritten data across all files
}
```

`pos` is maintained manually — it is incremented by exactly `b.len()` after every successful `write`. It is used to compute `EntryOffset.start` and `EntryOffset.end` before and after a write call, so the index always points to an exact byte range.

`uncompacted` is a running counter of "wasted" bytes: bytes belonging to overwritten Set entries, superseded Set entries (same key written twice), and tombstone Rm entries.

### Flush strategy

After every single entry write, `BufWriter::flush()` is called explicitly:

```rust
self.writer.write(b)?;
self.writer.flush()?;
```

This means every `set` or `remove` call results in an `fsync`-equivalent drain of the userspace buffer. This is conservative — it sacrifices some throughput for durability.

---

## 5. In-Memory Index

```rust
pub index: Arc<SkipMap<K, EntryOffset>>,
```

The index is a `crossbeam_skiplist::SkipMap<K, EntryOffset>` — a **lock-free, concurrent, ordered map** implemented as a skip list.

### Why a skip list?

- **Ordered**: keys are kept sorted, which allows efficient range scans (not currently exploited, but available).
- **Lock-free**: multiple reader threads can call `index.get(key)` concurrently without blocking each other, and without acquiring a mutex. This is critical for read-heavy workloads dispatched to multiple worker threads.
- **Non-blocking inserts**: `index.insert` uses atomic CAS operations internally, so writers do not need to hold a lock to update the index (beyond the `Mutex<Writer>` that already serializes writes).
- **Concurrent iteration during compaction**: compaction iterates all entries via `index.iter()` while other threads may still be reading from the index.

### Index lifecycle

- **Populated on startup**: `load_inactive_files` replays all existing `.log` files in file_id order, replaying `Set` and `Rm` entries to reconstruct the last known state.
- **Updated on write**: after a successful `Writer::write`, `index.insert(key, EntryOffset{...})` replaces any prior mapping for that key.
- **Updated on remove**: `index.remove(&key)` is called before writing the tombstone.
- **Updated during compaction**: index entries are atomically updated in-place during the compaction pass to point to the new compaction file.

---

## 6. Store: The Core Engine

`Store<K, V>` is the central data structure. Its fields break into three ownership categories:

```rust
pub struct Store<K, V> {
    pub dir:                    Arc<PathBuf>,              // shared, immutable path
    pub readers:                RefCell<HashMap<u32, Reader>>, // per-clone, thread-local
    pub writer:                 Arc<Mutex<Writer>>,        // shared, mutex-guarded
    pub index:                  Arc<SkipMap<K, EntryOffset>>, // shared, lock-free
    pub last_compaction_point:  Arc<AtomicU32>,            // shared, atomic
    _phantom:                   PhantomData<V>,
}
```

### Clone semantics

When `KvStore` (and by extension `Store`) is cloned — which happens each time a new connection is dispatched to a worker thread — the following happens:

```rust
fn clone(&self) -> Self {
    Self {
        dir:                   self.dir.clone(),             // Arc clone: same path
        readers:               RefCell::new(HashMap::new()), // NEW empty readers map
        writer:                self.writer.clone(),          // Arc clone: same writer
        index:                 self.index.clone(),           // Arc clone: same index
        last_compaction_point: Arc::clone(&self.last_compaction_point),
        _phantom:              PhantomData,
    }
}
```

The key design decision: **each clone gets its own `readers` map** (a `RefCell<HashMap<u32, Reader>>`), but all clones share the same `writer`, `index`, and `last_compaction_point`. This means:

- Reads are fully parallel — each worker thread opens its own set of file descriptors and seeks independently with no contention.
- Writes are serialized through the single `Arc<Mutex<Writer>>`.
- The in-memory index is always consistent across all clones via the shared `Arc<SkipMap>`.

### Why `RefCell` for readers?

`RefCell<HashMap<u32, Reader>>` provides interior mutability so that `read()` — which takes `&self` (shared reference) — can lazily open file descriptors and insert them into the map. Since each clone owns its own `RefCell`, there is no cross-thread sharing of readers, so `RefCell` (which is `!Sync`) is safe here.

---

## 7. Read Path

```
client.get(key)
  -> engine.get(key)                               [KvStore::get]
    -> index.get(&key) -> Option<EntryOffset>      [SkipMap, lock-free]
    -> store.read(file_id, start, end)             [Store::read]
      -> close_stale_fds()                         [evict FDs for compacted files]
      -> readers.get_mut(file_id)                  [or lazily open new Reader]
      -> reader.read::<K,V>(start, end)
        -> BufReader::seek(SeekFrom::Start(start))
        -> reader.take(end - start)                [bounded read]
        -> serde_json::from_reader::<Entry<K,V>>   [deserialize]
        -> extract Entry::Set { val } -> Some(val)
```

Complexity:
- Index lookup: O(log n) in the skip list.
- Disk read: O(1) seeks + O(entry size) read. No file scanning.

The `close_stale_fds()` call at the top of every read evicts readers whose `file_id < last_compaction_point`, cleaning up file descriptors for compacted-away files and deleting the corresponding `.log` files from disk.

---

## 8. Write Path

```
client.set(key, val)
  -> engine.set(key, val)                          [KvStore::set]
    -> Entry::init_set(key, val)                   [Entry::Set{key, val}]
    -> serde_json::to_string(&entry)               [JSON encode]
    -> store.write(key, bytes)                     [Store::write]
      -> writer.lock()                             [Mutex<Writer>: serialized]
        -> record old index entry size -> uncompacted
        -> writer.write(bytes) -> end_pos          [BufWriter append + flush]
        -> index.insert(key, EntryOffset{...})     [SkipMap insert]
        -> if uncompacted > 1MB: compact(...)
      -> writer.unlock()
```

The `Mutex` ensures that even with 10 concurrent worker threads, only one `set` appends to the active log file at a time. The index update happens while the lock is still held, so any concurrent reader that calls `index.get` will either see the old offset (still valid on disk) or the new offset (also valid on disk) — never a torn state.

---

## 9. Remove Path

```
client.remove(key)
  -> engine.remove(key)                            [KvStore::remove]
    -> store.remove(key)                           [Store::remove]
      -> index.contains_key(&key) -> error if missing
      -> Entry::init_rm(key)                       [Entry::Rm{key}]
      -> serde_json::to_string(&entry)
      -> index.remove(&key) -> add old size to uncompacted
      -> store.write(key, tombstone_bytes)         [appends Rm entry to log]
```

A remove writes a **tombstone** `Rm` entry to the log. The key is removed from the index immediately, before the tombstone is written. This means:

- After `index.remove`, any concurrent reader that checks the index will get `None` and return "key not found" — correct behavior.
- The tombstone on disk serves as a durable record needed for crash recovery: without it, a restart would replay the original `Set` and resurrect the key.

The tombstone bytes themselves are counted as `uncompacted` since they are logically dead weight once written.

---

## 10. Compaction

Compaction is triggered inside `Store::write` when `writer.uncompacted > COMPACTION_THRESHOLD` (1 MiB):

```rust
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;
```

### Algorithm (`Writer::compact`)

```
1. Allocate a new compaction output file: file_id = current_file_id + 1
   -> e.g., if active was 3.log, compaction output is 4.log

2. Iterate index.iter() -- all live keys in sorted order:
   for each (key, EntryOffset{file_id, start, end}):
     a. Seek to (start..end) in the source file via readers
     b. Copy raw bytes to the compaction output file's BufWriter
     c. Update index entry in-place:
        index.insert(key, EntryOffset{file_id: compaction_file_id, start: new_pos, end: new_pos + len})
     d. Advance new_pos

3. Flush the compaction output file

4. Open a brand-new active log file: file_id = compaction_file_id + 1
   -> e.g., 5.log becomes the new active file
   -> Reset writer.pos = 0, writer.uncompacted = 0

5. Store last_compaction_point = compaction_file_id (atomically, SeqCst)

6. Subsequent calls to close_stale_fds() remove readers for file_id < last_compaction_point
   and delete those .log files from disk
```

### Compaction invariants

- **Index consistency**: index entries are updated atomically in the skip list during the compaction pass. Since the source bytes are never modified (append-only), any reader that still holds an old `EntryOffset` can still read from the old file until `close_stale_fds()` deletes it.
- **No data loss**: compaction only copies entries that are currently in the index — i.e., the latest `Set` for each live key. Tombstones and overwritten values are excluded.
- **Write continuity**: after compaction, a new active file is opened. Any writes arriving during or after compaction go to this new file, unaffected by the compaction output.
- **Stale FD cleanup**: `close_stale_fds()` is called at the start of every `read` and after every compaction. It checks `last_compaction_point` (an `AtomicU32`) and evicts + deletes files with IDs below that point.

### File lifecycle during compaction

```
Before:  1.log  2.log  3.log(active)
                              ^ writer here, uncompacted > 1MB

Compact: create 4.log (compaction output), rewrite live entries from 1-3
         create 5.log (new active)
         last_compaction_point = 4

After (on next close_stale_fds):
         delete 1.log, 2.log, 3.log
         5.log(active)  4.log(compaction snapshot)
```

---

## 11. Startup and Recovery

When `KvStore::open(dir)` is called:

```rust
Store::new(dir):
  1. fs::create_dir_all(dir)                       -- ensure dir exists
  2. get_inactive_file_ids(dir)                    -- scan dir for *.log files, parse IDs, sort
  3. new_file_id = last_inactive_id + 1 (or 1)    -- determine next active file ID
  4. Writer::new(new_file_id, ...)                 -- open/create new active file (append mode)
  5. load_inactive_files(index):
       for each inactive file (in sorted ID order):
         Reader::load_index(file_id, index):
           -- stream-deserialize every Entry in the file
           -- Set{key} -> index.insert(key, offset)
                          if key already in index, old size -> uncompacted
           -- Rm{key}  -> index.remove(key)
                          old size + tombstone size -> uncompacted
  6. writer.uncompacted = total uncompacted bytes found during replay
```

This replay reconstructs the exact last-known state by replaying entries in file order. Because files are sorted by ID (which is monotonically increasing) and entries within a file are in append order, later entries for the same key correctly overwrite earlier ones in the index.

The new active file opened at startup is always empty — no existing data is in it. The startup process does **not** replay the new active file (there is nothing to replay).

If the server restarts mid-compaction (after new files were created but before old ones were deleted), the stale old files will be re-read on the next startup. This is safe because the compaction output file will contain the same logical data — the replay will produce the same index state, just with more uncompacted bytes counted (triggering another compaction on next write).

---

## 12. Multi-Threading Model

### Concurrency primitives used

| Component | Type | Concurrency strategy |
|---|---|---|
| `Writer` | `Arc<Mutex<Writer>>` | One writer at a time; all threads serialize through this mutex |
| `index` | `Arc<SkipMap<K, EntryOffset>>` | Lock-free concurrent reads; atomic CAS-based inserts |
| `readers` | `RefCell<HashMap<u32, Reader>>` | Per-clone (per-thread); no sharing across threads |
| `last_compaction_point` | `Arc<AtomicU32>` | Atomic load/store; no lock needed |
| `dir` | `Arc<PathBuf>` | Immutable after construction; reference-counted sharing |

### Thread layout

```
Main thread
  |-- TcpListener::incoming() loop
  |     for each connection:
  |       engine.clone()    <- Arc clones, new RefCell readers
  |       pool.execute(move || handle_client(engine_clone, stream))
  |
  v
ThreadPool (10 workers, each running a recv loop)
  Worker-0: handles connection A  -> engine_clone_0 (own readers)
  Worker-1: handles connection B  -> engine_clone_1 (own readers)
  ...
  Worker-9: handles connection J  -> engine_clone_9 (own readers)
```

All 10 workers share `Arc<Mutex<Writer>>` and `Arc<SkipMap>`. Reads are fully parallel. Writes contend on the mutex but are fast (buffered I/O + flush).

### Why `KvsEngine: Clone + Send + 'static`?

The trait bound `KvsEngine<K,V>: Clone + Send + 'static` is required because the engine is cloned once per connection and moved into a `FnOnce() + Send + 'static` closure for the thread pool:

```rust
let engine = self.engine.clone();  // Clone: needed for per-connection copy
self.pool.execute(move || {        // Send: the clone crosses thread boundaries
    handle_client(engine, stream)  // 'static: no non-owned borrows
});
```

`KvStore<K,V>` is `Clone` because all its heap data is behind `Arc` (which is `Clone`), and the readers are re-initialized as empty.

`KvStore<K,V>` is `Send` because:
- `Arc<T>` is `Send` when `T: Send + Sync`
- `SkipMap<K,V>` is `Send + Sync`
- `RefCell<...>` is **not** `Sync`, but it is `Send` — and since each clone is owned by exactly one thread, this is safe.

---

## 13. Thread Pool

The thread pool (`src/threadpool.rs`) is a hand-rolled implementation using `std::sync::mpsc` channels:

```
ThreadPool
  sender: Option<mpsc::Sender<Job>>
  workers: Vec<Worker>
    each Worker holds a JoinHandle running:
      loop {
        match receiver.lock().unwrap().recv() {
          Ok(job) => job(),
          Err(_)  => { /* channel closed, loop ends */ }
        }
      }
```

### Channel architecture

```
main thread           worker threads
   |                    |   |   |
   | send(job) -------> |   |   |
   |              Arc<Mutex<Receiver>>
   |                    |   |   |
   |                 (one at a time pops)
```

The `mpsc::Receiver` is wrapped in `Arc<Mutex<_>>` so all worker threads share it. Despite MPSC being "multi-producer, single-consumer", this pattern converts it into a multi-consumer pattern by having each consumer lock the receiver before calling `recv()`.

### Graceful shutdown

`ThreadPool` implements `Drop`:

```rust
fn drop(&mut self) {
    drop(self.sender.take());          // closes the channel (drops Sender)
    for worker in self.workers.drain(..) {
        worker.thread.join().unwrap(); // wait for each thread to see Err(_) and exit
    }
}
```

Dropping the `Sender` causes `receiver.recv()` to return `Err(RecvError)` once the channel is empty, which breaks the worker loop. The `join()` calls ensure no worker is still executing a job when the pool is dropped.

### Pool size

The server hardcodes a pool size of 10:

```rust
let pool = ThreadPool::new(10);
```

This is a fixed-size pool. There is no dynamic resizing, work-stealing, or backpressure.

---

## 14. Client-Server Networking

### Protocol

All client-server communication is JSON over a persistent TCP connection. The protocol is framing-free — multiple requests can be pipelined over one TCP stream, with the `serde_json` streaming deserializer (`Deserializer::into_iter`) parsing one JSON value at a time.

### Request / Response types (`src/resource.rs`)

```rust
pub enum Request<K, V> {
    Get { key: K },
    Set { key: K, val: V },
    Rm  { key: K },
}

pub enum Response<V> {
    Ok(Option<V>),
    Err(String),
}
```

Example wire format:

```
Client sends:   {"Get":{"key":"foo"}}
Server replies: {"Ok":"bar"}           (key exists)
                {"Ok":null}            (key not found)
                {"Err":"..."}          (error)
```

### Server connection handling (`src/server.rs`)

```
TcpListener::incoming()
  -> TcpStream per connection
  -> engine.clone()
  -> pool.execute(|| handle_client(engine, stream))

handle_client:
  reader = BufReader::new(stream.try_clone())
  writer = stream
  request_reader = Deserializer::from_reader(reader).into_iter::<Request>()
  loop:
    req = request_reader.next()    // blocks until next JSON value arrives
    match req:
      Get -> engine.get -> serialize Response -> writer.write_all + flush
      Set -> engine.set -> serialize Response -> writer.write_all + flush
      Rm  -> engine.remove -> serialize Response -> writer.write_all + flush
```

`stream.try_clone()` produces a second OS file descriptor pointing to the same TCP socket. One is wrapped in `BufReader` for reading requests; the original is used for writing responses. This split avoids holding both a mutable and immutable reference to the same `TcpStream`.

### Client (`src/client.rs`)

```rust
pub struct KvsClient {
    request_stream:  TcpStream,
    response_stream: Deserializer<IoRead<BufReader<TcpStream>>>,
}
```

The client sends a request by JSON-serializing it and writing the raw bytes over the stream. It then reads back a `Response` by deserializing the next JSON value from the response stream. Since requests and responses are strictly 1:1 and ordered, no correlation IDs are needed.

---

## 15. Generics and Trait System

The entire store is fully generic over key type `K` and value type `V`. The trait bounds express the minimum set of capabilities each type must provide:

| Bound | Reason |
|---|---|
| `Clone` | Engine must be cloneable for per-connection copies; keys are cloned when inserting into the index |
| `Serialize + DeserializeOwned` | Keys and values are JSON-encoded for the WAL and the network protocol |
| `Ord` | Required by `SkipMap<K, _>` which maintains sorted order |
| `Send` | Keys and values cross thread boundaries via the thread pool |
| `Sync` (keys only) | The skip list index is shared across threads via `Arc`; values stored in the index must be readable from multiple threads |
| `'static` | Closures passed to the thread pool must not hold short-lived borrows |
| `Debug` | Error messages include key representations |

### KvsEngine trait (`src/engines/mod.rs`)

```rust
pub trait KvsEngine<K, V>: Clone + Send + 'static {
    fn get(&self, key: K) -> Result<Option<V>>;
    fn set(&self, key: K, val: V) -> Result<()>;
    fn remove(&self, key: K) -> Result<K>;
}
```

All methods take `&self` (shared reference). This is intentional: the engine must be usable from multiple threads simultaneously, and interior mutability (`Mutex`, `AtomicU32`, `SkipMap`) is used inside the implementation to achieve thread-safe mutation without requiring exclusive access.

### PhantomData

`Store<K, V>` holds `PhantomData<V>` because `V` does not appear in any field directly (all `V`-typed values exist only on the stack during reads/writes, never stored in the struct). Without `PhantomData<V>`, the compiler would reject the unused type parameter.

---

## 16. Error Handling

```rust
pub enum Error {
    Io(io::Error),
    Serde(serde_json::Error),
    DoesNotExist { key: String },
    UnhandledError(String),
    Sled(sled::Error),
    Utf8(FromUtf8Error),
}
```

Errors use the `failure` crate, which provides:
- `Fail` trait (similar to `std::error::Error` with causal chains)
- `#[cause]` for wrapping underlying errors
- `Display` derivation via `#[fail(display = "...")]`

`From` implementations are provided for `io::Error`, `serde_json::Error`, `sled::Error`, and `FromUtf8Error`, enabling `?`-based propagation throughout. All public API functions return `Result<T>` which is `std::result::Result<T, Error>`.

---

## 17. Key Design Trade-offs and Limitations

### Strengths

- **Fast reads**: O(log n) index lookup + single seek to exact byte offset. No LSM tree compaction pauses during reads.
- **Simple write path**: append-only writes are sequential I/O, the fastest possible disk access pattern.
- **Fast crash recovery**: replay is linear in total log file size, and only requires reading key/offset metadata — not deserialized values — to rebuild the index.
- **Lock-free reads**: the `SkipMap` allows true concurrent reads across all worker threads with no mutex contention.

### Limitations

- **All keys in memory**: the in-memory index holds every live key. For very large datasets, this can be a significant memory cost.
- **Single writer**: the `Arc<Mutex<Writer>>` serializes all writes. Under high write concurrency this is the main bottleneck.
- **Per-thread file descriptors**: each worker thread opens its own set of file descriptors when it first reads from a file. With many workers and many log files, this can exhaust OS FD limits.
- **No WAL fsync guarantee**: `BufWriter::flush()` writes to the OS page cache. Without an explicit `fsync`, a kernel crash after `flush()` but before the OS flushes dirty pages could lose the last written entry. The current implementation does not call `fsync`.
- **Compaction blocks the writer**: `Writer::compact` is called while holding `Arc<Mutex<Writer>>`. During compaction, all concurrent `set`/`remove` operations block.
- **No range queries**: despite the `SkipMap` being ordered, the `KvsEngine` trait only exposes point get/set/remove. No scan or range API exists.
- **Readers can see deleted files**: the `close_stale_fds()` + `fs::remove_file` approach means a reader that has a valid `EntryOffset` for a compacted file could try to read from a file that is being deleted. This is a potential race condition — the current code does not handle `ENOENT` on read, relying on the compaction point advancing before any concurrent reader gets an offset into a stale file.

Ref - [TP 201: Practical Networked Applications in Rust](https://github.com/pingcap/talent-plan/blob/master/courses/rust/projects/project-2/README.md).
