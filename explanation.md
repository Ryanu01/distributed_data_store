# What Was Implemented

This document explains the local storage extension added to the p2p Rust
project. The previous version could only send and receive a whole file using an
iroh `BlobTicket`. The new version adds content-defined chunking, manifests,
local storage, deduplication, reassembly, persistent node identity, and a redb
metadata index.

Milestone M1 (chunking, bundles, local storage, identity, metadata) is
complete. The implementation is currently **local-only**. Networking, peer
replication, and fault tolerance are not implemented yet; they are the next
milestones in `IMPLEMENTATION_PLAN.md`.

---

## Summary

The new flow is:

```text
input file
    |
    v
FastCDC chunking
    |
    v
BLAKE3 hash per chunk + BLAKE3 hash of the whole file
    |
    v
manifest.json (file metadata + ordered chunk list)
    |
    v
iroh Collection bundle
    |
    v
iroh-blobs FsStore + persistent tags
```

Retrieval reverses the process:

```text
file id or file name
    |
    v
find bundle in local store
    |
    v
load manifest.json
    |
    v
read and verify each chunk
    |
    v
reassemble in order
    |
    v
verify whole-file hash
    |
    v
atomic write to output path
```

---

## Files Added or Changed

### `Cargo.toml`

Added direct dependencies:

- `blake3` - whole-file hashing during reassembly.
- `clap` - command-line parsing.
- `fastcdc` - FastCDC content-defined chunking.
- `futures` - iterating over iroh tag streams.
- `redb` - persistent node/file/replica metadata.
- `serde` and `serde_json` - manifest and metadata serialization.
- `tempfile` (dev dependency) - isolated storage tests.
- `tracing` and `tracing-subscriber` - structured diagnostics.

Existing dependencies `iroh`, `iroh-blobs`, `tokio`, and `anyhow` are still used.

### `src/config.rs`

Defines the data-directory layout used by every command:

- data directory
- `identity.key`
- `metadata.db`

### `src/error.rs`

Shared `anyhow` result/context/bail re-exports used across modules.

### `src/identity.rs`

Loads or creates a persistent Ed25519 node identity at
`<data-dir>/identity.key`.

- The key is 32 raw bytes and is created with `0600` permissions on Unix.
- Reloading the same data directory returns the same `EndpointId`.
- A malformed key (wrong length) is rejected instead of silently replaced.

### `src/metadata.rs`

Implements the redb metadata database at `<data-dir>/metadata.db`.

Tables:

```text
files     file_id        -> FileRecord JSON
names     file name      -> file_id
replicas  file_id        -> [endpoint id]
```

`FileRecord` stores the file ID, name, size, whole-file hash, chunk count, and
creation time. Recording a file is idempotent and preserves the original
creation timestamp when identical content is stored again. Replica entries are
deduplicated.

### `src/cli.rs`

Holds the clap command definitions and command handlers. `main.rs` is now only
module wiring, tracing setup, and dispatch.

### `src/chunker.rs`

Implements content-defined chunking.

- Uses FastCDC v2020.
- Chunk sizes:
  - minimum: 16 KiB
  - average: 64 KiB
  - maximum: 256 KiB
- Produces a `ChunkedFile` containing:
  - ordered `Chunk` values
  - each chunk's BLAKE3 hash
  - the whole-file BLAKE3 hash
  - the original file size
- Empty files produce zero chunks and the BLAKE3 empty-file hash.

Tests cover:

- chunks covering the entire input exactly
- deterministic chunk boundaries
- empty input
- small input

### `src/manifest.rs`

Defines the on-disk manifest that travels with each file bundle.

Manifest contents:

- manifest version
- original file name
- original file size
- whole-file BLAKE3 hash
- chunking parameters
- ordered chunk references (`index`, `hash`, `length`)
- a reserved timestamp field kept at zero so identical content produces
  identical manifest bytes (the real creation time is stored in redb)

Validation checks:

- supported manifest version
- non-empty file name
- chunks are in ascending index order
- no zero-length chunks
- chunk sizes add up to the declared file size
- all hashes are valid
- empty files have no chunks

### `src/bundle.rs`

Implements the actual `put`, `get`, and `list` operations.

#### `put_file`

1. Reads the file.
2. Chunks it with FastCDC.
3. Computes BLAKE3 hashes.
4. Builds `manifest.json`.
5. Adds the manifest to the iroh-blobs store.
6. Adds each chunk to the store.
7. Counts chunks that already existed as deduplicated bytes.
8. Creates an iroh `Collection` with:
   - `manifest.json`
   - `chunk-000000`
   - `chunk-000001`
   - ...
9. Stores the collection and tags its root as:
   - `file/<file-id>` for listing and lookup
   - `name/<file-name>` for lookup by original name
10. Returns the file ID, size, file hash, chunk count, creation time, and dedup
    savings.

The collection root hash is the file ID. Because iroh-blobs is
content-addressed, identical chunks are stored only once.

#### `get_file`

1. Resolves the argument in the CLI as either:
   - a 64-character hex file ID
   - an `s`-prefixed hash-sequence ID
   - a name from the redb `names` table
   - a `name/` tag as a fallback for older local stores
2. Loads the collection and `manifest.json`.
3. Validates the manifest against the collection entries.
4. Reads each chunk from the store.
5. Recomputes each chunk hash and compares it with the manifest.
6. Writes chunks in order to a temporary output file.
7. Recomputes the whole-file BLAKE3 hash.
8. Renames the temporary file to the requested output path only after all
   checks pass.

This prevents a partially written or corrupted output file from being left
behind on failure.

#### `list_files`

- Reads file records from the redb metadata database.
- Also scans `file/` tags and adds any bundles that predate the metadata
  database.
- Prints file ID, name, size, chunk count, and creation timestamp.
- Skips unreadable bundles with a warning instead of failing the whole listing.

### `src/main.rs` and `src/cli.rs`

The CLI supports:

```bash
p2p send <file> [--data-dir <dir>]
p2p receive <ticket> <output> [--data-dir <dir>]
p2p put <file> [--data-dir <dir>]
p2p get <file-id-or-name> <output> [--data-dir <dir>]
p2p list [--data-dir <dir>]
```

The original `send` and `receive` commands are preserved. They now use the
persistent identity key in the selected data directory instead of generating a
new endpoint identity on every run.

`put`, `get`, and `list` use `./p2p_data` by default. `receive` uses
`./p2p_data_receive` by default; all commands accept `--data-dir`.

---

## Storage Layout

The selected data directory contains both the blob store and node metadata:

```text
<data-dir>/
├── identity.key        # persistent endpoint secret key (0600 on Unix)
├── metadata.db         # redb: files, names, replicas
├── blobs.db            # iroh-blobs metadata/inline blobs
├── data/               # large blob files named by BLAKE3 hash
└── temp/               # temporary imports
```

Files are stored inside an iroh-blobs `FsStore` in the same data directory.

Each stored file is represented by a bundle:

```text
<collection root>
├── manifest.json
├── chunk-000000
├── chunk-000001
└── ...
```

The collection root is a `HashSeq` blob. Tagging the root protects the bundle
from garbage collection. Tests confirm that bundles survive closing and
reopening the store.

Two tags are created per file:

```text
file/<file-id>      -> collection root
name/<file-name>    -> collection root
```

The `name/` tag remains a fallback for stores created before `metadata.db`
existed. New writes also record the file and its name in redb. If two files
share a name, the newer name mapping wins, but both file IDs remain available
by ID.

---

## Deduplication

Deduplication happens at the chunk level.

Before adding a chunk, `put_file` checks whether the chunk hash already exists
in the store. If it does, the chunk is not written again and its length is
counted as saved bytes.

For example, storing the same 1 MiB file twice produced:

```text
first put:   dedup = 0 bytes
second put:  dedup = 1048576 bytes
```

The second put returns the same file ID because the manifest and all chunks are
identical.

---

## Verification Performed

### Automated tests

`cargo test` passes 22 tests:

- chunker coverage
- chunker determinism
- empty input
- small input
- manifest round-trip
- deterministic manifest bytes
- invalid manifest total size
- out-of-order chunks
- config path derivation
- identity stability across loads
- identity rejects malformed keys
- identity file permissions
- metadata file storage and listing
- metadata name resolution
- metadata replica tracking
- metadata restart persistence
- local put/get round-trip
- chunk deduplication
- empty-file round-trip
- store restart persistence
- lookup by file name
- listing stored files (metadata plus tag merge)

### Quality checks

- `cargo fmt --check` passes.
- `cargo clippy --all-targets -- -D warnings` passes.

### Manual end-to-end test

A 1 MiB random file was stored and retrieved:

- chunks created: 13
- round-trip bytes: identical
- second put deduplicated: 1,048,576 bytes
- lookup by file name: successful

---

## Current Limitations

- **Local only.** Files are not yet sent to or replicated on other peers.
  `send`/`receive` still transfer a whole raw blob and are not bundle-aware.
- **Whole file is read into memory.** The chunker currently reads the complete
  file before splitting it. Large-file streaming should be added before the
  replication milestone.
- **Peer state is not stored yet.** The `peers` table will arrive with M2; the
  current redb schema covers files, names, and replicas.
- **No authentication.** The local store is trusted; there is no peer
  authorization or encryption beyond iroh's transport security.
- **Name collisions.** The `name/` mapping stores only the latest file with a
  given name; older files remain reachable by file ID.

---

## Next Steps

The next milestone is remote retrieval, followed by replication and fault
tolerance:

1. **M2 - Control protocol and two-node transfer**
   - persistent node daemon owning the store
   - peer registry backed by redb (the `peers` table)
   - control protocol for fetching a bundle by file ID
   - HashSeq ticket output and remote retrieval with fallback
2. **M3 - Replication**
   - replication factor
   - replica tracking
   - `put --replicas N`
   - replica status reporting
3. **M4 - Fault tolerance and repair**
   - heartbeats and offline detection
   - retrieval retries across providers
   - repair of under-replicated bundles
4. **M5 - Benchmarks and demo**
   - storage savings
   - retrieval latency
   - fault-injection demo

See `IMPLEMENTATION_PLAN.md` for the full architecture and milestone breakdown.
