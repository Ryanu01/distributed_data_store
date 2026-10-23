# What Was Implemented

This document explains the local storage extension added to the p2p Rust
project. The previous version could only send and receive a whole file using an
iroh `BlobTicket`. The new version adds content-defined chunking, manifests,
local storage, deduplication, reassembly, persistent node identity, and a redb
metadata index.

Milestones M1 (chunking, bundles, local storage, identity, metadata), M2a
(control protocol, peer registry, two-node transfer), M2b (local HTTP API and
daemon-backed CLI), and M2c (remote retrieval and hardening) are complete.
**M2d (name-based retrieval)** is planned next. A long-lived `p2p node` serves
blobs, control requests, and a localhost HTTP API, and another node can fetch
and verify a bundle by ticket or file ID. Today a name is resolved only from
the local metadata; remote name lookup is planned in M2d. Peer replication and
fault tolerance remain M3/M4; see `IMPLEMENTATION_PLAN.md`.

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

1. Resolves the argument locally as either:
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
p2p node [--data-dir <dir>] [--api <addr>]
p2p [--api <addr>] put <file> [--data-dir <dir>]
p2p [--api <addr>] get <file-id-or-name-or-ticket> <output> [--data-dir <dir>]
p2p [--api <addr>] list [--data-dir <dir>]
p2p [--api <addr>] status [--data-dir <dir>]
p2p [--api <addr>] health [--data-dir <dir>]
p2p [--api <addr>] info <file-id-or-name> [--data-dir <dir>]
p2p [--api <addr>] ticket <file-id-or-name> [--data-dir <dir>]
p2p [--api <addr>] peers list [--data-dir <dir>]
p2p [--api <addr>] peers add <endpoint-id> [--addr <ip:port>] [--data-dir <dir>]
p2p send <file> [--data-dir <dir>]
p2p receive <ticket> <output> [--data-dir <dir>]
```

The daemon owns the store. `put/get/list/status/health/info/ticket/peers` send
HTTP requests to it, resolving `127.0.0.1:7001` unless `--api` or the
`<data-dir>/api.addr` file says otherwise. The original `send` and `receive`
commands remain direct and use the persistent identity key; they must not run
against a data directory that a daemon is using.

`put`, `get`, and `list` use `./p2p_data` by default. `receive` uses
`./p2p_data_receive` by default; all commands accept `--data-dir`.

---

## Local HTTP API and Metrics

The daemon binds `127.0.0.1:7001` by default and exposes `/health`, `/status`,
`/files`, `/files/{id}`, `/fetch`, `/files/{id}/ticket`, and `/peers`. It writes
the actual bound address to `<data-dir>/api.addr`, which is removed on graceful
shutdown. All file paths in HTTP requests are absolute.

`/health` returns:

- `node_id`: the daemon endpoint ID.
- `free_storage`: available bytes on the disk containing the data directory.
- `cpu_usage` and `memory_usage`: system percentages.
- `latency`: milliseconds, an EMA of tracked HTTP/control operations.
- `bandwidth`: bytes/second, an EMA of successful tracked file operations.
- `reliability` and `failure_rate`: success and failure ratios in `0..=1`.
- `current_load`: active tracked operations divided by eight, clamped to
  `0..=1`.

System values are sampled every two seconds and initialized before the API
starts. These fields are intended as inputs for later AI-based node ranking;
the ranking itself is not implemented yet.

---

## Remote Retrieval

`p2p get <target> <output>` accepts a local file ID, a local name, or a
HashSeq ticket. `POST /fetch` carries `{"target": "...", "output": "..."}`.
Remote retrieval by file ID and ticket is implemented; remote lookup by name is
the planned M2d extension described below.

Retrieval works as follows:

1. Parse the target and resolve its file ID.
2. If the bundle is already local, reassemble it without network access.
3. Build provider candidates:
   - The ticket's provider, if a ticket was supplied.
   - Endpoint IDs recorded in the replicas table.
   - Known peers that advertise the file.
4. Query known peers concurrently with `GetTicket` and a 3 second timeout.
5. Connect directly to each provider using its full `EndpointAddr` and fetch the
   complete HashSeq bundle. Provider connection attempts time out after 5
   seconds.
6. Try the next provider if one fails. If all providers fail, return `502` with
   the attempted providers.
7. Tag the downloaded bundle, record the file and local replica, and remember
   the provider as a peer.
8. Reassemble into a temporary file, verify the hashes, and rename atomically.

Tickets are self-contained: Node B can retrieve from Node A without adding A
as a peer first. File-ID retrieval requires at least one known peer that has the
file, added with `p2p peers add`.

Errors are explicit:

- `400`: malformed ticket or a ticket for a raw blob instead of a bundle.
- `404`: not local, and no known peer advertises the file.
- `502`: providers were found but every retrieval attempt failed.

A failed download leaves no output file. Partial content-addressed chunks may
remain in the store, but the file is not tagged or recorded as complete.

Provider fallback is best-effort until M3 adds replication. If only the original
holder has a file, stopping it still means retrieval fails; fallback helps when
another peer previously downloaded and cached the bundle.

---

## Planned M2d - Name-Based Retrieval

Status: **planned, not implemented**. The test count remains 40 until this slice
is built.

The main product flow becomes:

```bash
p2p get report.pdf ./downloaded.pdf
```

The user does not need a ticket. The node finds the file by exact filename on
itself or on a peer it already knows, then downloads and verifies it.

### Selected behavior

- Search only peers already registered with `p2p peers add`.
- Match names exactly and case-sensitively.
- A local match always wins and requires no network access.
- If different files share a name on different peers, the newest
  `created_at_unix` wins.
- If the same file ID exists on multiple peers, merge all providers for
  fallback.
- Keep ticket and file-ID retrieval as advanced options.
- M2d does not replace or renumber M3/M4; it is an additive retrieval slice.

### Planned protocol

The control protocol gains:

```rust
ControlRequest::FindByName { name: String }

ControlResponse::NameMatches { matches: Vec<NameMatch> }

struct NameMatch {
    file_id: Hash,
    file_name: String,
    file_size: u64,
    chunk_count: usize,
    created_at_unix: u64,
    ticket: BlobTicket,
}
```

Each peer performs an exact lookup in its local `names` table and returns the
current mapping, or an empty list. `CONTROL_VERSION` is bumped so mixed-version
nodes fail explicitly.

### Planned retrieval flow

1. Parse a ticket, local file ID, or name.
2. Resolve a local name or file ID first. Return immediately if the bundle is
   already present.
3. If the target is not local and is not a file ID, query all known peers
   concurrently with `FindByName` and a 3 second timeout.
4. Ignore failed or timed-out peers.
5. Group results by file ID and merge provider addresses for the same file.
6. Choose the highest `created_at_unix` when different files share the name;
   break ties by file ID. Local matches still take precedence.
7. Fetch from the selected file's providers using the existing sequential
   fallback and 5 second provider timeout.
8. Finalize, cache, tag, and record the bundle exactly as ticket/file-ID
   retrieval does now.

### Planned API and CLI

```http
GET /search?name=report.pdf
```

Returns grouped local and peer matches, sorted newest first.

```bash
p2p search report.pdf
p2p get report.pdf ./downloaded.pdf
```

`POST /fetch` keeps its current schema; `target` may now be a name that is not
local.

### Planned errors

- Unknown name: `404`, no output file.
- Name found but all providers fail: `502`, no output file.
- Empty or invalid name: `400`.
- Local name lookup remains offline-capable.

### Planned tests

- `FindByName` returns the exact local match and is case-sensitive.
- Unknown names return an empty match list.
- Remote `get <name>` works after adding the holder as a peer.
- Missing holder in the peer list returns `404` with no output.
- Non-holding peers are skipped.
- Newest remote duplicate wins; local names take precedence.
- Identical file IDs on multiple peers are merged for fallback.
- `/search` returns local and remote matches.
- Existing ticket and file-ID tests continue to pass.

Expected total after implementation: roughly 48-50 tests.

### Why this is not a conflict with M3/M4

Name lookup is a retrieval feature built on the existing peer registry. M3
still implements replication, placement, and replica tracking. M4 still
implements heartbeats, offline detection, retries, and repair. Once replication
exists, the same name lookup automatically finds replica holders; no M3/M4
design change is required.

### Known limits

- Only known peers are searched; there is no global filename index.
- Duplicate-name selection trusts peer-provided timestamps.
- Partial/substring name search is not included.
- A name cannot make an offline sole holder available; M3/M4 provide that
  reliability.

---

## Storage Layout

The selected data directory contains both the blob store and node metadata:

```text
<data-dir>/
├── identity.key        # persistent endpoint secret key (0600 on Unix)
├── metadata.db         # redb: files, names, replicas, peers
├── blobs.db            # iroh-blobs metadata/inline blobs
├── data/               # large blob files named by BLAKE3 hash
├── temp/               # temporary imports
└── api.addr            # live HTTP API address while the daemon runs
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

`cargo test` passes 40 tests covering chunking, manifests, identity, redb
metadata, bundle creation and reassembly, deduplication, restart persistence,
name lookup, listing, the local HTTP API, peer registry, two-node transfer,
remote retrieval by ticket and file ID, provider fallback, invalid tickets,
unavailable providers, output atomicity, and metrics.

### Quality checks

- `cargo fmt --check` passes.
- `cargo clippy --all-targets -- -D warnings` passes.
- `cargo test` passes 40 tests, including two-node `MemoryLookup` transfers.

### Manual end-to-end test

A 1 MiB random file was stored and retrieved:

- chunks created: 13
- round-trip bytes: identical
- second put deduplicated: 1,048,576 bytes
- lookup by file name: successful

Remote ticket retrieval, file-ID discovery through known peers, and local
caching were also exercised between separate daemons.

---

## Current Limitations

- **Remote name lookup is planned, not implemented.** `p2p get <name>` resolves
  names only from the local metadata today. Remote retrieval requires a ticket
  or file ID until M2d is built.
- **No automatic replication yet.** Remote retrieval caches a bundle on the
  fetching node, but `put` does not proactively copy it to other peers.
  Provider fallback therefore requires another node that already has the file.
  `send`/`receive` still transfer a whole raw blob and are not bundle-aware.
- **Whole file is read into memory.** The chunker currently reads the complete
  file before splitting it. Large-file streaming should be added before the
  replication milestone.
- **Peer state is basic.** The `peers` table stores `EndpointAddr`, last-seen
  time, and an online flag, but heartbeats and offline detection are still M4.
- **No authentication.** The local store is trusted; there is no peer
  authorization or encryption beyond iroh's transport security.
- **Name collisions.** The local `name/` mapping stores only the latest file
  with a given name; older files remain reachable by file ID. M2d extends this
  with newest-wins selection across known peers.

---

## Next Steps

The next milestone is name-based retrieval (M2d), followed by replication and
fault tolerance:

1. **M2 - Control protocol and two-node transfer**
   - M2a (complete): daemon, control ALPN, peer registry, HashSeq retrieval
   - M2b (complete): daemon-owned HTTP API, `api.addr` discovery, CLI commands,
     and `/health` metrics
   - M2c (complete): remote retrieval by ticket/file ID, provider fallback,
     caching, timeouts, and failure hardening
   - M2d (planned): exact filename search across known peers, `p2p search`,
     newest-wins duplicate handling, and `get <name>` without a ticket
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
