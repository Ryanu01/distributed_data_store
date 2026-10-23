# P2P Core Extension Plan

**Status:** Draft for review
**Scope:** Extend the current iroh-based file transfer prototype into a usable
content-addressed distributed storage core: chunking, manifests, persistent
identity, multi-peer replication, retrieval, and fault tolerance.

This plan implements Phases 1-3 of `p2p.md` (storage engine, networking,
replication/fault tolerance). AI deduplication and AI-based placement are
explicitly deferred, but the placement layer is designed as a seam for them.

---

## 1. Current State

Milestones **M0 (scaffolding)**, **M1 (chunking, bundles, local storage)**,
**M2a (control protocol, peer registry, two-node transfer)**, **M2b (local
HTTP API and daemon-backed CLI)**, and **M2c (remote retrieval and hardening)**
are complete. **M2d (name-based retrieval)** is planned next, followed by M3
(replication). M2d is an additive retrieval slice and does not renumber or
replace M3/M4/M5. The repository is still a single binary crate, now split into
focused modules:

- `src/main.rs` - module wiring, tracing setup, dispatch only.
- `src/cli.rs` - clap definitions and command handlers.
- `src/config.rs` - data-directory layout.
- `src/error.rs` - shared error re-exports.
- `src/identity.rs` - persistent `identity.key` (`0600` on Unix).
- `src/chunker.rs` - FastCDC chunking and BLAKE3 hashing.
- `src/manifest.rs` - manifest schema and validation.
- `src/bundle.rs` - Collection creation, tagging, retrieval, reassembly.
- `src/metadata.rs` - redb `files`, `names`, `replicas`, and `peers` tables.
- `src/protocol.rs` - versioned control ALPN, framed messages, handler/client.
- `src/node.rs` - daemon lifecycle and combined blob/control Router.
- `src/retrieval.rs` - ticket creation, provider fallback, remote HashSeq fetch,
  download finalization, and local caching.
- `src/http.rs` - local Axum API for health, status, files, tickets, peers, and
  (planned M2d) filename search.
- `src/client.rs` - HTTP client and `api.addr` discovery for CLI commands.
- `src/metrics.rs` - node resource/operation metrics for `/health`.

Implemented behavior:

- `p2p node --data-dir <dir> [--api <addr>]` runs the long-lived daemon and
  serves the blob ALPN, control ALPN, and local HTTP API.
- `p2p put <file>` and `p2p get <file-id|name|ticket> <output>` send requests
  to the daemon, which chunks, stores, verifies, downloads when needed, and
  writes through the owned store. Today a name is resolved locally; M2d adds
  remote name lookup across known peers.
- `p2p list`, `p2p status`, `p2p health`, `p2p info`, `p2p ticket`, and
  `p2p peers list/add` are daemon-backed.
- `p2p send`/`p2p receive` preserve the original whole-file blob-ticket flow
  and use the persistent identity key. All commands accept `--data-dir`; all
  commands except `send`/`receive` accept global `--api`.
- Deduplication is chunk-level and identical files produce the same file ID.
- Tickets are self-contained; file-ID retrieval discovers holders through the
  control protocol. Provider failure falls back to the next known holder.
- Remote downloads are cached locally, recorded as replicas, and written
  atomically. Failures return actionable `400`/`404`/`502` errors without a
  partial output file.
- `/health` reports live resource and operation metrics for later AI-based
  node ranking.

Tests: 40 unit/integration tests pass, covering chunking, manifests, identity,
redb metadata, bundle round-trips, dedup, restart persistence, name lookup,
listing, HTTP API operations, peer registry, two-node transfer, remote ticket
and file-ID retrieval, provider fallback, invalid tickets, failure handling, and
metrics. `cargo fmt --check` and
`cargo clippy --all-targets -- -D warnings` pass.

What is missing (next milestones):

- Name-based retrieval across known peers, plus `p2p search <name>` (M2d).
- Replication factor, placement policy, and replica reporting (M3).
- Heartbeats, offline detection, repair, and fault-injection tests (M4).
- Benchmarks, demo script, and monitoring output (M5).
- Streaming ingest, file-size limits, and tracked-doc cleanup (M5).
- AI near-duplicate detection and predictive placement (deferred, `p2p.md`).

The `send`/`receive` path still transfers a whole raw blob; it is not yet
bundle-aware or chunked.

---

## 2. Target Architecture

```
                    +---------------------------+
                    |        p2p CLI            |
                    |  node / put / get / ...   |
                    +-------------+-------------+
                                  |
                         local HTTP (127.0.0.1)
                                  |
+-----------------------------------------------------------------------+
|                            p2p node process                            |
|                                                                       |
|  +-------------+   +--------------+   +---------------------------+   |
|  |  Chunker    |-->|   Bundle     |-->|  iroh-blobs FsStore       |   |
|  |  FastCDC    |   |  (Collection)|   |  chunks + manifest + tags |   |
|  +-------------+   +--------------+   +---------------------------+   |
|         |                                      |                      |
|         v                                      v                      |
|  +-------------+   +--------------+   +---------------------------+   |
|  |  Metadata   |   |  Placement   |   |  Retrieval / Reassembly   |   |
|  |  redb       |<->|  policy      |<->|  downloader + verifier    |   |
|  +-------------+   +--------------+   +---------------------------+   |
|         ^                                      ^                      |
|         |                                      |                      |
|  +------+--------------------------------------+--------------------+ |
|  |                     Control protocol (ALPN)                     | |
|  |  hello / status / replicate / repair / list-manifests / ping    | |
|  +----------------------------------------------------------------+ |
|                                  |                                    |
|                          iroh Endpoint (QUIC)                         |
+-----------------------------------------------------------------------+
                                   |
              +--------------------+--------------------+
              |                                         |
       blobs ALPN                                 control ALPN
       (iroh-blobs)                              (p2p-storage/1)
```

Key idea: a file is stored as an iroh `Collection` bundle whose root is a
`HashSeq`. The bundle contains `manifest.json` plus ordered chunk blobs.
Replication and retrieval operate on the bundle root, while iroh-blobs handles
BLAKE3 verification and chunk-level deduplication automatically.

---

## 3. Design Decisions

| Area | Decision | Rationale |
|---|---|---|
| Chunking | FastCDC, min 16 KiB / avg 64 KiB / max 256 KiB | Content-defined boundaries tolerate insertions/edits; configurable constants. |
| Hashing | BLAKE3 via `iroh_blobs::Hash` | Already used by iroh-blobs; no second hash dependency. |
| Storage | `iroh-blobs` `FsStore` for content + `redb` for app metadata | Reuse verified streaming and dedup; keep node/file/replica state separate. |
| Bundle | `iroh-blobs` `Collection` containing `manifest.json` and chunks | One root hash identifies the file and transfers all chunks in one request. |
| Manifest | JSON inside the bundle | Debuggable, self-describing, travels with content. |
| Node identity | Persisted `SecretKey` at `<data-dir>/identity.key` (0600) | Stable `EndpointId` across restarts is required for peer registries and replication. |
| Process model | Long-lived `p2p node` daemon + local HTTP API | Avoids store locking, supports background health checks and repair. |
| Discovery | Static peer registry (`peers add`) with optional peer exchange | Simple and deterministic for a capstone; tracker/DHT remains a stretch goal. |
| Replication | Pull-based, replica fetches bundle from source via iroh-blobs | Avoids experimental push support; uses the stable download path. |
| Replication factor | Default 3, configurable; at least 2 acks required for success | Standard fault-tolerance tradeoff; configurable for small demos. |
| Placement | `PlacementPolicy` trait; default `LeastReplicated`/round-robin | Clean seam for AI node-selection later. |
| Retrieval | Ticket provider then known replicas/peers; direct full-address fetch with per-provider fallback, then reassemble and verify whole-file hash | Works with ticket direct addresses without relying on global discovery; failures are attributable to specific providers. |
| Failure handling | Heartbeats, timeouts, retries, periodic repair | Detect offline peers and restore replication factor. |
| Crate layout | Single crate with focused modules | Avoids a workspace migration now; can split later if modules grow. |

### Deferred decisions

1. **Tracker vs. static peers.** Default: static registry plus optional
   `peers exchange`. A central tracker can be added later without changing the
   control protocol.
2. **Exact chunk parameters.** Defaults above are proposed; tune after the
   first benchmark.
3. **Docker demo.** Optional; integration tests will use in-process nodes.

---

## 4. Data Model

### 4.1 Bundle contents

```text
collection root (HashSeq)
├── manifest.json
├── chunk-000000
├── chunk-000001
└── chunk-000002
```

Chunk names are zero-padded to preserve order. The collection root hash is the
file's content identifier (`file_id`).

### 4.2 Manifest

```json
{
  "version": 1,
  "file_name": "report.pdf",
  "file_size": 10485760,
  "file_hash": "<blake3 hex of entire file>",
  "chunking": {
    "min_size": 16384,
    "avg_size": 65536,
    "max_size": 262144
  },
  "chunks": [
    { "index": 0, "hash": "<blake3 hex>", "length": 65536 },
    { "index": 1, "hash": "<blake3 hex>", "length": 32768 }
  ],
  "created_at_unix": 0
}
```

`created_at_unix` is reserved and written as zero so the content-addressed
manifest bytes and file ID are deterministic. The real creation time is stored
in the redb `files` table. `file_hash` enables end-to-end verification after
reassembly. Each chunk hash is
already verified by iroh-blobs during transfer.

### 4.3 redb tables

- `files`: `file_id` (32 bytes) -> `FileRecord` JSON (name, size, file hash,
  chunk count, creation time)
- `names`: logical file name -> `file_id`
- `replicas`: `file_id` -> list of `EndpointId`
- `peers`: `EndpointId` -> serialized `EndpointAddr` + last_seen + online flag
- `config`: optional persisted node settings

Implemented: `files`, `names`, `replicas`, and `peers`. `config` remains a
future table.

Values are JSON-encoded where the schema may evolve; keys are raw 32-byte hashes
or UTF-8 names.

### 4.4 Control protocol

ALPN: `b"p2p-storage/control/1"`.

Each request opens a new bidirectional QUIC stream and exchanges one JSON object
per line, or a single length-prefixed JSON frame. Messages:

```rust
enum ControlRequest {
    Hello { version: u32, endpoint: EndpointAddr },
    Ping,
    Status,
    ListManifests,
    GetManifest { file_id: Hash },
    GetTicket { file_id: Hash },
    FindByName { name: String }, // M2d
    Replicate { file_id: Hash, source: EndpointId },
    Repair { file_id: Hash },
}

enum ControlResponse {
    HelloAck { version: u32, files: u64, chunks: u64 },
    Pong { online: bool },
    Status { ... },
    Manifests { files: Vec<FileSummary> },
    Manifest { manifest: Manifest },
    Ticket { ticket: BlobTicket },
    NameMatches { matches: Vec<NameMatch> }, // M2d
    ReplicateAck { file_id: Hash, provider: EndpointId },
    Error { message: String },
}

struct NameMatch { // M2d
    file_id: Hash,
    file_name: String,
    file_size: u64,
    chunk_count: usize,
    created_at_unix: u64,
    ticket: BlobTicket,
}
```

`GetTicket` returns a ticket for a locally held file ID. `FindByName` performs
an exact, case-sensitive lookup in the receiver's `names` table and returns the
current local mapping for that peer. An unknown name returns an empty
`NameMatches` list. M2d bumps `CONTROL_VERSION`; all nodes must run the same
version.

`Replicate` instructs the receiver to fetch the bundle `file_id` from `source`
using iroh-blobs. The receiver tags the bundle locally, updates its replica
records, and acknowledges.

---

## 5. Repository Layout

Keep a single crate initially; split modules by responsibility:

```text
p2p/
├── Cargo.toml
├── IMPLEMENTATION_PLAN.md
└── src/
    ├── main.rs          # CLI entry point, dispatch only
    ├── cli.rs           # clap definitions and command handlers
    ├── config.rs        # paths, chunk params, replication factor, timeouts
    ├── error.rs         # shared error types
    ├── identity.rs      # load/generate/persist SecretKey
    ├── chunker.rs       # FastCDC -> Vec<Chunk>
    ├── manifest.rs      # Manifest, ChunkRef, serialization
    ├── bundle.rs        # create/load Collection, tags, reassembly
    ├── metadata.rs      # redb tables and CRUD
    ├── node.rs          # daemon lifecycle, Router, background tasks
    ├── protocol.rs      # control ALPN handler and messages
    ├── replication.rs   # placement policy, replicate, repair
    ├── retrieval.rs     # provider selection, download, fallback
    └── http.rs          # local axum API + status payloads
```

A future `crates/` workspace split is possible once the module boundaries are
proven.

---

## 6. Process and CLI Contract

### Node daemon

```bash
p2p node --data-dir ./data --api 127.0.0.1:7001
```

M2b/M2c implement the `--api` flag, `api.addr` discovery, the HTTP API below,
and remote retrieval. M2d adds exact-name lookup over the control protocol.

- Loads or creates the identity key.
- Opens the `FsStore` and metadata database.
- Starts the iroh endpoint and Router with both ALPNs.
- Bounds the initial N0 online wait to 10 seconds so local direct operation is
  possible when relay/DNS is unavailable.
- Serves incoming blob and control requests.
- Exposes a localhost-only HTTP API.
- Performs remote retrieval with provider fallback and local caching.
- Heartbeat and repair tasks arrive in M4.

### Client commands

```bash
p2p [--api <addr>] put <file>
p2p [--api <addr>] get <name> <output>          # M2d: searches known peers
p2p [--api <addr>] get <file_id|ticket> <output>
p2p [--api <addr>] search <name>                 # M2d
p2p [--api <addr>] list
p2p [--api <addr>] status
p2p [--api <addr>] health
p2p [--api <addr>] info <file_id|name>
p2p [--api <addr>] ticket <file_id|name>
p2p [--api <addr>] peers list
p2p [--api <addr>] peers add <endpoint-id> [--addr <ip:port>]
```

Client commands talk to the local daemon over HTTP. This allows any node to
serve as the ingress point while the daemon stays alive for replication and
fault-tolerance behavior.

### Local HTTP API

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Node id, storage, CPU, memory, latency, bandwidth, reliability, failure rate, load |
| GET | `/status` | Node id, files, chunks, peers, API address, uptime |
| GET | `/files` | List stored files |
| POST | `/files` | Ingest a local path (`{"path": "..."}`) |
| GET | `/files/{id}` | Manifest, metadata record, and replicas |
| POST | `/fetch` | Retrieve a local or remote target; M2d adds remote name lookup (`{"target": "<id|name|ticket>", "output": "..."}`) |
| GET | `/search?name=<name>` | M2d: list exact local and known-peer matches |
| GET | `/files/{id}/ticket` | HashSeq ticket for the file |
| GET | `/peers` | List known peers |
| POST | `/peers` | Add a peer (`{"endpoint": {...}}`) |

`/health` metrics are sampled every two seconds. `free_storage` is bytes,
`cpu_usage`/`memory_usage` are percentages, `latency` is milliseconds,
`bandwidth` is bytes/second, `reliability`/`failure_rate` are ratios in `0..=1`,
and `current_load` is active tracked operations divided by eight, clamped to
`0..=1`. The repair endpoint is deferred to M4.

Bind to `127.0.0.1` by default; no authentication is needed for the capstone
demo, but this must be documented as a local-only API.

---

## 7. Flows

### 7.1 Ingest (`put`)

1. Read the file as a stream and compute the whole-file BLAKE3 hash.
2. Run FastCDC over the stream, producing chunks.
3. Add each chunk to the `FsStore`; record whether the chunk already existed
   (dedup counter).
4. Build `manifest.json` with chunk order, lengths, and file hash.
5. Add the manifest as a blob.
6. Build an iroh `Collection` from `manifest.json` + `chunk-*` entries; store it
   and tag the root as `file/<file_id>`.
7. Record the file and local replica in redb.
8. Select `replicas - 1` peers via `PlacementPolicy`.
9. Send `Replicate { file_id, source: self_id }` to each peer concurrently.
10. Wait for acknowledgements with a timeout. Success requires at least
    `min_replicas` (default 2) total holders.
11. Return `{ file_id, ticket, replicas, bytes_saved }`.

### 7.2 Replication (receiver side)

1. Validate the request and the source peer.
2. `Downloader` fetches the bundle `HashSeq` from `source`.
3. Confirm all child chunks and the manifest are present.
4. Tag the bundle as `file/<file_id>`.
5. Update `files` and `replicas` tables.
6. Return `ReplicateAck`.

### 7.3 Retrieval (`get`)

1. Parse the target as a ticket, file ID, or name. If a name is not local,
   follow the M2d flow in 7.4.
2. Resolve the `file_id`. If the bundle is already local, reassemble it without
   touching the network.
3. Build provider candidates from the ticket address, recorded replicas, and
   known peers.
4. Query known peers concurrently with `GetTicket` (3 second timeout) to find
   holders and obtain their current addresses.
5. Try providers in order. Connect directly with the full `EndpointAddr`
   (5 second timeout) and fetch the HashSeq bundle.
6. On provider failure, continue with the next candidate. If all fail, return
   `502` with the attempted providers.
7. Tag the downloaded bundle, record the file and local replica, and remember
   the provider peer.
8. Load the `Collection`, parse `manifest.json`, concatenate chunks, verify the
   whole-file hash, and write atomically via `<output>.tmp` + rename.
9. A failed retrieval leaves no output. Content-addressed partial chunks may be
   present but are not tagged or recorded as a complete file.
10. Repair of under-replicated bundles remains M4.

### 7.4 Name-based retrieval (M2d - planned)

1. Match names exactly and case-sensitively.
2. Check the local `names` table first. A local match always wins and never
   causes network traffic.
3. If the name is not local, query every known peer concurrently with
   `FindByName` and a 3 second timeout.
4. Ignore failed or timed-out peers and collect `NameMatch` responses.
5. Group matches by `file_id`. If one file ID has multiple providers, merge all
   provider addresses for fallback.
6. If different file IDs share the name, choose the highest
   `created_at_unix`; break ties deterministically by file ID. Local matches
   still take precedence.
7. If there are no matches, return `404` and write no output.
8. Fetch from the selected file's providers with the normal sequential
   fallback. If every provider fails, return `502`.
9. Cache and finalize the bundle exactly like ticket/file-ID retrieval.
10. `GET /search?name=<name>` returns grouped local and peer matches, sorted
    newest first, for display by `p2p search <name>`.

Why this is M2d, not M3: name lookup is a retrieval feature built on the
existing peer registry. M3 replication and M4 fault tolerance remain unchanged;
they make name-based retrieval more reliable by creating additional holders.

Known limits: only known peers are searched, peer-provided timestamps are
trusted, and there is no global index or partial-name matching.

### 7.5 Fault tolerance and repair

- A background heartbeat loop pings peers every 30 seconds.
- Peers that fail `N` consecutive heartbeats are marked offline.
- `repair` scans files, counts online replicas, and replicates missing copies
  from any available holder.
- Retrieval never depends on a single provider; provider lists are shuffled and
  retried.
- Timeouts and retry counts are configurable.

### 7.6 Placement seam for AI

```rust
#[async_trait]
trait PlacementPolicy {
    async fn select(
        &self,
        candidates: &[PeerInfo],
        replicas: usize,
        context: &PlacementContext,
    ) -> Vec<EndpointId>;
}
```

Default implementation: least-replicated, then round-robin. Later, an
`AiPlacementPolicy` can call the FastAPI node-selection service using collected
node metrics. No AI code is required for this plan.

---

## 8. Milestones

### M0 - Baseline cleanup and scaffolding

**Status: complete.**

- Add `.gitignore` entries for data directories, `*.db`, `identity.key`, and
  generated outputs.
- Stop tracking runtime artifacts (`p2p_data*/blobs.db`, `session.txt`,
  `downloaded.txt`, modified `test_data` outputs).
- Add dependencies: `serde`, `serde_json`, `redb`, `fastcdc`, `clap`, `tracing`,
  `tracing-subscriber`. (`axum` and `reqwest` are added with M2, when the local
  HTTP API and client are introduced.)
- Create module skeleton, `Config`, and shared error type.
- Preserve the existing `send`/`receive` commands as deprecated aliases until
  `put`/`get` are stable.

**Acceptance:** `cargo fmt --check`, `cargo clippy -- -D warnings`, and
`cargo test` pass; the existing transfer demo still works.

### M1 - Chunking, bundles, local storage

**Status: complete.**

- Implement persistent identity and configurable data directory.
- Implement FastCDC chunking and whole-file hashing.
- Implement manifest serialization.
- Implement bundle creation, tagging, loading, and reassembly.
- Implement redb metadata tables.
- Implement local-only `put` and `get` (no network replication).
- Add unit tests for chunker boundaries, manifest round-trip, and dedup.

**Acceptance:** A multi-chunk file and an empty file round-trip byte-identically
across a node restart; re-ingesting an identical file saves chunk bytes.

### M2 - Control protocol, transfer, and name lookup

M2 is split into three slices.

**M2a - transport core (complete)**

- Implement the control ALPN, framed messages, handler, and client.
- Implement the persistent peer registry.
- Implement `p2p node --data-dir` with a combined blob/control Router.
- Implement HashSeq ticket creation and verified bundle download.
- Add in-process integration tests using `MemoryLookup`.

**M2b - local HTTP API and daemon-backed CLI (complete)**

- Add `axum` and `reqwest`.
- Expose `/health`, `/status`, `/files`, `/files/{id}`, `/fetch`,
  `/files/{id}/ticket`, and `/peers`.
- Move `put/get/list` behind the daemon and add `peers`, `ticket`, `status`,
  `health`, and `info`.
- Track resource and operation metrics for `/health`.

**M2c - remote CLI flow and hardening (complete)**

- Implement `get <file_id|name|ticket>` end-to-end through `POST /fetch`.
- Discover file-ID holders through the control protocol.
- Fetch directly from full provider addresses, with sequential fallback.
- Add bounded peer-query and provider-connect timeouts.
- Cache successful downloads, record replicas, and return `400`/`404`/`502`
  errors with actionable messages.
- Guarantee no partial output file on failure.
- Document the two-node daemon lifecycle and demo flow.

**Acceptance:** Node B fetches a bundle from Node A by ticket/file ID; if A is
unavailable, retrieval fails cleanly with a useful error and no partial output.

**M2d - name-based retrieval (planned)**

- Add `FindByName` and `NameMatch` to the control protocol; no redb schema
  migration is needed.
- Add `p2p search <name>` and `GET /search?name=<name>`.
- Make `p2p get <name>` search known peers when no local name exists.
- Use exact, case-sensitive matching with local precedence.
- Merge providers for identical file IDs and select the newest upload when
  different files share a name.
- Keep `400`/`404`/`502` behavior and no-partial-output guarantees.
- Document that M2d does not replace M3/M4; it only exposes retrieval by name.

**Acceptance:** Node A stores `report.pdf`. Node B has A in its peer list and
runs `p2p get report.pdf output.pdf` without a ticket; the bytes match. An
unknown name returns `404` with no output. If two peers hold different files
named `report.pdf`, the newest wins.

### M3 - Replication

- Implement `PlacementPolicy` and the default policy.
- Implement `Replicate` handling and replica tracking.
- Implement `put --replicas N` and acknowledgement timeouts.
- Implement `status` showing file, chunk, and replica state.
- Record dedup savings.

**Acceptance:** With 4 nodes and replication factor 3, a file is present on 3
nodes; retrieval by file ID or filename succeeds after the original ingress
node is stopped. The M3 replication tasks are otherwise unchanged by M2d.

### M4 - Fault tolerance and repair

- Add heartbeat/status tasks and offline detection.
- Add periodic and on-demand repair.
- Add retrieval retries and provider blacklisting for the current attempt.
- Add fault-injection integration tests: kill one node, kill two nodes, restart,
  and re-replicate.
- Add timeouts for all control and transfer operations.

**Acceptance:** With replication factor 3, killing one node does not affect
retrieval; killing two still allows retrieval if one replica remains; repair
restores the target replica count when a node returns or a new peer joins.

### M5 - Polish, benchmarks, and demo

- Add atomic writes, file-size limits, and input validation.
- Add structured logging and a `/status` summary suitable for a dashboard.
- Add a demo script or optional Docker Compose file for 3-4 nodes.
- Benchmark storage savings, retrieval latency (p50/p95), and recovery time.
- Update `README`/`p2p.md` with the actual implemented architecture.
- Document limitations and the deferred AI integration.

**Acceptance:** A single documented demo adds a file, shows replica locations,
kills a peer, retrieves the file, and reports metrics.

---

## 9. Test Plan

### Unit tests

- FastCDC: empty input, input smaller than minimum, exact boundary sizes,
  random data, deterministic boundaries across repeated runs.
- Manifest: JSON round-trip, version mismatch, invalid chunk index.
- Bundle: create/load/reassemble, empty file, one chunk, many chunks.
- Metadata: CRUD for files, names, replicas, peers; restart persistence.
- Placement: deterministic default policy, insufficient candidates.

### Integration tests (`tokio::test`)

- Two nodes: put on A, get on B by ticket without pre-registering A.
- Two nodes: get by file ID after adding A as a peer.
- Control: `FindByName` returns a `NameMatch` containing a ticket for an exact
  local name.
- Name lookup: unknown and differently-cased names return no matches.
- Two nodes: `get <name>` retrieves from a known peer without a ticket.
- Name lookup without the holder in the peer list returns `404` and no output.
- Non-holding peers are skipped without failing the search.
- Duplicate names: the newest remote upload wins; local names take precedence.
- Identical file IDs on multiple peers are merged for provider fallback.
- `/search?name=<name>` reports local and remote matches.
- Provider fallback: ticket provider unavailable, known peer serves the bundle.
- Cached retrieval works after the original provider stops.
- Invalid and non-HashSeq tickets are rejected without output.
- Unavailable providers return a clear error without output.
- Three nodes: replication factor 2 and 3.
- Provider failure: stop A after put, fetch from B.
- Partial failure: one provider returns an error, retrieval tries the next.
- Repair: remove a replica, run repair, confirm replica count restored.
- Identity: restart preserves `EndpointId` and data.
- Tamper test: corrupt a chunk on disk, confirm BLAKE3 verification rejects it
  and the file is not written.

Use `presets::Minimal` + `MemoryLookup` for deterministic tests. Use
`presets::N0` only for the manual/demo path.

### Current M2c two-node demo

1. Start node A on `127.0.0.1:7001` and node B on `127.0.0.1:7002` with
   separate data directories.
2. Put a multi-chunk file on A and print its ticket.
3. Fetch the ticket on B and verify the output checksum.
4. Stop A and fetch the cached bundle again from B.
5. For file-ID retrieval, add A to B with `p2p peers add`, then fetch by ID.
6. Retry with an unavailable provider and confirm a clear error with no output.

### Target M2d name-based demo

1. Start nodes A and B with separate data directories.
2. Add A to B's peer list once with `p2p peers add`.
3. Put `report.pdf` on A.
4. On B, run `p2p search report.pdf` and show the match.
5. On B, run `p2p get report.pdf output.pdf` without a ticket.
6. Verify `cmp` succeeds, then stop A and fetch the now-cached file again by
   name.
7. Add node C with the same name on two peers to show newest-upload selection.

### Target M3/M5 demo

1. Start 3 nodes with separate data directories.
2. Add peers to each node.
3. Put a multi-chunk file on node 1 with replication factor 3.
4. Show `status` and replica holders.
5. Stop node 1.
6. Fetch the file from node 2 and verify the checksum.
7. Stop node 2 and fetch from node 3.
8. Restart node 1, run repair, and show the replica count restored.

### Quality gates

- `cargo fmt --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test`
- No runtime data committed to git.
- Manual demo succeeds from a clean checkout.

---

## 10. Metrics to Capture

- **Dedup:** bytes written vs. logical bytes ingested, chunk hit rate.
- **Retrieval:** p50/p95 latency by file size and provider count.
- **Replication:** time to replicate, success/failure counts.
- **Fault tolerance:** detection time, repair time, retrieval success rate with
  1 and 2 peers down.
- **Storage:** total blob bytes, number of chunks, per-peer distribution.

These feed directly into the evaluation section of `p2p.md`.

---

## 11. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| iroh-blobs GC removes chunks | Tag the bundle root; test data persistence across restart and repair. |
| Many small chunk blobs | Use 64 KiB average chunk size; benchmark against whole-file blobs. |
| Multi-process store locking | Daemon owns the store; all CLI operations go through local HTTP. |
| Relay/DNS flakiness in tests | Use in-memory discovery for tests; reserve N0 for manual demos. |
| Control protocol abuse | Allowlist peers for `Replicate`; bind the local HTTP API to localhost. |
| Name lookup only sees known peers | Search all registered peers; document the static-registry limit. A tracker/DHT remains deferred. |
| Duplicate filenames | Select the newest remote upload; keep older files reachable by file ID and allow `search` to show matches. |
| Untrusted timestamps | Treat the newest-name policy as a convenience for the capstone; authentication is deferred. |
| Partial files after crash | Write to a temp path and atomically rename after hash verification. |
| Disk exhaustion | Check free space before ingestion/replication; expose limits in config. |
| Scope creep into AI | Keep `PlacementPolicy` as a trait; AI integration is a later milestone. |

---

## 12. Out of Scope (Deferred)

- Embedding-based near-duplicate detection.
- AI node selection / adaptive replication integration.
- Central tracker or Kademlia DHT.
- A global filename index or partial/substring name search; M2d searches only
  known peers with exact names.
- Full monitoring dashboard UI (a JSON status API is included).
- Authentication/authorization beyond localhost binding and peer allowlists.
- Erasure coding; this plan uses full-copy replication.

---

## 13. Definition of Done

The core extension is complete when:

1. A node has a stable identity and persistent chunk store.
2. Files are chunked, bundled, deduplicated, and reassembled correctly.
3. Multiple nodes can register each other and transfer bundles.
4. A configurable replication factor is enforced and visible via `status`.
5. Retrieval by filename works across known peers without a ticket.
6. Retrieval survives at least one offline provider and repairs under-replication.
7. Unit, integration, and fault-injection tests pass.
8. The demo and metrics are documented.
9. No runtime artifacts are tracked in git.
