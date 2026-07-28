# P2P Distributed Storage System with AI-Assisted Deduplication
### Capstone Project Document

---

## 1. Project Overview

**Goal:** Build a Rust-based peer-to-peer distributed storage system (conceptually similar to a minimal IPFS/BitTorrent hybrid) that splits files into content-addressed chunks, replicates them across multiple peer nodes for fault tolerance, and retrieves/reassembles them on demand. An AI-driven layer sits on top to make the system "smart" — detecting near-duplicate content using embeddings (or optimizing replica placement using access-pattern prediction) so storage and retrieval are more efficient than a naive fixed-rule system.

**Team split:**
- **Partner A (Rust / Systems):** chunking, content addressing, peer networking, replication, fault tolerance, retrieval, benchmarking
- **Partner B (AI/ML):** embedding model for similarity detection, evaluation of dedup effectiveness (or predictive replica placement), integration hook into the storage layer

**Why this project works well:** it is systems-first (plays to Rust/concurrency strengths), has a clearly separable and gradeable AI component, and produces concrete, measurable results (storage savings, latency, fault-tolerance behavior) that make for a strong final report and demo.

---

## 2. System Architecture

```
                     ┌─────────────────────┐
                     │   Client / CLI       │
                     │  (add file / get)     │
                     └──────────┬───────────┘
                                │
                     ┌──────────▼───────────┐
                     │      Chunker          │
                     │ (content-defined split)│
                     └──────────┬───────────┘
                                │
                     ┌──────────▼───────────┐
                     │  Content Addressing   │
                     │ (BLAKE3 hash per chunk)│
                     └──────────┬───────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                                     │
   ┌──────────▼───────────┐          ┌──────────────▼─────────────┐
   │  AI Similarity Layer   │          │   Replication Manager       │
   │ (embeddings / near-dup)│          │ (fixed or smart placement)  │
   └──────────┬───────────┘          └──────────────┬─────────────┘
              │                                       │
              └───────────────────┬───────────────────┘
                                   │
                     ┌─────────────▼─────────────┐
                     │   Peer Discovery / Routing  │
                     │ (tracker or Kademlia DHT)    │
                     └─────────────┬─────────────┘
                                   │
                     ┌─────────────▼─────────────┐
                     │   P2P Networking Layer      │
                     │ (async, multi-peer transfer) │
                     └─────────────┬─────────────┘
                                   │
                     ┌─────────────▼─────────────┐
                     │  Retrieval & Reassembly     │
                     │ (fetch chunks, verify hash,  │
                     │   rebuild file)              │
                     └─────────────┬─────────────┘
                                   │
                     ┌─────────────▼─────────────┐
                     │   Monitoring Dashboard      │
                     │ (storage savings, latency)   │
                     └─────────────────────────────┘
```

---

## 3. Step-by-Step Build Plan

### Phase 0 — Setup & Design (Week 1)
1. Decide chunking strategy: fixed-size vs. content-defined chunking (CDC).
2. Decide peer discovery strategy: start with a simple central tracker/registry; treat a full Kademlia DHT as a stretch goal.
3. Decide the AI hook: near-duplicate detection via embeddings (recommended primary) vs. predictive replica placement (stretch/secondary).
4. Set up repo structure (Cargo workspace with separate crates: `chunker`, `store`, `network`, `cli`), plus a separate Python project for the AI model.
5. Set up basic CI (cargo test, cargo clippy) and a shared design doc.

### Phase 1 — Core Storage Engine (Weeks 2–3)
1. Implement content-defined chunking using a rolling hash (e.g., Rabin fingerprint or FastCDC-style algorithm).
2. Hash each chunk with BLAKE3 to generate a content ID.
3. Implement local chunk storage (on-disk key-value store, e.g., using `sled` or `redb`, keyed by hash).
4. Implement file manifests: an ordered list of chunk hashes representing a file, plus metadata (filename, size, timestamp).
5. Write unit tests: chunk → hash → store → retrieve → reassemble → verify byte-identical output.

### Phase 2 — Networking & Peer Discovery (Weeks 4–5)
1. Build the async networking layer with `tokio`.
2. Implement peer registration with a simple tracker service (a node announces itself and the chunks it holds).
3. Implement basic peer-to-peer protocol messages: `HAVE(hash)`, `WANT(hash)`, `DATA(hash, bytes)`.
4. Implement chunk transfer over TCP (or QUIC via `quinn`/`iroh` — see tech stack notes below).
5. (Stretch) Replace the central tracker with a Kademlia DHT using `libp2p`'s `kad` module for fully decentralized peer discovery.

### Phase 3 — Replication & Fault Tolerance (Weeks 6–7)
1. Implement a fixed replication factor (e.g., every chunk stored on 3 peers).
2. Implement peer health checks (heartbeat/ping) to detect offline nodes.
3. Implement re-replication: when a peer goes offline, detect under-replicated chunks and copy them to a healthy peer.
4. Write fault-injection tests: kill a peer mid-transfer, kill a peer holding needed chunks, verify retrieval still succeeds from replicas.

### Phase 4 — AI Similarity / Dedup Layer (Weeks 6–8, parallel with Partner A's Phase 3)
1. Partner B selects an embedding approach:
   - Images: a lightweight pretrained CNN feature extractor (e.g., a small ResNet or MobileNet) to generate perceptual embeddings.
   - Text/documents: a sentence-embedding model (e.g., a small SentenceTransformers model).
2. Build a pipeline: chunk/file → embedding → similarity comparison (cosine similarity) against existing stored embeddings.
3. Define a similarity threshold above which content is treated as a "near duplicate" and stored as a diff/reference rather than a full copy.
4. Build an evaluation set: a folder of files with known exact duplicates, near-duplicates (recompressed/edited), and unrelated files. Measure precision/recall of the dedup detector.
5. Define the integration contract with Partner A: given a new file's chunks, the AI layer returns a similarity score or "duplicate-of" reference; the Rust storage layer decides whether to store a full copy or a diff.

### Phase 5 — Integration (Week 9)
1. Wire the AI similarity service into the ingestion path (likely as a local HTTP/gRPC microservice the Rust code calls, or a shared file-based interface — decide early to avoid last-minute integration pain).
2. Add a manifest field marking chunks/files as "deduplicated" and linking to the canonical copy.
3. Re-run fault-injection and retrieval tests with dedup enabled to make sure nothing breaks.

### Phase 6 — Monitoring, Benchmarking & Demo Polish (Weeks 10–11)
1. Build a simple dashboard (CLI table or a small web UI) showing: total stored chunks, storage saved by dedup, per-peer chunk distribution, replication health.
2. Benchmark: storage footprint with vs. without the AI dedup layer, on a dataset containing intentional near-duplicates.
3. Benchmark: retrieval latency under normal conditions vs. with a peer offline (triggering fallback to replica).
4. Prepare the live demo script: add files from one node, kill a node, retrieve from another, show dedup savings on-screen.

### Phase 7 — Report & Presentation (Week 12+)
1. Write up architecture, design decisions (chunking strategy, replication factor, dedup threshold), and comparison against real systems (IPFS, BitTorrent) — what was simplified and why.
2. Include benchmark graphs (storage savings %, latency percentiles, fault-tolerance recovery time).
3. Prepare the live demo and slides.

---

## 4. Tech Stack

### Rust (Partner A)
| Purpose | Recommended crate(s) | Notes |
|---|---|---|
| Async runtime | `tokio` | Standard choice; needed for concurrent peer connections and transfers |
| P2P networking | `libp2p` (full framework) **or** `iroh` (higher-level, BLAKE3-native) | `libp2p` gives you full control (transports, Kademlia DHT, gossipsub) but has a steeper learning curve. `iroh` is a newer, higher-level Rust-native P2P stack built around QUIC and BLAKE3 content addressing — arguably a better fit for this exact project since it ships `iroh-blobs` (content-addressed blob transfer) out of the box |
| Content hashing | `blake3` | Very fast, parallelizable, ideal for content addressing |
| Content-defined chunking | `fastcdc` crate, or hand-rolled Rabin fingerprint | FastCDC is a well-documented, efficient CDC algorithm |
| Local key-value storage | `sled` or `redb` | Embedded, persistent, good for chunk storage keyed by hash |
| Serialization | `serde` + `bincode` or `postcard` | For manifests and network messages |
| REST/monitoring API | `axum` | For the dashboard/monitoring endpoint |
| CLI | `clap` | For the add/get/peer-list commands |
| Testing/fault injection | `tokio::test`, custom harness | Simulate peer kill/restart in integration tests |

**Two viable networking paths — decide early:**
- **Path 1 (recommended for time-boxed capstone): `iroh` + `iroh-blobs`.** Iroh is a Rust-native library for direct peer connections over QUIC, with built-in BLAKE3-based content-addressed blob transfer (`iroh-blobs`) and address/peer discovery already handled. This removes a lot of the networking plumbing you'd otherwise write by hand, letting you focus more time on replication logic, fault tolerance, and the AI integration.
- **Path 2 (more "from scratch," more systems depth): `libp2p`.** Gives you the actual building blocks (transports, stream multiplexing, Kademlia DHT via the `kad` module, gossipsub) and is the same stack real projects like IPFS and Filecoin's Forest client use. More impressive if your program values "built it yourself," but expect to spend meaningfully more time on networking than on replication/dedup logic.

### AI/ML (Partner B)
| Purpose | Recommended tool(s) | Notes |
|---|---|---|
| Embedding models (images) | Pretrained small CNN (MobileNetV3, ResNet-18) via `torchvision`, or CLIP's image encoder | For perceptual similarity between images |
| Embedding models (text/docs) | `sentence-transformers` (e.g., `all-MiniLM-L6-v2`) | Lightweight, fast, good general-purpose text embeddings |
| Similarity computation | `scikit-learn` (cosine similarity), `faiss` (if scaling to many embeddings) | `faiss` is worth learning if you want fast nearest-neighbor lookup at scale |
| Evaluation | Precision/recall scripts, `pandas` for result tables | For the dedup effectiveness benchmark |
| Serving the model to Rust | Small `FastAPI` microservice (simplest), or export to ONNX and call via `ort` crate directly from Rust (more integrated, more work) | FastAPI microservice is the pragmatic choice for a one-semester project; ONNX-in-Rust is a strong stretch goal |

### Shared / Supporting Tools
| Purpose | Tool |
|---|---|
| Version control | Git + GitHub |
| Containerization (for multi-peer demo) | Docker / Docker Compose (spin up N peer containers easily) |
| Diagramming (for report) | draw.io / Excalidraw |
| Benchmark data | A curated dataset with known exact + near duplicates (can be self-assembled from image sets with recompressed/cropped variants) |

---

## 5. Evaluation Metrics for the Final Report

- **Storage efficiency:** % storage saved with AI dedup vs. hash-only dedup, on a dataset with known near-duplicates.
- **Dedup accuracy:** precision/recall of the similarity detector against a labeled duplicate/non-duplicate test set.
- **Fault tolerance:** time to detect an offline peer and re-replicate its chunks; success rate of file retrieval with 1 or 2 peers down.
- **Latency:** average and p95 chunk retrieval time, with varying peer counts and replication factors.
- **Scalability (optional stretch):** how performance changes as the number of peers or stored chunks grows.

---

## 6. Resources & Links

### P2P Networking & Content Addressing
- rust-libp2p (GitHub): https://github.com/libp2p/rust-libp2p
- libp2p Rust docs: https://docs.rs/libp2p
- libp2p official site/specs: https://libp2p.io
- Iroh (Rust P2P + QUIC + BLAKE3 blobs): https://docs.rs/iroh
- iroh-blobs docs: https://docs.rs/iroh-blobs
- Example: minimal P2P file sharing with Iroh: https://github.com/Syed-Imthiaz/p2p-file-sharing-rust-iroh
- IPFS docs (concepts to compare against): https://docs.ipfs.tech
- Kademlia DHT paper (original): search "Kademlia: A Peer-to-peer Information System Based on the XOR Metric" (Maymounkov & Mazières)

### Hashing & Chunking
- BLAKE3 crate: https://crates.io/crates/blake3
- BLAKE3 spec/repo: https://github.com/BLAKE3-team/BLAKE3
- fastcdc crate (content-defined chunking): https://crates.io/crates/fastcdc

### Storage
- sled (embedded DB): https://docs.rs/sled
- redb (embedded DB): https://docs.rs/redb

### Rust Async & Networking Basics
- Tokio docs: https://tokio.rs
- Tokio tutorial: https://tokio.rs/tokio/tutorial
- axum (web framework): https://docs.rs/axum
- quinn (QUIC implementation in Rust, underlies iroh): https://docs.rs/quinn

### AI/ML Side
- sentence-transformers docs: https://www.sbert.net
- torchvision pretrained models: https://pytorch.org/vision/stable/models.html
- FAISS (similarity search at scale): https://github.com/facebookresearch/faiss
- ONNX Runtime Rust bindings (`ort` crate): https://docs.rs/ort
- FastAPI (for the model-serving microservice): https://fastapi.tiangolo.com

### General Reference Reading
- "Designing Data-Intensive Applications" (Kleppmann) — chapters on replication and partitioning are directly relevant background reading.
- IPFS whitepaper: search "IPFS - Content Addressed, Versioned, P2P File System" (Benet, 2014)

---

## 7. Honest Risk Notes

- The **AI component is naturally secondary** to the systems work here — make sure Partner B's evaluation work (precision/recall benchmarking, ablation studies) is substantial enough to be clearly gradeable on its own, not just "we called a pretrained model once."
- **Kademlia DHT and libp2p have a real learning curve** — budget extra time if you choose that path over `iroh`, or explicitly scope the DHT as a stretch goal you can drop if time runs short.
- **Decide the Rust↔AI integration boundary (microservice vs. in-process ONNX) in Phase 0**, not later — this is the point where two-person projects most often lose a week to miscommunication.
