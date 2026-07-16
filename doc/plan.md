# Condensed CDC Architecture: Postgres -> Redis Streams -> OpenSearch

## 1. The Core Flow
**PostgreSQL** (WAL/`pgoutput`) $\rightarrow$ **Rust Producer** $\rightarrow$ **Redis Streams** ($N$ Partitions) $\rightarrow$ **Rust Tokio Workers** $\rightarrow$ **OpenSearch**

## 2. Key Algorithms
* **Partition Routing:** $P = \text{hash}(\text{table\_name} + \text{primary\_key}) \pmod{N}$
  * *Why:* Ensures all mutations for a specific row always go to the same stream, preserving strict chronological order.
* **LSN to Version (Idempotency):** $V = (\text{high} \ll 32) | \text{low}$
  * *Why:* Converts the Postgres Log Sequence Number into a 64-bit integer for OpenSearch's `external` versioning, preventing stale updates.

## 3. Component Breakdown

### Producer (Rust)
* Connects to Postgres logical replication slot.
* Parses `pgoutput` to extract table, operation (`I`/`U`/`D`), row data, and LSN.
* Calculates partition $P$ and publishes: `XADD events:P * ...`
* **Critical Rule:** Only acknowledges the LSN to Postgres *after* receiving a successful `XADD` response from Redis.

### Message Broker (Redis)
* **Topology:** $N$ separate stream keys (`events:0` through `events:N-1`).
* **Consumer Group:** A single group (e.g., `os_indexer`) spans all partitioned streams.

### Worker Fleet (Rust / Tokio)
* **Concurrency:** Single binary running $N$ isolated Tokio tasks. Each task is pinned to one `events:P` partition.
* **The Loop:**
  1. `XREADGROUP` (Fetch batch of ~500 messages).
  2. Transform data into OpenSearch `_bulk` format.
  3. Append `version_type=external` and `version=$V`.
  4. Send HTTP `_bulk` request to OpenSearch.
  5. `XACK` the processed message IDs back to Redis.

## 4. Resilience & Error Handling

* **Stale/Out-of-Order Messages:** Handled automatically by OpenSearch. If an incoming document's $V$ is $\le$ the existing version, it returns `409 Conflict` (which the worker ignores and `XACK`s).
* **Crashed Workers (Orphaned Messages):** 
  * Unacknowledged messages sit in the Redis Pending Entries List (PEL).
  * A background Tokio task runs `XAUTOCLAIM` every 60s to reclaim messages idle for $> 30$ seconds.
* **Poison Pills (Malformed Data):**
  * If a document fails OpenSearch mapping $\ge 3$ times, the worker publishes the raw payload to `events:dlq`.
  * The worker then `XACK`s the bad message on the main stream to prevent head-of-line blocking.
* **Graceful Shutdown:** Workers intercept `SIGTERM`/`SIGINT`, stop fetching new messages, drain current batches to OpenSearch, issue final `XACK`s, and exit.