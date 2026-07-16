# Testing the CDC Connector

This guide covers how to run the connector end-to-end and how to verify both the
**happy path** and the **failure / resilience scenarios** described in `plan.md`
(DLQ, crash recovery, idempotency, graceful shutdown, partition routing).

Assumes you have Docker (the stack is defined in `docker-compose.yml`).

---

## 1. Start the stack

```bash
docker compose up -d
# wait until all three are healthy
docker compose ps
```

Services: `redis` (6379), `postgres` (5432, `wal_level=logical`), `opensearch` (9200).

## 2. Initialize PostgreSQL (one time)

The producer connects to an **existing** replication slot, so create the table,
publication, and slot once:

```bash
docker compose exec -T postgres psql -U postgres -d cdc_db <<'SQL'
CREATE TABLE IF NOT EXISTS users (
  id    serial PRIMARY KEY,
  name  text NOT NULL,
  email text
);
CREATE PUBLICATION users_pub FOR TABLE users;
SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
SQL
```

> If you need to reset: `SELECT pg_drop_replication_slot('my_slot');` then recreate.

## 3. Happy path

Start the **consumer first** (it creates the `os_indexer` group at `$`, i.e. it
only tails *new* messages), then the producer, then generate changes.

```bash
# Terminal A — consumer
PARTITIONS=16 OPENSEARCH_URL=http://localhost:9200 \
  ./target/debug/redis-connector cdc-consumer

# Terminal B — producer
PARTITIONS=16 ./target/debug/redis-connector cdc-producer

# Terminal C — make changes
docker compose exec postgres psql -U postgres -d cdc_db -c \
  "INSERT INTO users(name,email) VALUES ('alice','a@x.com'); \
   UPDATE users SET email='a2@x.com' WHERE id=1; \
   DELETE FROM users WHERE id=1;"
```

### What to verify (happy path)

**Producer logs** (`Terminal B`) should print a `COMMIT` per transaction:
```
Connected to Redis at redis://127.0.0.1:6379
Connected to PostgreSQL replication slot my_slot
COMMIT lsn=19976B0 events=1 -> publishing to Redis
```

**Redis** — the change landed on exactly one partitioned stream (`P = fnv(table+pk) % 16`):
```bash
docker compose exec redis redis-cli XLEN events:0   # most partitions are 0
# find the non-empty one:
for p in $(seq 0 15); do n=$(docker compose exec -t redis redis-cli XLEN events:$p); [ "$n" != "0" ] && echo "events:$p = $n"; done
```

**Consumer group** — `os_indexer` exists, `pending` trends back to 0:
```bash
docker compose exec redis redis-cli XINFO GROUPS events:0
```

**OpenSearch** — the document exists with the correct final state and an external version:
```bash
curl -s "localhost:9200/cdc-users/_search?pretty" | head -40
# single-row check:
curl -s "localhost:9200/cdc-users/_doc/users:id=1?pretty"
```
Expect `_index: cdc-users`, `_id: users:id=1:email=...`, `_source` reflecting the
latest value, and `_version` equal to the commit LSN.

---

## 4. Verification cheat-sheet

| Check | Command |
|-------|---------|
| Stream lengths per partition | `for p in $(seq 0 15); do docker compose exec -t redis redis-cli XLEN events:$p; done` |
| Consumer group / pending | `docker compose exec redis redis-cli XINFO GROUPS events:<P>` |
| Inspect a stream entry | `docker compose exec redis redis-cli XRANGE events:<P> - + COUNT 1` |
| DLQ length | `docker compose exec redis redis-cli XLEN events:dlq` |
| DLQ contents | `docker compose exec redis redis-cli XRANGE events:dlq - + COUNT 5` |
| OpenSearch indices | `curl -s "localhost:9200/_cat/indices?v"` |
| OpenSearch doc count | `curl -s "localhost:9200/cdc-users/_count?pretty"` |
| Pending (PEL) across all partitions | `for p in $(seq 0 15); do docker compose exec -t redis redis-cli XINFO GROUPS events:$p 2>/dev/null | grep -A1 pending; done` |

---

## 5. Failure / bad-scenario tests

### 5.1 DLQ — malformed payload (decode failure)

A message the consumer cannot deserialize must go straight to the DLQ and be
acknowledged (so it doesn't block the stream).

```bash
docker compose exec redis redis-cli XADD events:0 '*' payload 'this is not valid json'
```

Verify:
```bash
docker compose exec redis redis-cli XLEN events:dlq        # -> 1
docker compose exec redis redis-cli XRANGE events:dlq - + COUNT 1   # shows the raw bad payload
docker compose exec redis redis-cli XLEN events:0          # unchanged (acked, not left in PEL)
```

### 5.2 DLQ — mapping conflict (poison pill, 3 retries)

This exercises the `>=3` mapping-failure rule. Force OpenSearch to reject a
document by giving the index a mapping that conflicts with the string data the
producer sends (e.g. require `email` to be an integer).

```bash
# Put a strict mapping on the index (do this BEFORE inserting)
curl -s -XPUT "localhost:9200/cdc-users" -H 'Content-Type: application/json' -d '{
  "mappings": { "properties": { "email": { "type": "integer" } } }
}'
```

Run the consumer with a **fast** reclaim/retry config so the test doesn't take
minutes:
```bash
PARTITIONS=16 OPENSEARCH_URL=http://localhost:9200 CLAIM_IDLE_MS=2000 CLAIM_INTERVAL_S=5 POISON_MAX_RETRIES=3 \
  ./target/debug/redis-connector cdc-consumer
```

Now insert a user (string email → OpenSearch mapping error):
```bash
docker compose exec postgres psql -U postgres -d cdc_db -c \
  "INSERT INTO users(name,email) VALUES ('bob','bob@x.com');"
```

Verify the poison flow:
```bash
# pending on the partition grows (message not acked, left for reclaim)
docker compose exec redis redis-cli XINFO GROUPS events:<P>     # pending > 0
# after ~3 reclaim cycles (CLAIM_INTERVAL_S * POISON_MAX_RETRIES) it lands in DLQ:
docker compose exec redis redis-cli XLEN events:dlq            # -> 1
# and the bad index gets deleted (mapping conflict -> index rejected):
curl -s "localhost:9200/_cat/indices?v" | grep cdc || echo "cdc-users absent (rejected)"
```

Reset for the next test: `curl -s -XDELETE "localhost:9200/cdc-users"`.

### 5.3 Crash recovery — `XAUTOCLAIM`

Kill the consumer while messages are in flight; on restart, orphaned (pending)
messages are reclaimed and reprocessed.

1. Generate a burst of changes.
2. While they're being consumed, `Ctrl-C` the consumer (or `kill <pid>`).
3. Restart the consumer.
4. Watch the PEL drain back to 0 and the OpenSearch doc count settle.

```bash
# pending before/after restart
for p in $(seq 0 15); do docker compose exec -t redis redis-cli XINFO GROUPS events:$p 2>/dev/null | grep -A1 pending; done
```
Because of external versioning, reprocessing is **idempotent** — re-delivered
messages with an older/equal version return `409` and are still `XACK`ed, so the
doc count does not double.

### 5.4 Idempotency / stale (`409`)

The version is `commit_lsn + in-transaction index`. If a message is delivered
out of order (older version than what's already indexed), OpenSearch returns
`409`; the worker treats it as `Stale` and `XACK`s it without duplicating.

To observe: with the consumer stopped, `XADD` a manual payload with a low
`version` for an already-indexed `users:id=1`, then start the consumer. The
message is acked and the existing doc is untouched.
```bash
docker compose exec redis redis-cli XADD events:0 '*' payload \
  '{"version":1,"event":{"Insert":{"table":"users","key":{"id":"1"},"data":{"name":"stale","email":"x"}}}}'
```
Then confirm `cdc-users/_doc/users:id=1` keeps its higher-version value.

### 5.5 Graceful shutdown

`Ctrl-C` / `SIGTERM` the consumer. It should finish the current batch, `XACK`
outstanding messages it processed, print `shutdown signal received` / `partition N shutting down`, and exit. Messages still pending remain in the PEL and are
reclaimed on the next start (see 5.3).

### 5.6 Producer resume after crash (no data loss)

The producer only acknowledges the Postgres LSN **after** a successful `XADD`.
Kill the producer mid-stream; the slot's `confirmed_flush_lsn` will not have
advanced past un-flushed transactions, so on restart it replays them. Because
OpenSearch uses external versioning, replaying the same transaction is safe.

```bash
docker compose exec postgres psql -U postgres -d cdc_db -c \
  "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='my_slot';"
```
Compare before/after a kill to confirm the LSN did not advance without a
successful publish.

### 5.7 Partition routing stability

All mutations for the same row must hash to the same `events:P` (preserves
per-row order). Insert the same `id` repeatedly and confirm the **same**
partition grows:

```bash
docker compose exec postgres psql -U postgres -d cdc_db -c \
  "INSERT INTO users(name,email) VALUES ('x','x@x.com'); \
   UPDATE users SET email='y@x.com' WHERE id=1; \
   UPDATE users SET email='z@x.com' WHERE id=1;"
# the three events all land on the same events:P
for p in $(seq 0 15); do n=$(docker compose exec -t redis redis-cli XLEN events:$p); [ "$n" != "0" ] && echo "events:$p = $n"; done
```

---

## 6. Cleanup

```bash
docker compose down            # stop containers (add -v to wipe volumes)
# rebuild after code changes:
cargo build
```

### Environment variables (quick reference)

| Var | Default | Notes |
|-----|---------|-------|
| `PARTITIONS` | `16` | must match between producer & consumer |
| `REDIS_URL` | `redis://127.0.0.1:6379` | |
| `PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE` | `127.0.0.1/5432/postgres/password/cdc_db` | |
| `PGSLOT` / `PGPUBLICATION` | `my_slot` / `users_pub` | must pre-exist |
| `OPENSEARCH_URL` | `http://127.0.0.1:9200` | consumer only |
| `OPENSEARCH_USER` / `OPENSEARCH_PASSWORD` | none | basic auth if set |
| `CONSUMER_GROUP` | `os_indexer` | |
| `OS_INDEX_PREFIX` | `cdc-` | |
| `CONSUMER_DLQ` | `events:dlq` | |
| `CONSUMER_BATCH` | `500` | |
| `CLAIM_IDLE_MS` | `30000` | XAUTOCLAIM idle threshold |
| `CLAIM_INTERVAL_S` | `60` | XAUTOCLAIM sweep interval |
| `POISON_MAX_RETRIES` | `3` | mapping failures before DLQ |
