# Real-Time Poll System — Architecture Reference

A production-grade design for a live polling application with real-time vote
counts, voter deduplication, and horizontal scalability. This document captures
every decision made during the design process, the problem each decision solves,
and the trade-offs accepted.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Stack](#2-stack)
3. [Overall Architecture Drawing](#3-overall-architecture-drawing)
4. [Component Breakdown](#4-component-breakdown)
   - 4.1 [API Instances](#41-api-instances)
   - 4.2 [Redis — Three Distinct Roles](#42-redis--three-distinct-roles)
   - 4.3 [Redis Streams + Consumer Groups](#43-redis-streams--consumer-groups)
   - 4.4 [Aggregator Worker Pool](#44-aggregator-worker-pool)
   - 4.5 [MongoDB — Vote Records](#45-mongodb--vote-records)
   - 4.6 [SSE Fan-out via easy-sse.go](#46-sse-fan-out-via-easy-ssego)
5. [Key Design Decisions](#5-key-design-decisions)
   - 5.1 [SSE over WebSockets](#51-sse-over-websockets)
   - 5.2 [Two-Tier Vote Accumulation](#52-two-tier-vote-accumulation)
   - 5.3 [Voter Identity and Deduplication Chain](#53-voter-identity-and-deduplication-chain)
   - 5.4 [One Stream Per Poll](#54-one-stream-per-poll)
   - 5.5 [Worker Self-Assignment via Redis Locks](#55-worker-self-assignment-via-redis-locks)
   - 5.6 [XACK Before XTRIM — No Message Loss](#56-xack-before-xtrim--no-message-loss)
   - 5.7 [XAUTOCLAIM — Orphan Recovery](#57-xautoclaim--orphan-recovery)
   - 5.8 [Redis for Live Counters, MongoDB for Records](#58-redis-for-live-counters-mongodb-for-records)
   - 5.9 [Worker Broadcasts via HTTP POST](#59-worker-broadcasts-via-http-post)
6. [Failure Scenarios and Mitigations](#6-failure-scenarios-and-mitigations)
7. [Durability Guarantee Chain](#7-durability-guarantee-chain)
8. [Project Structure](#8-project-structure)

---

## 1. System Overview

Users visit a poll page and connect via SSE to receive live result updates.
When a user submits a vote, the system must:

- Reject duplicate votes from the same email address
- Accept the vote without blocking on database writes
- Accumulate votes across multiple API instances before writing to the database
- Broadcast the updated totals to every connected SSE client across all instances
- Guarantee no vote is silently lost, even if a worker crashes mid-processing

The system is designed to handle many simultaneous polls, thousands of
concurrent SSE connections, and horizontal scaling of both API instances and
worker processes.

---

## 2. Stack

| Layer | Technology | Why |
|---|---|---|
| **API / SSE server** | Go | Lightweight goroutines make holding thousands of open SSE connections cheap. Goroutines for the local flush loop add negligible overhead. |
| **SSE library** | [easy-sse.go](https://github.com/gabriGutiz/easy-sse.go) | Provides `SseConnHandler` (subscribe client to a named channel) and `BroadcastHandler` (HTTP POST fan-out to all subscribers). Removes boilerplate and gives the worker a clean HTTP interface to trigger broadcasts. |
| **Message queue** | Redis Streams | Persistent, ordered, consumer-group-aware. Unlike pub/sub, messages survive restarts. `XREADGROUP` delivers each message to exactly one consumer. `XACK` marks completion; unACKed messages are redeliverable. |
| **Dedup gate** | Redis SET (`SADD`) | `SADD` is atomic. Returns 1 on first insertion, 0 if the email already exists. Used as the fast per-vote gate on the API layer before any buffering occurs. |
| **Live counters** | Redis Hash (`HINCRBY`) | Atomic increment with microsecond latency. Already in the stack. The worker reads current totals here to build the SSE payload — no database read needed per broadcast cycle. |
| **Vote records** | MongoDB | Flexible document model for vote records. Unique compound index `{ pollId, email }` provides the hard deduplication guarantee at the storage layer. Native `bulkWrite` + `insertMany` maps cleanly to the batch-write pattern. |
| **Worker** | Go | Same language as the API. Consumer group logic, HTTP POST broadcasting, and Redis lock management are straightforward to implement with goroutines. |
| **Service discovery** | Kubernetes headless service or Consul | The worker needs the addresses of all live API instances to POST the broadcast. A headless service exposes individual pod IPs via DNS. |

### Why not WebSockets?

Vote submission is a plain HTTP POST — one direction only. Result updates flow
server-to-client — also one direction only. WebSockets provide a bidirectional
channel that is never used in this application. The overhead is pure cost with
no benefit.

SSE on HTTP/2 is strictly better here:

- The browser's `EventSource` API auto-reconnects without any application code
- HTTP/2 multiplexes many SSE streams over a single TCP connection per client
- Standard L7 load balancers and CDN edges handle SSE transparently
- SSE workers are stateless — no sticky sessions, no socket broker

WebSockets become the right choice only when the client needs to send frequent
real-time data back (collaborative cursors, live editing, multiplayer games).

---

## 3. Overall Architecture Drawing

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              CLIENTS (Browsers)                              │
│                                                                              │
│  POST /polls/{pollId}/vote          GET /channels/{pollId}  (EventSource)    │
│  { email, optionId }                ← SSE result updates                     │
└────────────┬────────────────────────────────┬────────────────────────────────┘
             │                                │
             ▼                                ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                         API Instances  (Go, N pods)                        │
│                                                                            │
│  ┌──────────────────────────────────┐   ┌──────────────────────────────┐  │
│  │        Vote Handler              │   │      easy-sse.go             │  │
│  │                                  │   │                              │  │
│  │  1. SADD voted:{pollId} {email}  │   │  SseConnHandler              │  │
│  │     → 0: 409 Conflict, stop      │   │  registers client under      │  │
│  │     → 1: new voter, continue     │   │  channel = pollId            │  │
│  │                                  │   │                              │  │
│  │  2. 202 Accepted (immediate)     │   │  BroadcastHandler            │  │
│  │                                  │   │  receives POST from worker   │  │
│  │  3. append to local buffer       │   │  fans out to local clients   │  │
│  │     { pollId, email,             │   │                              │  │
│  │       optionId, ts }             │   └──────────────────────────────┘  │
│  │                                  │                                      │
│  │  ┌────────────────────────────┐  │                                      │
│  │  │  Flush goroutine (~20ms)   │  │                                      │
│  │  │  if buffer non-empty:      │  │                                      │
│  │  │    XADD votes:{pollId}     │  │                                      │
│  │  │      [{email,optId,ts}...] │  │                                      │
│  │  │    clear buffer            │  │                                      │
│  │  │  → 1 msg/instance/20ms     │  │                                      │
│  │  └────────────────────────────┘  │                                      │
│  └──────────────────────────────────┘                                      │
└──────────┬──────────────────────────────────────────────────────────────────┘
           │  bulk messages (≤ N msgs per 20ms, N = api instance count)
           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        Redis  (three roles)                                  │
│                                                                              │
│  ┌──────────────────────┐  ┌────────────────────────┐  ┌──────────────────┐ │
│  │  Streams             │  │  Dedup Gate            │  │  Live Counters   │ │
│  │                      │  │                        │  │                  │ │
│  │  votes:poll-001  ──► │  │  SET voted:{pollId}    │  │  HASH            │ │
│  │  votes:poll-002  ──► │  │  { email, email, ... } │  │  poll:{id}:      │ │
│  │  votes:poll-003  ──► │  │                        │  │  counts          │ │
│  │  votes:poll-004  ──► │  │  SADD → atomic         │  │                  │ │
│  │                      │  │  O(1) per vote         │  │  HINCRBY atomic  │ │
│  │  consumer group:     │  │                        │  │  HGETALL for     │ │
│  │  "workers"           │  │                        │  │  SSE payload     │ │
│  │                      │  │                        │  │                  │ │
│  │  PEL: unACKed msgs   │  │                        │  │                  │ │
│  │  survive crashes     │  │                        │  │                  │ │
│  └──────────────────────┘  └────────────────────────┘  └──────────────────┘ │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  Worker Lock Registry                                                │   │
│  │                                                                      │   │
│  │  SET lock:stream:poll-001 worker-A NX PX 10000                       │   │
│  │  SET lock:stream:poll-002 worker-B NX PX 10000                       │   │
│  │  SET lock:stream:poll-003 worker-A NX PX 10000                       │   │
│  │                                                                      │   │
│  │  NX = only set if not exists (atomic race-free assignment)           │   │
│  │  PX 10000 = auto-expire in 10s (dead worker recovery)               │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└──────────────────────┬───────────────────────────────────────────────────────┘
                       │
                       │  XREADGROUP BLOCK 100ms
                       │  (returns on first message or after 100ms timeout)
                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                     Aggregator Worker Pool  (Go, M pods)                     │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  Stream Discovery Loop  (every 5s, each worker)                      │   │
│  │                                                                      │   │
│  │  SCAN "votes:*"                                                      │   │
│  │  for each stream:                                                    │   │
│  │    SET lock:stream:{pollId} {workerId} NX PX 10000                   │   │
│  │    → OK  : XGROUP CREATE (idempotent), spawn drainLoop goroutine     │   │
│  │    → nil : owned by another worker, skip                             │   │
│  │                                                                      │   │
│  │  Lock Keepalive goroutine (every 5s):                                │   │
│  │    EXPIRE lock:stream:{pollId} 10                                    │   │
│  │    if key gone → lock lost, stop drainLoop, relinquish stream        │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  drainLoop goroutine  (one per owned stream)                         │   │
│  │                                                                      │   │
│  │  // Recover orphaned msgs from dead workers on startup               │   │
│  │  XAUTOCLAIM votes:{pollId} workers {workerId} 15000 0-0 COUNT 100    │   │
│  │  process any claimed msgs                                            │   │
│  │                                                                      │   │
│  │  loop:                                                               │   │
│  │    msgs = XREADGROUP GROUP workers {workerId}                        │   │
│  │               BLOCK 100ms COUNT 10000                                │   │
│  │               STREAMS votes:{pollId} >                               │   │
│  │    if empty → continue                                               │   │
│  │                                                                      │   │
│  │    flatten → []VoteRecord                                            │   │
│  │    dedup by email within batch   (second safety layer)              │   │
│  │                                                                      │   │
│  │    ── write phase (both must succeed before ACK) ──                  │   │
│  │                                                                      │   │
│  │    MongoDB bulkWrite:                                                │   │
│  │      insertMany { pollId, email, optionId, ts }                      │   │
│  │      unique index {pollId, email} → hard dedup, skip duplicates      │   │
│  │                                                                      │   │
│  │    Redis HINCRBY poll:{pollId}:counts {optionId} N  (per option)     │   │
│  │                                                                      │   │
│  │    ── commit ──                                                      │   │
│  │                                                                      │   │
│  │    XACK votes:{pollId} workers {msgIds...}                           │   │
│  │    XTRIM votes:{pollId} MINID {lastMsgId}                            │   │
│  │                                                                      │   │
│  │    totals = HGETALL poll:{pollId}:counts                             │   │
│  │                                                                      │   │
│  │    for each API instance (service registry):                         │   │
│  │      go POST /channels/{pollId}/broadcast   timeout: 200ms           │   │
│  │         (parallel goroutine per instance, fire-and-forget)           │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  Assignment example (4 polls, 3 workers):                                    │
│    Worker-A → poll-001, poll-003                                             │
│    Worker-B → poll-002                                                       │
│    Worker-C → poll-004                                                       │
│    Worker-A dies → lock expires → Worker-B takes poll-001 via XAUTOCLAIM     │
└─────────────────────────────┬────────────────────────────────────────────────┘
                              │
                              │  POST /channels/{pollId}/broadcast (parallel)
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
           API-1           API-2           API-3
        BroadcastHandler  BroadcastHandler  BroadcastHandler
        (easy-sse.go)
              │
        SSE fan-out to all
        local subscribers
        of that pollId
              │
              ▼
          Browsers receive:
          { optionA: 821, optionB: 543 }
```

---

## 4. Component Breakdown

### 4.1 API Instances

**Role:** Accept vote submissions, maintain SSE connections, accumulate votes
locally before forwarding to the queue.

**Problem solved — database overuse per vote:** Writing every individual vote
directly to MongoDB would saturate the database under any meaningful load. The
API instance acts as a first-tier buffer.

**Local accumulator (mutex-protected map):**

```
Vote arrives → SADD check → append to in-memory buffer
                                     │
                              flush goroutine
                              every ~20ms
                                     │
                              XADD votes:{pollId}
                              with all buffered records
```

The flush goroutine fires every ~20ms regardless of how many votes arrived.
At most one Redis Stream message is produced per API instance per poll per 20ms.
With 10 API instances, the queue receives at most 10 messages every 20ms across
all instances for a given poll — regardless of vote rate.

**Why 20ms for the local window?** It is meaningfully shorter than the worker's
100ms drain window, so the worker always has a full batch to process. If both
windows were equal, the worker might drain an empty stream on every cycle.

---

### 4.2 Redis — Three Distinct Roles

Redis serves three independent purposes in this system, all on the same cluster.

**Role 1 — Dedup Gate (`SET voted:{pollId}`):**

`SADD voted:{pollId} {email}` is called synchronously on every incoming vote,
before any buffering. It returns:

- `1` — email is new, proceed
- `0` — already voted, return 409 immediately

`SADD` is atomic at the Redis level. No two concurrent requests for the same
email can both receive `1`. This closes the race window that would exist if the
check-then-act happened at the application layer.

The SADD happens on the API instance, not after the worker commits. This is
deliberate: waiting for the worker to SADD would leave a 100ms race window
during which two different API instances could both accept the same email.

**Accepted trade-off:** If an API instance crashes after SADD but before XADD,
the email is marked as voted but the vote never reaches the stream. The vote is
lost and the user cannot re-vote. This window is at most ~20ms. The alternative
— SADD only after DB commit — has a 100ms race window and is harder to make
correct. The 20ms exposure is the accepted trade-off.

**Role 2 — Live Counters (`HASH poll:{pollId}:counts`):**

`HINCRBY poll:{pollId}:counts optionA N` is atomic and runs in microseconds.
The worker reads `HGETALL poll:{pollId}:counts` after every write cycle to
build the SSE payload. This avoids a MongoDB read on every broadcast cycle.

If Redis restarts and the hash is lost, it can be rebuilt from MongoDB vote
records with a recount query on startup.

**Role 3 — Worker Lock Registry (`SET lock:stream:{pollId} NX PX`):**

Described in section 4.4.

---

### 4.3 Redis Streams + Consumer Groups

**Why Redis Streams over pub/sub:**

Pub/sub messages are lost if no subscriber is listening at the moment of
publish. Redis Streams persist messages regardless of consumer state. If the
worker crashes, messages remain in the stream and are reprocessed when the
worker restarts or another takes over.

**Consumer group mechanics:**

```
XGROUP CREATE votes:{pollId} workers $ MKSTREAM
```

Creates the consumer group `workers` on the stream. `MKSTREAM` creates the
stream if it does not yet exist. The `$` means the group starts reading from
the current end — historical messages before the group was created are not
redelivered (they predate the group).

```
XREADGROUP GROUP workers {workerId} BLOCK 100ms COUNT 10000 STREAMS votes:{pollId} >
```

The `>` means "deliver messages not yet delivered to any consumer in the
group". Redis guarantees each message is delivered to exactly one consumer
within the group. Concurrent workers on the same stream would receive different
messages — consumer group exclusivity is enforced by Redis, not application
logic.

**Messages are never auto-deleted.** The stream grows until explicitly trimmed.
`XTRIM votes:{pollId} MINID {lastProcessedId}` deletes only messages before
the given ID and only after they have been ACKed. Unprocessed messages in the
Pending Entries List (PEL) are never trimmed by MINID.

**MAXLEN ~ on XADD** applies a soft cap to prevent unbounded growth during
worker outages. The `~` makes it approximate (Redis only trims at radix tree
node boundaries for efficiency). This is a safety cap, not a durability
guarantee — the worker's XTRIM after ACK is the normal cleanup path.

---

### 4.4 Aggregator Worker Pool

**Role:** Drain Redis Streams every 100ms, write to MongoDB and Redis counters,
broadcast results to API instances.

**Self-assignment — the problem:**

With multiple workers and multiple poll streams, you need exactly one worker
per stream without a central coordinator that itself becomes a single point of
failure.

**Self-assignment — the solution (Redis distributed lock):**

```
SET lock:stream:{pollId} {workerId} NX PX 10000
```

- `NX` — only set if the key does not exist (atomic)
- `PX 10000` — auto-expire after 10 seconds

Every worker runs a discovery loop every 5 seconds:
1. `SCAN` for all `votes:*` keys (active poll streams)
2. For each stream, attempt `SET NX`
3. On success, spawn a `drainLoop` goroutine for that stream
4. On failure (nil return), skip — another worker owns it

A keepalive goroutine runs every 5 seconds per owned stream:
- `EXPIRE lock:stream:{pollId} 10` — refreshes the TTL
- If the key no longer exists (expired because the worker was paused too long),
  stop the drainLoop and release the stream

**Why not a coordinator process?** A coordinator is a single point of failure
and adds deployment complexity. Redis `SET NX` provides the same exclusivity
guarantee with no additional infrastructure.

**Load balancing:** Workers race on every poll. With random processing delays
and scan ordering, polls distribute across workers roughly evenly in practice.
For more deterministic balancing, consistent hashing (`hash(pollId) % N`) can
be applied before the lock attempt, so each worker only attempts to lock its
designated subset.

**Worker scalability:** If a single worker becomes a bottleneck, add more worker
pods. Each new pod joins the discovery loop and will pick up unlocked streams.
Existing polls are not disrupted — their locks remain held by current workers
until they die or relinquish.

**Drain strategy — `XREADGROUP BLOCK 100ms`:**

The worker does not poll in a tight loop. `BLOCK 100ms` means:

- If messages are waiting: Redis returns immediately with up to 10,000 messages
- If no messages: Redis holds the connection open and returns after 100ms

This is a single round-trip per cycle regardless of traffic. Under high load,
the worker returns quickly with a large batch. Under low load, it waits the
full 100ms without hammering Redis with empty reads.

---

### 4.5 MongoDB — Vote Records

**Role:** Durable, auditable storage of every individual vote.

**Document schema:**

```json
// Poll counters (read rarely — live counters live in Redis)
{
  "_id": "poll:001",
  "question": "What is the biggest challenge in scaling real-time apps?",
  "options": {
    "A": { "label": "Connection management", "votes": 4821 },
    "B": { "label": "State synchronization",  "votes": 3190 }
  },
  "total": 8011
}

// Vote record (one per vote, append-only)
{
  "_id": ObjectId,
  "pollId":   "poll:001",
  "email":    "voter@example.com",
  "optionId": "A",
  "votedAt":  "2026-05-24T10:00:00.123Z"
}
// Index: { pollId: 1, email: 1 }  unique: true
```

The unique index on `{ pollId, email }` is the final hard guarantee against
duplicate votes. Even if the Redis SADD gate and the worker's in-batch dedup
both fail (two messages for the same email reach the worker in separate batches
after a crash/replay scenario), MongoDB will reject the second insert silently
when using `ordered: false` in `bulkWrite`.

**Why MongoDB over alternatives:**

- **PostgreSQL** — equally valid. `ON CONFLICT DO NOTHING` provides the same
  dedup guarantee. Better for complex queries on vote data. Becomes a write
  bottleneck before Mongo at extreme scale without PgBouncer.
- **Cassandra** — better for pure append workloads at very large scale. Counter
  tables handle atomic increments. Operationally heavier; worth considering
  above ~100M votes/day.
- **Redis only** — feasible for the counter store. Without MongoDB, there is no
  durable audit trail. Counter data in Redis is lost on a full cluster failure
  without RDB/AOF persistence configured carefully.

---

### 4.6 SSE Fan-out via easy-sse.go

**Role:** Maintain open SSE connections to browser clients; receive broadcast
payloads from the worker and forward to all local subscribers.

**The fan-out problem:**

API instances are stateful with respect to SSE connections — each instance
holds a different set of connected clients. The worker cannot send one message
and expect all clients to receive it. It must reach every instance.

**Solution:** The worker maintains a list of live API instance addresses (from
the service registry) and fires one `POST /channels/{pollId}/broadcast` per
instance in parallel goroutines with a 200ms timeout. `BroadcastHandler` inside
`easy-sse.go` fans out to all clients subscribed to that poll channel.

**Why HTTP POST instead of Redis pub/sub for fan-out:**

`easy-sse.go` manages its own in-process channel registry. The worker has no
access to goroutines or channels inside each API instance. `BroadcastHandler`
is the package's own fan-out primitive and the correct integration point.
Adding Redis pub/sub just to bridge to `BroadcastHandler` would add a
round-trip with no benefit.

**What if a broadcast POST fails or times out:**

The vote is already committed to MongoDB and the Redis counter is already
incremented. The failed broadcast means one SSE tick is missed for clients on
that instance. They will receive the correct totals on the next 100ms cycle.
Vote durability is not affected.

---

## 5. Key Design Decisions

### 5.1 SSE over WebSockets

| Concern | SSE | WebSocket |
|---|---|---|
| Protocol | HTTP/1.1 or HTTP/2 | TCP upgrade |
| Direction needed | Server → Client only | Bidirectional (unused here) |
| Auto-reconnect | Built into EventSource | Manual |
| CDN/proxy support | Transparent | Requires WS-aware infra |
| HTTP/2 multiplexing | Yes — many streams, one TCP conn | No — one TCP per client |
| Load balancer | Standard L7 | Sticky sessions or socket broker |

Since votes are submitted as plain HTTP POST and results flow server-to-client
only, the bidirectional capability of WebSockets is never used. SSE is the
correct tool.

---

### 5.2 Two-Tier Vote Accumulation

**Problem:** At high vote rates, writing every vote immediately to MongoDB or
even Redis Streams would produce enormous I/O pressure.

**Solution:** Two accumulation windows in series.

```
Individual votes
      │
 API local buffer (~20ms)
      │
 1 bulk stream message per instance per 20ms
      │
 Redis Stream (all instances aggregate here)
      │
 Worker drain window (100ms, BLOCK)
      │
 1 MongoDB bulkWrite + 1 Redis HINCRBY per option
 per 100ms per poll
```

The 20ms window is intentionally shorter than the 100ms window. This ensures
the worker always has a meaningful batch to process rather than racing with the
API flush cycle.

The two windows are independently tunable. Under very high load, increase the
20ms window to reduce stream message volume. Under low load, decrease the 100ms
window to improve perceived update latency.

---

### 5.3 Voter Identity and Deduplication Chain

**Problem:** Each email address may vote only once per poll. This must hold
even under concurrent requests, multiple API instances, and crash/replay
scenarios.

**Three-layer chain:**

```
Layer 1 — Redis SADD (API instance, synchronous, per request)
  Atomic. Closes the concurrent-request race window.
  Operates in microseconds.

Layer 2 — Worker in-batch dedup (aggregator, per drain cycle)
  Catches the case where two API instances both saw the same email
  in the same 100ms window before SADD propagated.
  (Rare but possible during Redis replication lag.)

Layer 3 — MongoDB unique index { pollId, email }
  Hard guarantee at the storage layer.
  Survives all upstream failures.
  insertMany with ordered:false skips duplicates silently.
```

No single layer alone is sufficient. Layer 1 covers the common case efficiently.
Layer 3 covers all edge cases definitively.

---

### 5.4 One Stream Per Poll

**Problem:** Putting all votes for all polls on a single stream creates a
bottleneck. The worker processing one heavy poll would delay processing for all
other polls.

**Solution:** One Redis Stream per poll (`votes:{pollId}`). Workers own
independent streams. A slow poll does not affect other polls. Worker pods can
be added and new polls are automatically distributed across them via the lock
race.

---

### 5.5 Worker Self-Assignment via Redis Locks

**Problem:** With M workers and N poll streams, you need exactly one worker per
stream without a central coordinator.

**Solution:** `SET lock:stream:{pollId} {workerId} NX PX 10000`

- `NX` makes the assignment atomic — only one worker can win per stream
- `PX 10000` auto-expires the lock if the worker dies without releasing it
- The keepalive goroutine refreshes every 5s, well within the 10s TTL
- On graceful shutdown, the worker calls `DEL` on its locks to allow immediate
  takeover instead of waiting for TTL expiry
- `XAUTOCLAIM` recovers any messages that were in-flight when the previous
  worker died

---

### 5.6 XACK Before XTRIM — No Message Loss

**Problem:** If the stream automatically deleted messages, a worker crash after
receiving but before processing a message would permanently lose those votes.

**Solution:** Messages are never deleted until explicitly trimmed after ACK.

```
XREADGROUP delivers message → message enters PEL (pending)
  │
  write to MongoDB + Redis counters
  │
XACK → message removed from PEL
XTRIM MINID {lastId} → message deleted from stream
```

If the worker crashes between delivery and ACK, the message stays in the PEL.
The next worker to claim the stream calls `XAUTOCLAIM` with a 15-second idle
threshold and receives all orphaned messages for reprocessing. Because the
MongoDB write is idempotent (unique index skips duplicates) and `HINCRBY` was
not called before the crash, reprocessing is safe.

---

### 5.7 XAUTOCLAIM — Orphan Recovery

**Problem:** After a worker dies, messages it received but did not ACK sit in
the PEL indefinitely. The new worker taking over the stream must recover them.

**Solution:**

```
XAUTOCLAIM votes:{pollId} workers {newWorkerId} 15000 0-0 COUNT 100
```

- `15000` — claim messages idle for more than 15 seconds
- `0-0` — scan from the beginning of the PEL
- Run on startup of the drainLoop goroutine after acquiring the stream lock

The 15-second threshold is well above the normal processing time (~100ms drain
+ DB write time). This prevents stealing messages from a live but momentarily
slow worker.

---

### 5.8 Redis for Live Counters, MongoDB for Records

**Problem:** Serving the SSE payload requires knowing current vote totals.
Reading MongoDB on every 100ms broadcast cycle would add unnecessary latency
and load.

**Solution:** The worker increments Redis hashes atomically after every batch
(`HINCRBY`), then reads current totals with `HGETALL` to build the SSE payload.
MongoDB is only written to (never read) in the hot path.

If Redis is lost, counters are rebuilt from MongoDB on startup:

```
db.votes.aggregate([
  { $match: { pollId: "poll:001" } },
  { $group: { _id: "$optionId", count: { $sum: 1 } } }
])
```

---

### 5.9 Worker Broadcasts via HTTP POST

**Problem:** SSE connections are maintained inside each API instance's process.
The worker cannot reach those connections directly.

**Solution:** The worker fires one HTTP POST to `/channels/{pollId}/broadcast`
on each API instance. `easy-sse.go`'s `BroadcastHandler` receives the payload
and fans it out to all locally connected SSE clients for that poll.

The worker discovers instance addresses from the service registry (Kubernetes
headless service DNS, Consul, or a configured list). Each POST is sent in a
separate goroutine with a 200ms timeout. A failed POST is logged and skipped —
it causes one missed SSE tick, not a lost vote.

---

## 6. Failure Scenarios and Mitigations

| Scenario | Effect | Mitigation |
|---|---|---|
| API instance crashes after SADD, before XADD | Vote lost, email marked used (~20ms window) | Accepted trade-off. Alternative (SADD after commit) has wider race window. |
| API instance crashes with votes in buffer | Up to 20ms of buffered votes lost | Acceptable for a poll. Reduce with smaller flush window if needed. |
| Redis Stream message delivered but worker crashes before XACK | Message stays in PEL | XAUTOCLAIM on next worker startup redelivers it. |
| MongoDB write fails on batch | Worker does not XACK. Messages stay in PEL and are reprocessed. | Retry logic in worker. Idempotent insert (unique index) makes replay safe. |
| Redis counter hash lost (restart without persistence) | Live counts zeroed | Rebuild from MongoDB on startup via aggregation query. |
| Worker dies holding stream lock | Lock expires after 10s | Standby worker acquires lock on next SCAN cycle. XAUTOCLAIM recovers orphans. |
| Broadcast POST to API instance fails | One SSE tick missed for clients on that instance | Vote already committed. Correct totals sent on next 100ms cycle. |
| All workers down | Stream accumulates messages, no SSE updates | Votes buffered in stream. Processing and broadcasts resume when workers come back. No data loss. |

---

## 7. Durability Guarantee Chain

```
User submits vote
  │
  ├─ SADD voted:{pollId} {email}        ← atomic dedup gate
  │    returns 0 → 409, stop
  │    returns 1 → continue
  │
  ├─ 202 Accepted returned to browser
  │
  ├─ vote appended to in-memory buffer   ← 20ms exposure window
  │
  ├─ XADD votes:{pollId} [...]           ← persisted in Redis Stream
  │    message survives worker crashes
  │    message lives in PEL until ACKed
  │
  ├─ XREADGROUP delivers to worker       ← consumer group exclusivity
  │    exactly one worker per message
  │
  ├─ worker dedup pass                   ← second safety layer
  │
  ├─ MongoDB insertMany                  ← durable audit trail
  │    unique index skips duplicates
  │    ordered:false continues on dup
  │
  ├─ Redis HINCRBY                       ← live counter updated
  │
  ├─ XACK + XTRIM                        ← message cleanup
  │    only after both writes succeed
  │
  └─ POST /broadcast to all API instances ← SSE fan-out
       fire-and-forget, 200ms timeout
       missed tick ≠ lost vote
```

---

## 8. Project Structure

```
poll-platform/
│
├── Makefile                              ← top-level: make dev, make build,
│                                           make test, make deploy
│                                           delegates into backend/ and frontend/
├── README.md                             ← this document
├── .env.example                          ← all env vars documented, no real values
│
├── .github/
│   └── workflows/
│       ├── ci.yml                        ← test + lint on PR (Go + frontend)
│       ├── deploy-api.yml                ← build + push api image on merge to main
│       ├── deploy-worker.yml             ← build + push worker image on merge to main
│       └── deploy-frontend.yml           ← build + deploy frontend on merge to main
│
├── backend/                              ← self-contained Go module
│   │                                       go tooling, gopls, and linters
│   │                                       root themselves here, never at repo root
│   ├── go.mod                            ← module poll-platform/backend
│   ├── go.sum
│   │
│   ├── cmd/
│   │   ├── api/
│   │   │   └── main.go                   ← API binary: HTTP server, routes,
│   │   │                                   SSE setup, graceful shutdown
│   │   └── worker/
│   │       └── main.go                   ← Worker binary: discovery loop,
│   │                                       drain loops, graceful shutdown
│   │
│   └── internal/                         ← shared Go code, not importable
│       │                                   outside this module
│       ├── accumulator/
│       │   ├── accumulator.go            ← mutex buffer + 20ms flush goroutine
│       │   └── accumulator_test.go
│       │
│       ├── gate/
│       │   ├── gate.go                   ← Redis SADD/SISMEMBER dedup gate
│       │   └── gate_test.go
│       │
│       ├── stream/
│       │   ├── producer.go               ← XADD (used by api)
│       │   ├── consumer.go               ← XREADGROUP, XACK, XTRIM, XAUTOCLAIM
│       │   │                               (used by worker)
│       │   └── stream_test.go
│       │
│       ├── lock/
│       │   ├── lock.go                   ← SET NX PX acquire/release
│       │   ├── keepalive.go              ← EXPIRE refresh goroutine
│       │   └── lock_test.go
│       │
│       ├── discovery/
│       │   ├── discovery.go              ← SCAN votes:* loop, self-assigns
│       │   │                               streams to this worker via Redis lock
│       │   └── discovery_test.go
│       │
│       ├── counter/
│       │   ├── counter.go                ← HINCRBY, HGETALL live counts
│       │   └── counter_test.go
│       │
│       ├── broadcast/
│       │   ├── broadcast.go              ← parallel HTTP POST to API instances,
│       │   │                               200ms timeout, fire-and-forget
│       │   └── broadcast_test.go
│       │
│       ├── registry/
│       │   ├── registry.go               ← API instance address discovery
│       │   │                               (k8s headless DNS / Consul / env list)
│       │   └── registry_test.go
│       │
│       ├── store/
│       │   ├── votes.go                  ← MongoDB insertMany vote records
│       │   ├── votes_test.go
│       │   ├── polls.go                  ← MongoDB poll documents,
│       │   │                               recount query for Redis rebuild
│       │   └── polls_test.go
│       │
│       └── config/
│           └── config.go                 ← loads + validates all env vars into
│                                           typed struct on startup; both binaries
│                                           import this, fail fast if var missing
│
├── frontend/                             ← self-contained Node module
│   ├── package.json
│   ├── tsconfig.json
│   ├── vite.config.ts
│   ├── .env.example
│   │
│   ├── src/
│   │   ├── main.ts
│   │   ├── App.tsx
│   │   │
│   │   ├── pages/
│   │   │   ├── PollPage.tsx              ← vote submission + live result display
│   │   │   └── ResultsPage.tsx           ← final results after poll closes
│   │   │
│   │   ├── components/
│   │   │   ├── PollOption.tsx            ← option button + animated bar
│   │   │   ├── ResultBar.tsx             ← percentage bar, animates on SSE update
│   │   │   └── ConnectionStatus.tsx      ← SSE connected / reconnecting indicator
│   │   │
│   │   ├── hooks/
│   │   │   ├── usePollResults.ts         ← owns all EventSource lifecycle:
│   │   │   │                               open, message, error, reconnect backoff
│   │   │   │                               exposes counts + connection state only
│   │   │   └── useVote.ts                ← POST /polls/{pollId}/vote,
│   │   │                                   handles 202 and 409 (already voted)
│   │   │
│   │   ├── lib/
│   │   │   └── api.ts                    ← base URL config, typed fetch wrappers
│   │   │
│   │   └── types/
│   │       └── poll.ts                   ← Poll, Option, VoteResult types
│   │
│   └── public/
│       └── favicon.ico
│
├── infra/
│   │
│   ├── terraform/                        ← cloud resource provisioning
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   ├── versions.tf
│   │   └── modules/
│   │       ├── gke/                      ← (or eks/) Kubernetes cluster
│   │       ├── redis/                    ← Memorystore / ElastiCache
│   │       ├── mongodb/                  ← Atlas / DocumentDB / self-hosted
│   │       └── networking/               ← VPC, subnets, firewall rules
│   │
│   ├── k8s/                              ← Kubernetes manifests
│   │   ├── namespace.yaml
│   │   │
│   │   ├── api/
│   │   │   ├── deployment.yaml           ← image, replicas, env refs,
│   │   │   │                               readiness probe
│   │   │   ├── service.yaml              ← ClusterIP for ingress traffic
│   │   │   ├── hpa.yaml                  ← HorizontalPodAutoscaler
│   │   │   └── headless-service.yaml     ← exposes individual pod IPs so
│   │   │                                   worker registry can discover them
│   │   │
│   │   ├── worker/
│   │   │   ├── deployment.yaml           ← image, replicas (2: active + standby)
│   │   │   └── service.yaml              ← no ingress; worker is outbound only
│   │   │
│   │   └── configmap.yaml                ← non-secret env vars shared by both
│   │                                       binaries (Redis host, Mongo host, etc.)
│   │
│   ├── helm/                             ← wraps k8s/ into a parameterized chart
│   │   └── poll-platform/
│   │       ├── Chart.yaml
│   │       ├── values.yaml               ← defaults (image tags, replicas, limits)
│   │       ├── values.prod.yaml
│   │       ├── values.staging.yaml
│   │       └── templates/                ← same resources as k8s/ but templated
│   │
│   └── docker/
│       ├── api.Dockerfile                ← multi-stage: build Go binary (context:
│       │                                   ./backend) → minimal runtime image
│       ├── worker.Dockerfile             ← same pattern (context: ./backend)
│       └── docker-compose.yml            ← local dev: api + worker + redis +
│                                           mongodb + mongo-express + frontend dev
│                                           server. only file that sees full repo.
│
└── scripts/
    ├── seed.go                           ← creates test polls in MongoDB for
    │                                       local dev and integration tests
    ├── load-test.sh                      ← k6 script: N concurrent voters
    │                                       across M polls simultaneously
    └── recount.go                        ← rebuilds Redis counters from MongoDB
                                            vote records; run after Redis data loss
```

**Binaries:**

- `api` — many replicas, stateless except for open SSE connections
- `worker` — many replicas, each owning a subset of poll streams via Redis locks

**Why `backend/` is isolated from the repo root:**

Go tooling (`go build`, `go test`, `gopls`) roots itself at `go.mod`. If
`go.mod` lives at the repo root, every Go tool call must be aware of
`frontend/` and `infra/` even though they are irrelevant to Go. Placing the
Go module inside `backend/` gives each part of the repo a clean boundary:
`cd backend && go test ./...` never touches Node; `cd frontend && npm test`
never touches Go. The root `Makefile` is the only place that coordinates
across directories.

**Deployment notes:**

- API instances expose `/channels/{pollId}` (SSE) and `/polls/{pollId}/vote` (POST)
- Worker instances need no public exposure — they only make outbound HTTP calls
  to API instances and Redis/MongoDB
- `infra/k8s/api/headless-service.yaml` is load-bearing: the worker's registry
  package queries this service's DNS to enumerate live API pod IPs for broadcast
- Configure Redis with AOF persistence (`appendfsync everysec`) to minimize
  counter data loss on a full Redis restart
- Set MongoDB write concern to `w: majority` for vote record durability in a
  replica set
