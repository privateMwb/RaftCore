# Benchmark Suite

This document describes the benchmark categories under `benchmarks/custom/suite/`
— what each one measures, and the individual benchmarks it contains.

| Category | Focus |
|---|---|
| [Access](#access) | Read and lookup operations against an already-built node state or leader state |
| [Core](#core) | Frequently-called state transitions and RPC handlers — voting, appending, and replication |
| [Lifecycle](#lifecycle) | Construction, copy, and move of `NodeState`, `RaftNode`, and the log/message value types |
| [Scaling](#scaling) | Per-operation cost as peer count, log size, or batch size grows |
| [Utility](#utility) | Timer ticks and the `RandomSource` implementations underneath the node |
| [Conventions](#conventions) | Naming pattern, shared fakes, and measurement gotchas specific to this suite |

This suite runs on the repo's custom `<support/framework.h>` harness
(`BENCH_SOLO()` + `REGISTER_BENCH_SUITE()`). There's no natural "std"
equivalent to benchmark RaftCore against — no standard-library consensus
implementation exists to pair each benchmark with — so every benchmark
times RaftCore alone. RaftCore is tick-driven and single-threaded, and
everything it touches sits behind `Storage`, `PersistentState`,
`Transport`, and `RandomSource`, so every benchmark here runs against
in-memory fakes with no sockets, disk, or sleeps — see
[Conventions](#conventions) for what those fakes do and don't model.

---

## Access

Benchmarks read and lookup operations against an already-built node
state or leader state — the cost of reading something that's already
there, not the cost of changing it.

### Benchmarks

| File | What it covers |
|---|---|
| `state_access.cpp` | `NodeState::currentTerm()`, `votedFor()` (set and unset), `role()`, `commitIndex()`, `lastApplied()`, `selfId()`, and `log().lastIndex()`/`lastTerm()` |
| `leader_access.cpp` | `leaderState()`, `nextIndex[peer]` and `matchIndex[peer]` lookups, and `matchIndex.contains()` hit and miss, on a 5-node leader |

---

## Core

Benchmarks the fundamental, most frequently exercised state transitions
and RPC handlers — changing term and role, deciding votes, accepting or
rejecting appended entries, and a leader processing replies.

### Benchmarks

| File | What it covers |
|---|---|
| `node_state_ops.cpp` | `NodeState::observeTerm()` (stale and newer), `grantVoteTo()`, `startElection()`, `becomeLeader()`, `stepDownToFollower()` |
| `request_vote.cpp` | `handleRequestVote()` (stale, granted, denied), `handleRequestVoteReply()` (ignored, duplicate), and a full election round from timeout to Leader |
| `append_entries.cpp` | `handleAppendEntries()` for a stale term, heartbeat, prev-entry check (pass and mismatch), retried entries, a new append, and a conflicting entry (truncate then append) |
| `leader_replication.cpp` | `handleAppendEntriesReply()` for a stale reply, a success with no majority, a success that commits, and a failure (back-off and resend) |

---

## Lifecycle

Benchmarks object lifetime operations — construction, copy, and move —
across the node and message types. `NodeState` holds references, so it
is move-constructible but not assignable; a live `RaftNode` must never
be moved, so it is benchmarked for construction only. `LogEntry` and
the RPC arg types are copyable, so their suite contrasts copy against
move directly — copies are real here, since `replicateTo()` hands its
`AppendEntriesArgs` to the `Transport` by copy once per peer.

### Benchmarks

| File | What it covers |
|---|---|
| `node_state.cpp` | `NodeState` construction, construction + move, and the same for a Leader carrying per-peer state |
| `raft_node.cpp` | `RaftNode` construction with 0 and 4 peers, plus the 4-peer list build alone as a baseline |
| `log_entry.cpp` | `LogEntry` copy (0 and 256 bytes) and move, `AppendEntriesArgs` copy at 1, 10, and 100 entries and move at 100, and `RequestVoteArgs` copy |

---

## Scaling

Benchmarks how per-operation cost changes as a structural size grows —
peer count, uncommitted log length, or entries per RPC — as opposed to
Access/Core's fixed-size snapshots. Contrasts work that should stay flat
against loops that grow with the input: the per-peer fan-out on
elections and heartbeats, `tryAdvanceCommitIndex()`'s scan of every
uncommitted entry, and the full-log resend after a failed reply.

### Benchmarks

| File | What it covers |
|---|---|
| `peer_scaling.cpp` | `becomeLeader()`, election start, and leader heartbeat fan-out at 2, 10, and 100 peers |
| `log_scaling.cpp` | The commit-index scan and the full-log resend at 1, 100, and 1000 uncommitted entries |
| `batch_scaling.cpp` | `handleAppendEntries()` copy baseline, retried (match) batch, and wholesale replace at 1, 10, 100, and 1000 entries per RPC |

---

## Utility

Benchmarks the timers and randomness primitives that the node runs on —
not called directly by application code, but underneath nearly every
operation above: `tick()` runs on every node every tick, and
`RandomSource` supplies every election timeout.

### Benchmarks

| File | What it covers |
|---|---|
| `election_timer.cpp` | `RaftNode::tick()` on the follower path: idle, a tick that fires an election, and a healthy follower's tick + heartbeat round |
| `heartbeat_tick.cpp` | `RaftNode::tick()` on the leader path: idle, heartbeat every tick, and heartbeat every 10th tick |
| `random_source.cpp` | `SystemRandomSource` construction and `nextInt()` (narrow and wide range) vs `FakeRandom` and a scripted source |

---

## Conventions

- **Harness**: every file includes `<support/framework.h>` and
  `<support/reference.h>`. Each case is a `static void bench_*()` with
  one `BENCH_SOLO("name", lambda)`; a `run_benchmarks()` driver calls
  them, followed by `REGISTER_BENCH_SUITE()`.
- **Short names**: a trailing number is the size (`"lead 100"` = 100
  peers). `obs` = observeTerm, `rv`/`rvr` = RequestVote request/reply,
  `ae` = AppendEntries, `cp`/`mv` = copy/move, `ctor` = construction,
  `lead` = Leader, `beat` = heartbeat.
- **Fakes**: `reference.h` provides in-memory `PersistentState`,
  `Storage`, `Transport`, and `RandomSource`, plus a `Rig` that wires up
  a node (default: 5-node cluster, term 7). Storage keeps only entry
  terms, the transport drops reply callbacks (replies are fed in by
  hand), and randomness returns the lower bound. Nothing models disk,
  network, or timeout jitter.
- **Steady state**: each lambda repeats many times, so every case does
  the same work each iteration — e.g. alternating conflicting batches to
  keep the log at a fixed length, or `nextIndex` pinned at 1 so a
  failure reply always resends the whole log.
- **Baselines**: some cases contain a neighbouring case's cost to
  subtract out — `"ctor+move"` − `"construct"`, `"lead+move"` −
  `"lead ctor"`, `"ctor 4 peer"` − `"peers 4"`, and `"match N"`/
  `"replace N"` − `"copy N"`.
- **Args by value**: `handleAppendEntries()` takes its args by value, so
  every `ae *` and batch case pays for building or copying them inside
  the timed call.
- **Containers**: fakes use `VectorPro::Vector`, not `std::vector`.
- **Not covered**: there's no `propose()` path or `StateMachine` apply
  yet, so end-to-end throughput isn't benchmarked.
