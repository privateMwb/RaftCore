# Google Benchmark Suite

This document describes the benchmark categories under `suite/` — what each
one measures, and the individual benchmarks it contains. Same categories as
`../custom/suite/`, reimplemented with Google Benchmark instead of the
custom framework.

| Category | Focus |
|---|---|
| [Access](#access) | Read and lookup operations against an already-built `NodeState` or leader state |
| [Core](#core) | The frequently-called state transitions and RPC handlers — voting, appending entries, and the leader processing replies |
| [Lifecycle](#lifecycle) | Object lifetime operations — `NodeState` and `RaftNode` construction, and copy vs. move of the log and message types |
| [Scaling](#scaling) | Cost vs. structural size (peer count, uncommitted log length, entries per RPC), independent of iteration count |
| [Utility](#utility) | `tick()` on the follower and leader paths, and the `RandomSource` implementations underneath |
| [Conventions](#conventions) | Registration, naming, and sizing conventions specific to Google Benchmark |

No comparison baseline exists for RaftCore — there's no reference
implementation to benchmark it against, so unlike a typical Google
Benchmark suite with paired library-vs-reference functions, every benchmark
here is a single `BENCHMARK()` function timing RaftCore alone, against
in-memory fakes with no sockets, disk, or sleeps. This applies uniformly
across the whole suite; it is not specific to any one category.

Iteration count is handled by Google Benchmark itself — each `BENCHMARK()`
runs until `--benchmark_min_time` is satisfied, the Google Benchmark
equivalent of the custom suite's SMALL/MEDIUM/LARGE tiers, without needing
to register separate sizes by hand. The **Scaling** category below measures
something different: how per-operation cost changes as capacity itself
grows or shrinks, independent of iteration count — those benchmarks
register one function per structural size instead, with the size in the
name.

Every benchmark auto-registers via `BENCHMARK(...)` at startup — no suite
list to maintain by hand. Benchmark names double as the filter you'd pass
to `--benchmark_filter`, e.g. `--benchmark_filter=append_entries` runs
everything with "append_entries" in its name. This applies uniformly across
every category below.

---

## Access

Benchmarks read and lookup operations against an already-built
`NodeState` or leader state.

### Benchmarks

| File | What it covers |
|---|---|
| `state_access.cpp` | `NodeState` getters: `state_access_current_term`, `state_access_voted_for_set`, `state_access_voted_for_none`, `state_access_role`, `state_access_commit_index`, `state_access_last_applied`, `state_access_self_id`, `state_access_log_last_index`, `state_access_log_last_term` |
| `leader_access.cpp` | Per-peer replication state on a 5-node leader: `leader_access_leader_state`, `leader_access_next_index_hit`, `leader_access_match_index_hit`, `leader_access_contains_hit`, `leader_access_contains_miss` |

---

## Core

Benchmarks the frequently-called state transitions and RPC handlers —
changing term and role, deciding votes, accepting or rejecting appended
entries, and the leader processing replies.

### Benchmarks

| File | What it covers |
|---|---|
| `node_state_ops.cpp` | `NodeState` transitions: `node_state_ops_observe_stale`, `node_state_ops_observe_newer`, `node_state_ops_grant_vote`, `node_state_ops_start_election`, `node_state_ops_become_leader`, `node_state_ops_step_down` |
| `request_vote.cpp` | `handleRequestVote()` and `handleRequestVoteReply()`: `request_vote_rv_stale`, `request_vote_rv_grant`, `request_vote_rv_deny`, `request_vote_rvr_ignore`, `request_vote_rvr_dup`, and a full timeout-to-Leader round, `request_vote_election_round` |
| `append_entries.cpp` | `handleAppendEntries()` across every branch: `append_entries_ae_stale`, `append_entries_ae_heartbeat`, `append_entries_ae_check`, `append_entries_ae_mismatch`, `append_entries_ae_dup`, `append_entries_ae_append`, `append_entries_ae_conflict` |
| `leader_replication.cpp` | `handleAppendEntriesReply()`: `leader_replication_reply_stale`, `leader_replication_reply_nocommit`, `leader_replication_reply_commit`, `leader_replication_reply_back` |

---

## Lifecycle

Benchmarks object lifetime operations — construction, copy, and move.
`NodeState` holds references, so it's move-constructible but not
assignable, and a live `RaftNode` must never be moved (in-flight callbacks
capture `this`), so `RaftNode` is benchmarked for construction only. The
log and message types are copyable, and copies are real: `replicateTo()`
passes its `AppendEntriesArgs` to the `Transport` by copy once per peer.

### Benchmarks

| File | What it covers |
|---|---|
| `node_state.cpp` | `NodeState` construction and move, as a Follower and as a Leader with per-peer state: `node_state_construct`, `node_state_construct_move`, `node_state_leader_build`, `node_state_leader_move` |
| `raft_node.cpp` | `RaftNode` construction: `raft_node_ctor_no_peers`, `raft_node_ctor_four_peers`, with `raft_node_peers_build` as the peer-list baseline |
| `log_entry.cpp` | Copy vs. move of the data types: `log_entry_copy_0`, `log_entry_copy_256`, `log_entry_move_256`, `log_entry_args_copy_1`/`10`/`100`, `log_entry_args_move_100`, `log_entry_vote_copy` |

---

## Scaling

Benchmarks how per-operation cost changes as a structural size grows — a
separate axis from Google Benchmark's own iteration-count tuning above:
that repeats the same fixed-size operation until timing stabilizes, while
Scaling grows the structure itself (peer count, uncommitted log length,
entries per RPC) and observes the resulting per-call cost.

### Benchmarks

| File | What it covers |
|---|---|
| `peer_scaling.cpp` | Leader and candidate work as the peer count grows, each at 2, 10, and 100 peers: `peer_scaling_lead_N`, `peer_scaling_elect_N`, `peer_scaling_beat_N` |
| `log_scaling.cpp` | The commit scan and the full-log resend as uncommitted entries grow, each at 1, 100, and 1000: `log_scaling_commit_N`, `log_scaling_resend_N` |
| `batch_scaling.cpp` | `handleAppendEntries()` as entries per RPC grow, each at 1, 10, 100, and 1000: `batch_scaling_copy_N` (copy baseline), `batch_scaling_match_N`, `batch_scaling_replace_N` |

---

## Utility

Benchmarks the timers and randomness primitives the node runs on — not
called directly by application code, but underneath nearly every operation
above: `tick()` runs on every node every tick, and `RandomSource` supplies
every election timeout.

### Benchmarks

| File | What it covers |
|---|---|
| `election_timer.cpp` | `tick()` on the follower path: `election_timer_tick_idle`, `election_timer_tick_fire`, `election_timer_tick_beat` |
| `heartbeat_tick.cpp` | `tick()` on the leader path: `heartbeat_tick_lead_idle`, `heartbeat_tick_lead_beat`, `heartbeat_tick_lead_one_in_ten` |
| `random_source.cpp` | `SystemRandomSource` vs. the test doubles: `random_source_sys_ctor`, `random_source_sys_narrow`, `random_source_sys_wide`, `random_source_fake`, `random_source_scripted` |

---

## Conventions

- **Registration** — every case is a `static void filename_method(
  benchmark::State& state)` registered directly below it with
  `BENCHMARK(filename_method)`. No file defines `main()` or calls
  `BENCHMARK_MAIN()`.
- **Naming** — a benchmark is named `filename_method`, which doubles as
  its `--benchmark_filter` pattern, so a single case (or a whole file) can
  be filtered to on its own. Scaling cases end in the size they run at
  (`peer_scaling_lead_100` = 100 peers).
- **No baseline to match** — with no reference implementation for
  RaftCore, every benchmark measures the library alone, so none of this
  suite uses the library-vs-reference pairing a typical Google Benchmark
  suite would.
- **Fakes, not I/O** — setup uses in-memory fakes of `PersistentState`,
  `Storage`, `Transport`, and `RandomSource`, shared through
  `support/raft_fakes.h`. Nothing here models disk, network latency, or
  real timeout jitter.
- **Steady state** — each loop runs many times, so every case is built to
  do the same work on every iteration (for example, alternating
  conflicting batches so a log stays at a fixed length).
- **Baselines** — a few cases contain a neighbouring case's cost to
  subtract out: `node_state_construct_move` − `node_state_construct`,
  `raft_node_ctor_four_peers` − `raft_node_peers_build`, and
  `batch_scaling_match_N`/`batch_scaling_replace_N` −
  `batch_scaling_copy_N`.
- **Iteration count vs. structural size** — a plain `BENCHMARK(name)` lets
  Google Benchmark pick its own iteration count via
  `--benchmark_min_time`; the Scaling category instead registers one
  function per structural size, since the axis under test is the size, not
  how many times the loop runs.
- **Preventing elision** — every result that matters is wrapped in
  `benchmark::DoNotOptimize(...)` so it can't be optimized away.
