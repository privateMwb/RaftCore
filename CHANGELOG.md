# Changelog

All notable changes to RaftCore are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `RaftNode::propose()`: appends a client command on the Leader and
  replicates it immediately instead of waiting for the next heartbeat.
- `RaftNode::setStateMachine()` / `RaftNode::applyCommitted()`: committed
  entries are now drained, in log order, into the bound `StateMachine`
  after every commit advance. Election no-op entries are consumed without
  being delivered.
- `Status::NOT_LEADER` and `Status::INVALID_ARGUMENT`.
- Test fake `RecordingStateMachine` and tests for the client surface.

### Fixed
- A single-node cluster (no peers) never became Leader: with no
  RequestVote replies to count, nothing ever called the vote tally. A node
  with an empty peer list now wins its election immediately, and its
  commit index advances without peer acks.

### Changed
- `StateMachine` documentation no longer promises exactly-once delivery
  across a restart: `lastApplied` is volatile, so a restarted node replays
  from index 1 and its `StateMachine` must start empty.

## [1.0.0] - 2026-10-03

The first stable release of RaftCore, a transport-agnostic C++23
implementation of the Raft consensus algorithm's core: leader election and
log replication.

### Added
- `RaftNode`, the consensus driver: `tick()`, `handleRequestVote()`,
  `handleRequestVoteReply()`, `handleAppendEntries()`, and
  `handleAppendEntriesReply()`.
- Leader election with per-term vote tracking, randomized election timeouts,
  and step-down to follower on any higher term observed.
- Log replication with prev-log consistency checks, conflict truncation,
  duplicate-entry skipping, and per-peer `nextIndex`/`matchIndex` tracking.
- Commit-index advancement by majority `matchIndex`, restricted to entries
  from the leader's current term.
- Tick-driven timing for election timeouts and leader heartbeats; no wall
  clock, so behavior is fully deterministic under test.
- `NodeState`, holding the role, `commitIndex`, `lastApplied`, and
  leader-only `nextIndex`/`matchIndex` state, which exists only while the
  node is Leader.
- Pluggable interfaces: `Transport`, `Storage`, `PersistentState`,
  `RandomSource`, and `StateMachine`.
- `SystemRandomSource`, a `std::mt19937`-backed default `RandomSource`.
- `[[nodiscard]] Status` on every fallible call (`OK`, `IO_ERROR`,
  `NOT_FOUND`, `OUT_OF_MEMORY`, `PARSE_ERROR`), so storage and transport
  failures propagate instead of being swallowed.
- `takeLastAsyncError()` for failures raised inside reply callbacks, which
  can't return a `Status` to the caller.
- Asynchronous reply callbacks via `FunctionPro`'s `MoveOnlyFunction`.
- Built on `VectorPro`, `HashMapPro`, and `FunctionPro`.

### Performance
- Role, term, commit-index, and self-id accessors are single-load reads,
  roughly 1 to 2 ns per call.
- Stale-term `AppendEntries` and `RequestVote` are rejected before touching
  the log.
- Leader-only state is built on `becomeLeader()` and dropped on step-down,
  rather than carried by every node.
- `LogEntry` and `AppendEntriesArgs` are movable, so batches of entries can
  be handed to `Transport` without a deep copy.
- Replication to a peer sends only the entries past that peer's
  `nextIndex`, not the whole log.
- Commit advancement scans downward from the log tail and stops at the
  highest index a majority has replicated.
- Benchmarked at 10K / 100K / 1M iterations across leader and state access,
  `AppendEntries`, `RequestVote`, replication, elections, entry copy/move,
  and peer-count and batch-size scaling. Full results in
  `benchmarks/baselines/v1.0.0/v1.0.0.md`.

### Testing
- Comprehensive test suite (341 tests) covering unit, integration,
  lifecycle, and concurrency/fault scenarios: single-node election,
  multi-node log replication, restart recovery, legal role transitions,
  `Status` propagation, term monotonicity, split votes, partitioned leaders,
  leader crash during replication, and log conflict resolution.
- Two ClusterFuzzLite harnesses that fuzz sequences of operations and check
  invariants after every one:
  - `fuzz_raft_rpc` feeds a single node arbitrary, mostly non-conformant
    RPCs, ticks, and local appends. Checks: terms and commit index never
    decrease, no vote changes or clears within a term, no vote granted to a
    stale term, stale `AppendEntries` leave the log untouched, a successful
    `AppendEntries` never over-reports `matchIndex`, and a Leader always has
    `nextIndex >= 1` and a `matchIndex` slot per peer.
  - `fuzz_raft_cluster` runs a three-node cluster whose bytes control only the
    schedule: ticks, message reordering, drops, partitions, client commands on
    stale Leaders, and crash/restart from persisted state. Checks the Raft
    safety properties: Election Safety, term monotonicity (including across a
    restart), vote stability, Log Matching, State Machine Safety, and commit
    sanity.
- A GoogleTest suite (45 cases) alongside the custom suite, mirroring its
  unit, integration, lifecycle, and concurrency layout, built on shared
  in-memory fakes (`InMemoryStorage`, `InMemoryPersistentState`,
  `ScriptedRandomSource`, `SimulatedTransport`).
- A regression tool that compares benchmark runs against the saved `v1.0.0`
  baseline.
- A runnable examples suite (quickstart, patterns, advanced, integration,
  and misuse cases).
- 89.2% line coverage (264/296 lines) and 90.2% function coverage (37/41
  functions), excluding test infrastructure and `libs/internal/`.

### CI
- Automated builds and tests across GCC and Clang on Linux, MSVC on
  Windows, and AppleClang on macOS, each in Debug and Release
  configurations.
- ASan/UBSan and TSan sanitizer jobs.
- `clang-format` and `clang-tidy` checks.
- Code coverage collection.
- ClusterFuzzLite fuzzing on pull requests (300 s per run, address and
  undefined-behavior sanitizers).
- CodeQL and OpenSSF Scorecard analysis on a weekly schedule.
- Conan and vcpkg packaging with consumer smoke tests.
- Automatic Conan recipe, vcpkg port, and `CMakeLists.txt` version bumps on
  `v*` tags.
- Doxygen API documentation published to GitHub Pages.

[Unreleased]: https://github.com/privateMwb/RaftCore/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/privateMwb/RaftCore/releases/tag/v1.0.0
