# Google Test Suite

This document describes the test categories in the GoogleTest suite
— what each one covers, and the individual tests it contains.

| Category | Focus |
|---|---|
| [Unit](#unit) | A single class/responsibility in isolation: NodeState, RaftNode's propose/apply surface |
| [Lifecycle](#lifecycle) | Cross-cutting contracts every path must honor: term/commitIndex monotonicity, Status propagation, leaderState reset guarantees |
| [Integration](#integration) | Multiple pieces exercised together the way a real deployment's event loop would: election, steady-state replication, client writes, a single-node cluster, restart recovery |
| [Concurrency](#concurrency) | Multiple independent nodes acting at once: split votes, partitions, a leader crashing mid-replication, log conflict resolution |
| [Regression](#regression) | One test file per fixed bug, pinning the specific behavior that broke |

---

## Unit

Verifies a single class or responsibility in isolation — direct calls
into NodeState rather than a multi-node simulation.

### Tests

| File | What it covers |
|---|---|
| `node_state.cpp` | Role/term/vote transitions (`startElection`, `observeTerm`, `grantVoteTo`), `becomeLeader()` initializing `nextIndex`/`matchIndex` per peer, commitIndex/lastApplied setters, `log()` aliasing the same Storage |
| `propose_apply.cpp` | `propose()` rejecting a non-Leader (`NOT_LEADER`) and an empty payload (`INVALID_ARGUMENT`), `applyCommitted()` as a no-op with no StateMachine bound, in-order delivery that skips the election no-op and never re-delivers, and a late-bound StateMachine catching up |

---

## Lifecycle

Verifies contracts that apply across more than one method or call path,
rather than one function's specific behavior.

### Tests

| File | What it covers |
|---|---|
| `term_monotonicity.cpp` | currentTerm ignoring a stale or equal term and only rising through repeated elections; commitIndex on a Leader never regressing from a delayed, out-of-order reply |
| `status_propagation.cpp` | A PersistentState failure blocking an election outright; a Storage failure propagating out of AppendEntries reconciliation; the one case where failure doesn't block a transition (`becomeLeader()` itself can't fail) |
| `role_transition_rules.cpp` | `becomeLeader()` always producing fresh nextIndex/matchIndex, regardless of whether a new election, a `stepDownToFollower`, or an `observeTerm` sits before it |

---

## Integration

Verifies multiple pieces of RaftCore used together the way a real
deployment's event loop would, driven only by `tick()`/`pump()`.

### Tests

| File | What it covers |
|---|---|
| `single_node_election.cpp` | A full election round trip: role transitions, the self-vote, and the no-op entry reaching both followers |
| `log_replication.cpp` | The no-op entry committing once a majority acks it, three client commands batched into one AppendEntries, and commitIndex covering all of them |
| `propose_replication.cpp` | `propose()` reaching both followers in one `pump()` with the heartbeat interval far away, the Leader applying right after the majority ack, and followers applying only after a later heartbeat carries the new `leaderCommit` |
| `single_node_cluster.cpp` | A peerless node electing itself and committing without any ack, `propose()` committing and applying synchronously, and a restarted node re-electing itself and replaying every committed command from index 1 |
| `restart_recovery.cpp` | currentTerm/votedFor/the log surviving a simulated restart over the same Storage/PersistentState, role correctly not persisting, and the recovered vote being honored |

---

## Concurrency

Verifies RaftCore's actual concurrency model — multiple independent
simulated nodes, not threads — acting at once or at cross purposes.

### Tests

| File | What it covers |
|---|---|
| `split_vote.cpp` | Two candidates deadlocked in the same term on a 4-node cluster, engineered via dropped/reordered RequestVotes, then resolved once one candidate's timeout fires again first |
| `partitioned_leader.cpp` | A Leader cut off from the majority while the majority elects a new one for a higher term; the old Leader stepping down once a higher-term RPC reaches it post-heal |
| `leader_crash_replication.cpp` | A crash after replicating to only one of two followers; the election restriction blocking the lagging follower's own bid; the eventual winner's nextIndex back-off reconciling it |
| `log_conflict_resolution.cpp` | `handleAppendEntries` truncating a stale conflicting entry and adopting the leader's, and leaving already-matching entries alone |

---

## Regression

One file per fixed bug. Each pins the specific behavior that broke,
named after the bug rather than the feature. Empty for now — nothing
has broken against a real build yet that a Concurrency/Integration/Unit
file didn't already cover by itself.

### Tests

| File | What it covers |
|---|---|
| — | *(none yet)* |

---

## Conventions

- Every test is a GoogleTest `TEST(SuiteName, TestName)` — one test file,
  one suite, named after the file's title without its category (e.g.
  `NodeStateTest`, `SplitVoteTest`). Suite and test names are CamelCase.
  No manual registration: GoogleTest discovers every `TEST` on its own.
- Each file includes `<gtest/gtest.h>` ahead of `<support/framework.h>`,
  and its header comment's first line ends with `(GoogleTest)`.
- Checks use `EXPECT_*` for the outcome under test and `ASSERT_*` for
  setup and any step a later step depends on (a precondition, an
  `Optional` that must hold a value before `.value()` is read, a role
  that must be reached before the scenario can continue). Comparisons use
  `EXPECT_EQ(actual, expected)` / `EXPECT_GT(...)`, booleans use
  `EXPECT_TRUE` / `EXPECT_FALSE`.
- `reference.h` (pulled in transitively via `<support/framework.h>`)
  holds the fakes every file builds on: `InMemoryStorage`,
  `InMemoryPersistentState`, `ScriptedRandomSource`, `SimulatedTransport`,
  and the universally-failing `FailingStorage`/`FailingPersistentState`
  used by the Lifecycle suite's propagation tests. File-specific helpers
  (e.g. a `registerCluster()` wiring one test's nodes into a transport)
  still live in an anonymous namespace in that file alone, not shared.
- `ScriptedRandomSource` returns a pre-scripted sequence instead of an
  actual random value. Each `resetElectionTimer()` call — on
  construction, on adopting a newer term, on granting a vote, and on
  receiving any valid AppendEntries — consumes exactly one scripted
  value, in that order. Miscounting how many of these happen before the
  point a test cares about silently lands the wrong value on the wrong
  call; it doesn't fail to compile or obviously misbehave, it just
  changes the test's timing. Get the count right, or pad early entries
  generously and let the sequence run out — an exhausted
  `ScriptedRandomSource` always falls back to the configured minimum.
