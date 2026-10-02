# Examples Suite

This document describes the example categories under `examples/suite/`
— what each one covers, and the individual examples it contains.

| Category | Focus |
|---|---|
| [Quickstart](#quickstart) | The smallest useful program at each step: one node, one election, one replicated entry |
| [Patterns](#patterns) | Idiomatic ways to drive RaftCore: `Status` handling, ticks, message routing, and timeouts |
| [Integration](#integration) | Multiple nodes used together the way a real cluster would: election, failover, and crash recovery |
| [Advanced](#advanced) | Compositions and manual workarounds for gaps RaftCore doesn't cover itself |
| [Misuse](#misuse) | Common mistakes, what RaftCore does instead of what you might expect, and the fix |
| [Conventions](#conventions) | Naming pattern and setup gotchas specific to this suite |

---

## Quickstart

The smallest useful program at each step -- meant to be read top to
bottom before anything else in this suite.

### Examples

| File | What it covers |
|---|---|
| `hello_raft.cpp` | Wire one `RaftNode` to in-memory `PersistentState`/`Storage`/`Transport`/`RandomSource`, tick it to its election timeout, and watch it become a Candidate |
| `basic_election.cpp` | Three nodes with different election timeouts on a queuing `Transport`: one times out, wins a majority, becomes Leader, and its heartbeats keep the followers quiet |
| `basic_replication.cpp` | A Leader appends an entry, followers accept it on the next heartbeat, the commit index advances, and followers learn it one heartbeat later |

---

## Patterns

Idiomatic ways to drive RaftCore -- the recurring shapes worth reaching
for rather than reinventing.

### Examples

| File | What it covers |
|---|---|
| `status_handling.cpp` | Checking `[[nodiscard]] Status` from `tick()` and the handlers, what a failed tick leaves behind, early-return chaining, explicit `(void)` discards, and `takeLastAsyncError()` for the reply handlers that return `void` |
| `tick_driver.cpp` | RaftCore has no clock: choosing a tick length, deriving timeouts in ticks, driving a node in a loop, and a `TickDriver` that converts elapsed milliseconds to ticks with a remainder and a catch-up cap |
| `message_routing.cpp` | A `Transport` that routes RPCs by `NodeId`, queues them, sends replies back through the callback, and returns `NOT_FOUND` for an unknown target; every hop traced |
| `scripted_timeouts.cpp` | A scripted `RandomSource` deciding which node times out first -- the same cluster with a different script elects a different winner |

---

## Integration

Multiple nodes used together the way a real cluster would, rather than
one class in isolation.

### Examples

| File | What it covers |
|---|---|
| `three_node_cluster.cpp` | Election, three replicated entries, and commit across a whole three-node cluster in one session, ending with a check that every log is identical |
| `leader_failover.cpp` | The Leader is cut off, a follower wins the next term, the cluster commits with two of three nodes, and the old Leader rejoins, steps down, and catches up |
| `crash_recovery.cpp` | Splitting a node into what survives a crash (`PersistentState`, `Storage`) and what doesn't (`NodeState`, `RaftNode`); a restart keeps term, vote, and log, relearns the commit index, and still refuses a second vote in the same term |

---

## Advanced

Compositions and manual workarounds for gaps RaftCore doesn't close
itself -- there is no `propose()` and nothing drains committed entries
to a `StateMachine`, so both are built by hand here.

### Examples

| File | What it covers |
|---|---|
| `manual_apply_loop.cpp` | `StateMachine` and a hand-written apply loop: entries `lastApplied + 1` through `commitIndex`, in order, each recorded with `setLastApplied()`, so nothing is applied twice |
| `propose_by_hand.cpp` | Appending to the Leader's `Storage` as a stand-in for `propose()`: refusing on non-Leaders, appended vs committed, waiting for the commit, and a cut-off Leader whose write never commits |
| `network_partition.cpp` | Five nodes split into a minority holding the old Leader and a majority: the minority can't commit, the majority elects a new Leader in a higher term, and after healing the stale Leader's uncommitted entry is discarded |
| `custom_storage.cpp` | The `Storage` contract, and an instrumented backend that counts reads, appends, truncates, and syncs for new, retried, and conflicting `AppendEntries`, plus an `IO_ERROR` surfacing as a `Status` |

---

## Misuse

Common mistakes, what RaftCore actually does instead of what you might
expect, and the fix -- each file is a trap first, then the correction.

### Examples

| File | What it covers |
|---|---|
| `stale_term_ignored.cpp` | A late vote from an older term doesn't count, an old Leader's `AppendEntries` is rejected, and a rejection carrying a higher term is how a stale Leader steps down -- so pass every reply to the handler |
| `never_move_node.cpp` | `RaftNode` is movable, but its in-flight callbacks capture `this`; a `PinnedNode` with deleted copy and move turns the mistake into a compile error |
| `identical_timeouts.cpp` | Nodes with the same election timeout all become Candidates on the same tick and split the vote forever; different timeouts fix it |
| `shared_storage.cpp` | Two nodes sharing one `PersistentState` overwrite each other's term and vote, and two sharing one `Storage` see and delete each other's log; one set per node is the fix |

---

## Conventions

- Every example is a single free function, `run_examples()`, registered
  with `REGISTER_EXAMPLE_SUITE()` at the bottom of the file -- one
  example file, one suite, no shared driver across files.
- `setTitle("...")` marks off each step within a file's console output;
  it's cosmetic only and has no effect on control flow.
- Examples print results with the expected value spelled out in a
  trailing comment (e.g. `// (IO_ERROR)`) rather than asserted -- these
  are narrated demonstrations to read and run, not test cases.
- `(void)` prefixes a call whose `[[nodiscard]] Status` is deliberately
  ignored, consistent with `status_handling.cpp`'s own guidance on when
  that's appropriate.
- Every file includes only `<support/framework.h>` and defines its own
  small in-memory `PersistentState`, `Storage`, `Transport`, and
  `RandomSource`, so each one reads top to bottom on its own.
- Nothing touches disk or a clock: time only moves when an example calls
  `tick()`, and the `Transport` queues RPCs until the example calls
  `deliverAll()`. Runs are deterministic, and there are no scratch files
  to clean up.
- Helper types are named `Member`, `Network`, `NodeRig`, and so on, never
  `Node`: `HashMapPro::Node` is in scope through `RaftCore`, and a bare
  `Node` is ambiguous.
