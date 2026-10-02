# Fuzzing

RaftCore is fuzzed via [ClusterFuzzLite](https://google.github.io/clusterfuzzlite/),
running on every pull request that touches the consensus code or the
libraries under it, plus a longer scheduled batch run every night.

## What's covered

RaftCore has no wire format to parse — messages arrive as typed structs
from whatever `Transport` you plug in — so the harnesses don't fuzz a
byte parser. They fuzz **sequences of operations**: the fuzzer's bytes
choose what happens next, and the harness checks invariants after every
single operation (not just at the end), so a failing input localizes to
the exact call that broke one.

**`fuzz_raft_rpc.cpp`** feeds one node arbitrary RPCs. Requests and
replies with arbitrary terms, indexes, and entry lists are thrown at a
single `RaftNode` alongside ticks and local appends. Most of that input
is not valid Raft — a "leader" claims a `prevLogIndex` the follower
never had, a reply carries a term from long ago — which is the point: a
node has to stay consistent whatever arrives. Because the input isn't
protocol-conformant, it only asserts properties that must hold for
*any* input:

- `currentTerm` and `commitIndex` never decrease.
- `votedFor` never changes, or clears, within a single term.
- A reply carries the node's resulting term; a vote is never granted to
  a stale-term candidate, and a granted vote is recorded as `votedFor`.
- A stale-term `AppendEntries` is rejected without touching the log.
- A successful `AppendEntries` reply never claims a `matchIndex` beyond
  what the RPC itself delivered (`prevLogIndex` plus entry count) —
  over-reporting lets a Leader count a follower toward a majority for
  entries it doesn't hold.
- A Leader always has `nextIndex >= 1` and a `matchIndex` slot for every
  peer.

**`fuzz_raft_cluster.cpp`** runs a whole three-node cluster. The nodes
generate their own RPCs, so every message is protocol-conformant and the
real Raft safety properties must hold whatever the fuzzer does to the
schedule. The bytes control only the environment: which node ticks,
which queued RPC is delivered next (reordering), which is dropped, a
partition toggled for a node, a client command appended wherever a node
believes it's Leader (stale Leaders included), and a node crashing and
restarting from its `PersistentState` and `Storage` alone. After every
operation it checks:

- **Election Safety** — at most one Leader is ever observed per term.
- **Term monotonicity** — a node's term never decreases, even across a
  restart.
- **Vote stability** — a vote cast in a term never changes.
- **Log Matching** — two logs with an entry at the same index and term
  are identical up to that index.
- **State Machine Safety** — every committed entry is the same entry
  (term and payload) on every node, now and in the future.
- **Commit sanity** — `commitIndex <= lastIndex`, and it never decreases
  within one run of a node.

Both run under AddressSanitizer and UndefinedBehaviorSanitizer, which
additionally catch out-of-range log access, use-after-free, and index
arithmetic overflow.

## What's deliberately NOT covered yet

- **Duplicated messages.** Reply callbacks are move-only, so the cluster
  harness can lose, delay, and reorder a message but not deliver it
  twice.
- **Cluster sizes other than three**, and peer lists that disagree
  between nodes.
- **Storage and persistence failures.** The fakes never return an error,
  so the `IO_ERROR` paths (and `takeLastAsyncError()`) are only
  exercised by the examples, not fuzzed. A fault-injecting
  `PersistentState`/`Storage` is the natural follow-up harness.
- **Real backends.** Everything runs against in-memory fakes; a
  file-backed `Storage` has its own bugs this can't see.
- **Membership change and snapshots.** RaftCore implements neither.

## Running locally

```bash
git clone --recursive https://github.com/google/oss-fuzz.git
cd oss-fuzz
python infra/helper.py build_fuzzers --sanitizer address RaftCore /path/to/RaftCore
python infra/helper.py run_fuzzer RaftCore fuzz_raft_cluster
```

Or, without OSS-Fuzz's tooling, directly with clang (RaftCore isn't
header-only, so every source file under `src/RaftCore/` is compiled in):

```bash
clang++ -std=c++20 -fsanitize=fuzzer,address \
  -Iinclude $(find libs -type d -name include -printf '-I%p ') \
  $(find src/RaftCore -name '*.cpp' ! -name main.cpp) \
  fuzz/fuzz_raft_cluster.cpp \
  -o fuzz_raft_cluster

./fuzz_raft_cluster
```

Add `-fsanitize=fuzzer,undefined` instead to run under UBSan, and swap in
`fuzz/fuzz_raft_rpc.cpp` for the other harness.

## Reproducing a crash

ClusterFuzzLite uploads the failing input as a workflow artifact when
a run fails. Download it, then:

```bash
./fuzz_raft_cluster path/to/crash-<hash>
```

This replays that exact byte sequence through
`LLVMFuzzerTestOneInput()` once, deterministically — no sanitizer flags
needed beyond however the binary was already built. An invariant
violation shows up as an `abort()`; the stack trace points at the check
that failed, and bisecting the input (or running with `-minimize_crash=1`)
narrows it to the shortest schedule that does it.

## Adding a new harness

1. Add `fuzz/fuzz_<target>.cpp` with an `extern "C" int
   LLVMFuzzerTestOneInput(const uint8_t*, size_t)` entry point.
2. That's it — `.clusterfuzzlite/build.sh` links every `fuzz/fuzz_*.cpp`
   against the library objects, and `cflite_pr.yml`/`cflite_batch.yml`
   build and run every binary it produces in `$OUT`.
