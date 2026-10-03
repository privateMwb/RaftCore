<p align="center">
  <img src=".github/assets/banner.svg" alt="RaftCore" width="100%">
</p>

<p align="center">
  <img src="https://img.shields.io/github/v/release/privateMwb/RaftCore?style=for-the-badge&logo=github&color=10B981&labelColor=07140F" alt="Version">
  <img src="https://img.shields.io/badge/License-MIT-F59E0B?style=for-the-badge&labelColor=07140F" alt="License - MIT">
  <img src="https://img.shields.io/badge/C%2B%2B-23-82AB46?style=for-the-badge&labelColor=07140F" alt="C++ - 23">
</p>

<p align="center">
  <img src=".github/assets/divider.svg" alt="" width="100%">
</p>

<p align="center"><sub><b>CI / CD</b></sub></p>
<p align="center">
  <a href="https://github.com/privateMwb/RaftCore/actions/workflows/build.yml">
    <img src="https://github.com/privateMwb/RaftCore/actions/workflows/build.yml/badge.svg" alt="Build and Test">
  </a>
  <a href="https://github.com/privateMwb/RaftCore/actions/workflows/benchmark.yml">
    <img src="https://github.com/privateMwb/RaftCore/actions/workflows/benchmark.yml/badge.svg" alt="Benchmarks">
  </a>
  <a href="https://github.com/privateMwb/RaftCore/actions/workflows/packaging.yml">
    <img src="https://github.com/privateMwb/RaftCore/actions/workflows/packaging.yml/badge.svg" alt="Packaging">
  </a>
  <a href="https://github.com/privateMwb/RaftCore/actions/workflows/release.yml">
    <img src="https://github.com/privateMwb/RaftCore/actions/workflows/release.yml/badge.svg" alt="Release">
  </a>
</p>

<p align="center"><sub><b>Code Quality &amp; Safety</b></sub></p>
<p align="center">
  <a href="https://github.com/privateMwb/RaftCore/actions/workflows/coverage.yml">
    <img src="https://github.com/privateMwb/RaftCore/actions/workflows/coverage.yml/badge.svg" alt="Coverage">
  </a>
  <a href="https://github.com/privateMwb/RaftCore/actions/workflows/sanitizers.yml">
    <img src="https://github.com/privateMwb/RaftCore/actions/workflows/sanitizers.yml/badge.svg" alt="Sanitizers">
  </a>
  <a href="https://github.com/privateMwb/RaftCore/actions/workflows/clang-tidy.yml">
    <img src="https://github.com/privateMwb/RaftCore/actions/workflows/clang-tidy.yml/badge.svg" alt="Clang Tidy">
  </a>
  <a href="https://github.com/privateMwb/RaftCore/actions/workflows/clang-format.yml">
    <img src="https://github.com/privateMwb/RaftCore/actions/workflows/clang-format.yml/badge.svg" alt="Clang Format">
  </a>
  <a href="https://github.com/privateMwb/RaftCore/actions/workflows/codeql.yml">
    <img src="https://github.com/privateMwb/RaftCore/actions/workflows/codeql.yml/badge.svg" alt="CodeQL">
  </a>
  <a href="https://github.com/privateMwb/RaftCore/actions/workflows/cflite_pr.yml">
    <img src="https://github.com/privateMwb/RaftCore/actions/workflows/cflite_pr.yml/badge.svg" alt="Fuzzing">
  </a>
</p>

<p align="center"><sub><b>Documentation</b></sub></p>
<p align="center">
  <a href="https://github.com/privateMwb/RaftCore/actions/workflows/docs.yml">
    <img src="https://github.com/privateMwb/RaftCore/actions/workflows/docs.yml/badge.svg" alt="Documentation">
  </a>
</p>

<p align="center">
  <img src=".github/assets/divider.svg" alt="" width="100%">
</p>

<p align="center"><sub><b>Compiler Support</b></sub></p>
<p align="center">
  <img src="https://img.shields.io/badge/GCC-support-B46F1B?style=flat&logo=gnu" alt="GCC - support">
  <img src="https://img.shields.io/badge/Clang-support-045891?style=flat&logo=llvm" alt="Clang - support">
  <img src="https://img.shields.io/badge/MSVC-support-5C2D91?style=flat" alt="MSVC - support">
  <img src="https://img.shields.io/badge/AppleClang-support-000000?style=flat&logo=apple" alt="AppleClang - support">
</p>

<p align="center">
  <img src=".github/assets/divider.svg" alt="" width="100%">
</p>

<p align="center">RaftCore is a transport-agnostic C++23 implementation of the Raft consensus algorithm's core — leader election and log replication, with no sockets, threads, clocks, or disk I/O of its own. You plug in the transport, the log storage, the durable term/vote state, and the randomness; RaftCore supplies the protocol, driven entirely by <code>tick()</code> so every run is deterministic — built on this author's own container and callable libraries.</p>

<br>

## 📑 Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Dependencies](#dependencies)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Development](#development)
- [Benchmarks](#benchmarks)
- [Fuzzing](#fuzzing)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [Changelog](#changelog)
- [Security](#security)
- [License](#license)

<br>

## <a id="features"></a>✨ Features

- **Pure consensus core** — no sockets, threads, clocks, or disk I/O. RaftCore talks to four interfaces (`Transport`, `Storage`, `PersistentState`, `RandomSource`) plus an optional `StateMachine`, so it embeds in any runtime and tests without a network.
- **Tick-driven timing** — `RaftNode::tick()` is the only source of time: election timeouts and leader heartbeats advance one call at a time, so a test never sleeps and a run is fully reproducible.
- **Leader election** — `RequestVote` handling with per-term vote tracking, randomized election timeouts, and step-down to Follower on any higher term observed.
- **Log replication** — `AppendEntries` with prev-log consistency checks, conflict truncation, duplicate-entry skipping, and per-peer `nextIndex`/`matchIndex` tracking; the commit index advances by majority `matchIndex`, restricted to entries from the leader's current term.
- **Status-checked, not exception-driven** — every fallible call returns a `[[nodiscard]] Status` (`OK`, `IO_ERROR`, `NOT_FOUND`, `OUT_OF_MEMORY`, `PARSE_ERROR`), and `takeLastAsyncError()` surfaces failures raised inside reply callbacks, which can't return a `Status` to the caller.
- **Move-only reply callbacks** — replies arrive through `MoveOnlyFunction`, so a callback can own move-only state such as a connection or a `unique_ptr`.
- **Leader-only state that only exists as Leader** — `nextIndex`/`matchIndex` live in a `std::optional` that is built on `becomeLeader()` and dropped on step-down, rather than carried by every node.
- **Fuzzed for safety properties** — two ClusterFuzzLite harnesses check term and commit monotonicity, vote stability, and, across a whole three-node cluster, Election Safety, Log Matching, and State Machine Safety.

<div align="right"><a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a></div>

## <a id="requirements"></a>📋 Requirements

- A C++23-conformant compiler (tested: GCC, Clang, MSVC, AppleClang)
- CMake 3.20+
- Git submodules initialized — unlike this author's other, dependency-free libraries, RaftCore is a consumer of 3 of them (see [Dependencies](#dependencies)) and needs their source present to build from source

<div align="right"><a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a></div>

## <a id="dependencies"></a>🔗 Dependencies

RaftCore is built entirely on this author's own libraries, vendored as git submodules under `libs/internal/`:

| Library | Provides | Repository |
|---|---|---|
| VectorPro | `Vector<T>`, used for the peer list, log entry ranges, and command payload bytes | [privateMwb/VectorPro](https://github.com/privateMwb/VectorPro) |
| HashMapPro | `HashMap<K,V>`, backing the leader's `nextIndex`/`matchIndex` and per-election vote tracking | [privateMwb/HashMapPro](https://github.com/privateMwb/HashMapPro) |
| FunctionPro | `MoveOnlyFunction<>`, carrying every `Transport` reply callback | [privateMwb/FunctionPro](https://github.com/privateMwb/FunctionPro) |

<div align="right"><a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a></div>

## <a id="installation"></a>📦 Installation

**From source:**

```bash
git clone --recurse-submodules https://github.com/privateMwb/RaftCore.git
cd RaftCore
cmake -B build \
  -DBUILD_TESTS=OFF \
  -DBUILD_BENCHMARKS=OFF \
  -DBUILD_REGRESSION=OFF \
  -DBUILD_EXAMPLES=OFF
cmake --install build
```

Then, in your own `CMakeLists.txt`:

```cmake
find_package(RaftCore CONFIG REQUIRED)
target_link_libraries(your_target PRIVATE RaftCore::RaftCore)
```

> vcpkg and Conan packages are built and verified (recipe in
> `packaging/recipes/raftcore/`, port in `packaging/vcpkg/ports/raftcore/`),
> but not yet published to the public registries. This section will be
> updated once they are.

<div align="right"><a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a></div>

## <a id="quick-start"></a>🚀 Quick Start

RaftCore ships no networking or storage, so you implement the interfaces it talks to. The smallest in-memory versions live in [`examples/suite/quickstart/hello_raft.cpp`](examples/suite/quickstart/hello_raft.cpp); the wiring looks like this:

```cpp
#include <RaftCore/RaftCore.h>

using namespace RaftCore;

int main() {
    MyPersistentState persistent;   // implements PersistentState: currentTerm, votedFor
    MyStorage         log;          // implements Storage: the replicated log
    MyTransport       transport;    // implements Transport: sends RPCs to peers
    SystemRandomSource random;      // RandomSource: election-timeout jitter

    NodeState state(persistent, log, "node-1");

    Vector<NodeId> peers;
    peers.push_back(NodeId("node-2"));
    peers.push_back(NodeId("node-3"));

    // Election timeout: 150-300 ticks. Heartbeat: every 50 ticks, once Leader.
    RaftNode raft(state, transport, random, peers, 150, 300, 50);

    // There is no clock: time only advances when you call tick().
    Status s = raft.tick();
    if (s != Status::OK) {
        // storage or transport failed -- the node's state is still consistent
    }
}
```

Incoming RPCs are handed to the node, and replies to your own outgoing RPCs are fed back in:

```cpp
RequestVoteReply reply;
if (raft.handleRequestVote(args, reply) == Status::OK) {
    // send `reply` back to the candidate
}

raft.handleRequestVoteReply(peerId, voteReply);          // reply to a RequestVote you sent
raft.handleAppendEntriesReply(peerId, appendReply);      // reply to an AppendEntries you sent
```

RaftCore has no `propose()` call: a client command enters the cluster by being appended to the Leader's `Storage`, and replication carries it from there:

```cpp
if (state.role() == Role::Leader) {
    LogIndex index;
    Status s = state.log().append(state.currentTerm(), payload, index);
    // `index` commits once a majority has replicated it
}
```

RaftCore reports failure through `Status`, not exceptions — every fallible call is `[[nodiscard]]` and meant to be checked, not wrapped in a `try`/`catch`. Failures inside reply callbacks are collected for you to poll:

```cpp
if (auto err = raft.takeLastAsyncError()) {
    // *err is the Status a reply callback could not return
}
```

<div align="right"><a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a></div>

## <a id="project-structure"></a>🗂️ Project Structure

```
RaftCore/
├── include/
│   └── RaftCore/
│       ├── Common/
│       │   ├── Status.h
│       │   └── Types.h
│       ├── Core/
│       │   ├── NodeState.h
│       │   └── RaftNode.h
│       ├── Interfaces/
│       │   ├── PersistentState.h
│       │   ├── RandomSource.h
│       │   ├── StateMachine.h
│       │   ├── Storage.h
│       │   └── Transport.h
│       └── RaftCore.h
│
├── src/
│   └── RaftCore/
│       └── Core/
│           ├── NodeState.cpp
│           └── RaftNode.cpp
│
├── libs/
│   └── internal/
│       ├── VectorPro/
│       ├── HashMapPro/
│       └── FunctionPro/
│
├── tests/
│   ├── custom/
│   ├── google/
│   ├── CMakeLists.txt
│   └── README.md
│
├── benchmarks/
│   ├── custom/
│   ├── google/
│   ├── baselines/
│   ├── results/
│   ├── CMakeLists.txt
│   └── README.md
│
├── examples/
│   ├── support/
│   ├── suite/
│   ├── example_main.cpp
│   ├── CMakeLists.txt
│   └── README.md
│
├── regression/
│   ├── custom/
│   ├── google/
│   ├── results/
│   ├── CMakeLists.txt
│   └── README.md
│
├── fuzz/
│   ├── fuzz_raft_rpc.cpp
│   └── fuzz_raft_cluster.cpp
│
├── .clusterfuzzlite/
│   ├── Dockerfile
│   ├── build.sh
│   └── project.yaml
│
├── packaging/
│   ├── README.md
│   ├── requirements.in
│   ├── requirements.txt
│   ├── recipes/
│   ├── vcpkg/
│   └── vcpkg-smoke-test/
│
├── scripts/
│   └── update_package_files.py
│
├── .github/
│   ├── releases/
│   └── workflows/
│
├── cmake/
│   └── RaftCoreConfig.cmake.in
│
├── docs/
│   ├── Doxyfile
│   └── README.md
│
├── .clang-format
├── .clang-tidy
├── .gitignore
├── .gitmodules
├── CMakeLists.txt
├── README.md
├── CONTRIBUTING.md
├── CHANGELOG.md
├── SECURITY.md
├── FUZZING.md
└── LICENSE
```

<div align="right"><a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a></div>

## <a id="development"></a>🛠️ Development

The from-source install above builds the library only. To work on
RaftCore itself — running tests, benchmarks, or the regression tool —
build with everything enabled (the default):

```bash
cmake -B build
cmake --build build
```

**Run the test suite:**

```bash
ctest --test-dir build
```

**Run benchmarks and check for regressions:**

```bash
./build/benchmarks
./build/regression                  # latest baseline vs. benchmarks/results/benchmark_results.json
./build/regression v1.2.0           # a specific baseline vs. current
./build/regression v1.2.0 v1.4.0    # two baselines against each other
```

`regression` picks the latest baseline by semantic version (`v1.10.0`
correctly outranks `v1.9.0`), not alphabetical filename order, and
auto-names its output (`regression_v1.2.0_vs_current.md`/`.json`, etc.).

See [packaging/README.md](packaging/README.md) for notes on verifying the vcpkg
port and Conan recipe locally, and [FUZZING.md](FUZZING.md) for running the
fuzz harnesses locally.

<div align="right"><a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a></div>

## <a id="benchmarks"></a>📊 Benchmarks

Unlike this author's other libraries, RaftCore has no natural drop-in
standard-library equivalent to benchmark against (there's no `std::`
consensus library) — these are absolute measurements, not a comparison.
Full results across every subsystem and scale: `benchmarks/baselines/v1.0.0/v1.0.0.md`.

| Operation | RaftCore(1M) |
|---|---|
| Leader State (`leaderState` access) | 873.36 µs |
| Term (`currentTerm` access) | 1.40 ms |
| AppendEntries Heartbeat | 22.51 ms |
| AppendEntries Append | 55.01 ms |
| RequestVote Grant | 42.76 ms |
| Leader Reply Commit | 117.41 ms |
| Become Leader | 249.31 ms |
| Election Round | 722.50 ms |
| Commit Advance (1000-entry log) | 74.02 s |

Moving entries is doing real work: handing a batch of 100 entries over by
move (`Args Mv 100`, 10.68 ms at 1M) is roughly **~220x faster** than
copying it (`Args Cp 100`, 2.36 s) — that gap is the argument for
`LogEntry` and `AppendEntriesArgs` being movable. The cheap accessors are
effectively free: `role`, `commitIndex`, and `selfId` each cost about
0.84 ms per 1M calls, under a nanosecond apiece.

Separately, commit advancement is the single most expensive operation
measured: `Commit 1000` takes 74.02 s at 1M iterations versus 114.35 ms
for `Commit 1` — roughly 650x for 1000x the log length. Expected, given
that each advance scans downward from the log tail doing an `entryAt`
lookup plus a sweep over every peer's `matchIndex` per candidate, but
worth knowing before letting a large uncommitted gap build up on a
latency-sensitive path. Cost also scales with peer count (`Lead 100` is
about 52x `Lead 2`), since leader-side work is one send per peer.

<div align="right"><a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a></div>

## <a id="fuzzing"></a>🐛 Fuzzing

`RaftNode` and a whole three-node cluster are fuzzed via
[ClusterFuzzLite](https://google.github.io/clusterfuzzlite/), under
AddressSanitizer and UndefinedBehaviorSanitizer. A 5-minute pass runs on
every PR touching the consensus code, the libraries under it, or the
harnesses.

RaftCore has no wire format to parse — messages arrive as typed structs
from whatever `Transport` you plug in — so neither harness fuzzes a byte
parser. They fuzz sequences of operations and check invariants after every
single one, so a failing input localizes to the exact call that broke it.
`fuzz_raft_rpc` throws arbitrary, mostly non-conformant RPCs, ticks, and
local appends at a single node, and asserts only what must hold for *any*
input: terms and the commit index never decrease, a vote never changes or
clears within a term, a stale-term `AppendEntries` leaves the log
untouched, and a successful reply never claims a `matchIndex` beyond what
the RPC delivered. `fuzz_raft_cluster` runs three nodes that generate
their own, protocol-conformant RPCs while the fuzzer controls only the
environment — tick order, message reordering and loss, partitions, client
commands on stale Leaders, and crash/restart from persisted state — and
checks the Raft safety properties: Election Safety, Log Matching, State
Machine Safety, vote stability, and term monotonicity across restarts.

Duplicated messages, clusters of other sizes, and storage write failures
are deliberately not covered yet, and membership change and snapshots
don't exist in RaftCore to fuzz.

See [FUZZING.md](FUZZING.md) for running the harnesses locally and
reproducing a failing input.

<div align="right"><a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a></div>

## <a id="documentation"></a>📖 Documentation

Full API reference, generated with Doxygen from `docs/Doxyfile`:

**https://privateMwb.github.io/RaftCore/**

<div align="right"><a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a></div>

## <a id="contributing"></a>🤝 Contributing

Issues and pull requests are welcome. Before submitting a PR:

- Run the test suite (`ctest --test-dir build`)
- If you're changing a hot path, run `./build/regression` and mention
  the results in your PR description
- If you're changing `RaftNode` or `NodeState`, a quick local fuzz pass
  (see [FUZZING.md](FUZZING.md)) before pushing catches most
  safety-property regressions before CI does

<div align="right"><a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a></div>

## <a id="changelog"></a>📝 Changelog

See the [Releases](https://github.com/privateMwb/RaftCore/releases)
page for version history and release notes.

<div align="right"><a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a></div>

## <a id="security"></a>🔒 Security

Please don't report a suspected vulnerability in a public issue. Use
GitHub's private reporting instead — this repository's **Security** tab,
then **Report a vulnerability** — so it can be fixed before it's disclosed.

<div align="right"><a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a></div>

## <a id="license"></a>📄 License

MIT — see [LICENSE](LICENSE) for details.

<p align="center">
  <sub>Built with C++23</sub>
</p>

<p align="center">
  <a href="#-table-of-contents"><img src=".github/assets/back-to-top.svg" alt="Back to top" height="28"></a>
</p>
