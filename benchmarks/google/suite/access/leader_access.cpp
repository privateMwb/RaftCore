// Leader Access Benchmark Suite
// Measures lookup cost of a Leader's replication-tracking state: the
// leaderState() accessor, and the per-peer nextIndex/matchIndex HashMap
// reads that replicateTo(), handleAppendEntriesReply() and
// tryAdvanceCommitIndex() perform on every RPC. Keys are string NodeIds,
// so each lookup includes a string hash.
//
// Covers:
// - leaderState() accessor (no lookup)
// - nextIndex[peer] hit (operator[] on an existing key)
// - matchIndex[peer] hit
// - matchIndex.contains() hit
// - matchIndex.contains() miss (unknown peer)

#include <benchmark/benchmark.h>

#include <support/framework.h>

using namespace RaftCore;

namespace {

// Minimal in-memory PersistentState: no I/O.
class FakePersistentState : public PersistentState {
  public:
    Status setCurrentTerm(Term term) override {
        term_ = term;
        return Status::OK;
    }
    Term currentTerm() const override {
        return term_;
    }

    Status setVotedFor(std::optional<NodeId> candidate) override {
        votedFor_ = std::move(candidate);
        return Status::OK;
    }
    std::optional<NodeId> votedFor() const override {
        return votedFor_;
    }

  private:
    Term term_ = 7;
    std::optional<NodeId> votedFor_;
};

// Minimal in-memory Storage: only lastIndex()/lastTerm() are exercised
// (becomeLeader() reads lastIndex()), so the entry-reading methods are
// unreachable stubs.
class FakeStorage : public Storage {
  public:
    Status append(Term term, Vector<std::uint8_t>, LogIndex& outIndex) override {
        lastTerm_ = term;
        outIndex = ++lastIndex_;
        return Status::OK;
    }
    Status entryAt(LogIndex, LogEntry&) const override {
        return Status::NOT_FOUND;
    }
    Status range(LogIndex, LogIndex, Vector<LogEntry>&) const override {
        return Status::NOT_FOUND;
    }
    Status truncateFrom(LogIndex) override {
        return Status::OK;
    }
    LogIndex lastIndex() const override {
        return lastIndex_;
    }
    Term lastTerm() const override {
        return lastTerm_;
    }

  private:
    LogIndex lastIndex_ = 100;
    Term lastTerm_ = 7;
};

// Builds the peer list for a 5-node cluster (this node plus 4 peers).
Vector<NodeId> makePeers() {
    Vector<NodeId> peers;
    peers.push_back(NodeId("node-2"));
    peers.push_back(NodeId("node-3"));
    peers.push_back(NodeId("node-4"));
    peers.push_back(NodeId("node-5"));
    return peers;
}

} // namespace

// Measures leaderState() -- accessor only, no map lookup.
static void leader_access_leader_state(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");
    node.becomeLeader(makePeers());

    for (auto _ : state) {
        LeaderVolatileState& s = node.leaderState();
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(leader_access_leader_state);

// Measures nextIndex[peer] on an existing key -- the read replicateTo()
// does before every send.
static void leader_access_next_index_hit(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");
    node.becomeLeader(makePeers());
    const NodeId peer("node-4");

    for (auto _ : state) {
        LogIndex n = node.leaderState().nextIndex[peer];
        benchmark::DoNotOptimize(n);
    }
}
BENCHMARK(leader_access_next_index_hit);

// Measures matchIndex[peer] on an existing key -- the read
// handleAppendEntriesReply() and tryAdvanceCommitIndex() do per peer.
static void leader_access_match_index_hit(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");
    node.becomeLeader(makePeers());
    const NodeId peer("node-4");

    for (auto _ : state) {
        LogIndex m = node.leaderState().matchIndex[peer];
        benchmark::DoNotOptimize(m);
    }
}
BENCHMARK(leader_access_match_index_hit);

// Measures matchIndex.contains() for a tracked peer.
static void leader_access_contains_hit(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");
    node.becomeLeader(makePeers());
    const NodeId peer("node-4");

    for (auto _ : state) {
        bool found = node.leaderState().matchIndex.contains(peer);
        benchmark::DoNotOptimize(found);
    }
}
BENCHMARK(leader_access_contains_hit);

// Measures matchIndex.contains() for a peer that isn't tracked.
static void leader_access_contains_miss(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");
    node.becomeLeader(makePeers());
    const NodeId stranger("node-99");

    for (auto _ : state) {
        bool found = node.leaderState().matchIndex.contains(stranger);
        benchmark::DoNotOptimize(found);
    }
}
BENCHMARK(leader_access_contains_miss);
