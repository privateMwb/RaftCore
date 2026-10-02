// State Access Benchmark Suite
// Measures read cost of NodeState's accessors: the persistent-state
// getters (which delegate through the PersistentState interface, and for
// votedFor() return an optional<NodeId> by value) vs the plain in-object
// volatile-state getters.
//
// Covers:
// - currentTerm (virtual delegation to PersistentState)
// - votedFor set (optional<string> copy) vs unset (empty optional)
// - role, commitIndex, lastApplied (direct member reads)
// - selfId (const-ref return)
// - log().lastIndex() / lastTerm() (virtual call into Storage)

#include <benchmark/benchmark.h>

#include <support/framework.h>

using namespace RaftCore;

namespace {

// Minimal in-memory PersistentState: no I/O, so the benchmark measures
// NodeState's own read path rather than a real fsync'd store.
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
// here, so the entry-reading methods are unreachable stubs.
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

} // namespace

// Measures currentTerm() -- one virtual call into PersistentState.
static void state_access_current_term(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    for (auto _ : state) {
        Term t = node.currentTerm();
        benchmark::DoNotOptimize(t);
    }
}
BENCHMARK(state_access_current_term);

// Measures votedFor() when a vote has been cast -- copies the NodeId
// string into the returned optional.
static void state_access_voted_for_set(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    (void)ps.setVotedFor(NodeId("node-2"));
    NodeState node(ps, st, "node-1");

    for (auto _ : state) {
        std::optional<NodeId> v = node.votedFor();
        benchmark::DoNotOptimize(v);
    }
}
BENCHMARK(state_access_voted_for_set);

// Measures votedFor() when no vote has been cast -- empty optional, no
// string copy.
static void state_access_voted_for_none(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    for (auto _ : state) {
        std::optional<NodeId> v = node.votedFor();
        benchmark::DoNotOptimize(v);
    }
}
BENCHMARK(state_access_voted_for_none);

// Measures role() -- a plain member read.
static void state_access_role(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    for (auto _ : state) {
        Role r = node.role();
        benchmark::DoNotOptimize(r);
    }
}
BENCHMARK(state_access_role);

// Measures commitIndex() -- a plain member read.
static void state_access_commit_index(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    for (auto _ : state) {
        LogIndex c = node.commitIndex();
        benchmark::DoNotOptimize(c);
    }
}
BENCHMARK(state_access_commit_index);

// Measures lastApplied() -- a plain member read.
static void state_access_last_applied(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    for (auto _ : state) {
        LogIndex a = node.lastApplied();
        benchmark::DoNotOptimize(a);
    }
}
BENCHMARK(state_access_last_applied);

// Measures selfId() -- a const-reference return, no copy.
static void state_access_self_id(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    for (auto _ : state) {
        const NodeId& id = node.selfId();
        benchmark::DoNotOptimize(id);
    }
}
BENCHMARK(state_access_self_id);

// Measures log().lastIndex() -- accessor plus a virtual call into Storage.
static void state_access_log_last_index(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    for (auto _ : state) {
        LogIndex i = node.log().lastIndex();
        benchmark::DoNotOptimize(i);
    }
}
BENCHMARK(state_access_log_last_index);

// Measures log().lastTerm() -- same path as lastIndex().
static void state_access_log_last_term(benchmark::State& state) {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    for (auto _ : state) {
        Term t = node.log().lastTerm();
        benchmark::DoNotOptimize(t);
    }
}
BENCHMARK(state_access_log_last_term);
