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
static void bench_current_term() {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    auto readTerm = [&] {
        Term t = node.currentTerm();
        doNotOptimize(t);
    };
    BENCH_SOLO("term", readTerm);
}

// Measures votedFor() when a vote has been cast -- copies the NodeId
// string into the returned optional.
static void bench_voted_for_set() {
    FakePersistentState ps;
    FakeStorage st;
    (void)ps.setVotedFor(NodeId("node-2"));
    NodeState node(ps, st, "node-1");

    auto readVote = [&] {
        std::optional<NodeId> v = node.votedFor();
        doNotOptimize(v);
    };
    BENCH_SOLO("vote set", readVote);
}

// Measures votedFor() when no vote has been cast -- empty optional, no
// string copy.
static void bench_voted_for_none() {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    auto readVote = [&] {
        std::optional<NodeId> v = node.votedFor();
        doNotOptimize(v);
    };
    BENCH_SOLO("vote none", readVote);
}

// Measures role() -- a plain member read.
static void bench_role() {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    auto readRole = [&] {
        Role r = node.role();
        doNotOptimize(r);
    };
    BENCH_SOLO("role", readRole);
}

// Measures commitIndex() -- a plain member read.
static void bench_commit_index() {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    auto readCommit = [&] {
        LogIndex c = node.commitIndex();
        doNotOptimize(c);
    };
    BENCH_SOLO("commit", readCommit);
}

// Measures lastApplied() -- a plain member read.
static void bench_last_applied() {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    auto readApplied = [&] {
        LogIndex a = node.lastApplied();
        doNotOptimize(a);
    };
    BENCH_SOLO("applied", readApplied);
}

// Measures selfId() -- a const-reference return, no copy.
static void bench_self_id() {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    auto readSelf = [&] {
        const NodeId& id = node.selfId();
        doNotOptimize(id);
    };
    BENCH_SOLO("self id", readSelf);
}

// Measures log().lastIndex() -- accessor plus a virtual call into Storage.
static void bench_log_last_index() {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    auto readLast = [&] {
        LogIndex i = node.log().lastIndex();
        doNotOptimize(i);
    };
    BENCH_SOLO("last idx", readLast);
}

// Measures log().lastTerm() -- same path as lastIndex().
static void bench_log_last_term() {
    FakePersistentState ps;
    FakeStorage st;
    NodeState node(ps, st, "node-1");

    auto readLast = [&] {
        Term t = node.log().lastTerm();
        doNotOptimize(t);
    };
    BENCH_SOLO("last term", readLast);
}

// Executes all NodeState access benchmark cases.
static void run_benchmarks() {
    bench_current_term();
    bench_voted_for_set();
    bench_voted_for_none();
    bench_role();
    bench_commit_index();
    bench_last_applied();
    bench_self_id();
    bench_log_last_index();
    bench_log_last_term();
}

REGISTER_BENCH_SUITE();
