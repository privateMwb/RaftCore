// Shared in-memory fakes and a ready-made node rig for the RaftCore
// benchmark suites. No I/O, no sockets, no real randomness: every cost
// measured through these is RaftCore's own.
//
// Simplifications (deliberate, for benchmarking only):
// - FakeStorage keeps only entry terms; payloads are discarded on append
//   and come back empty. All benchmarks use empty payloads anyway.
// - FakeTransport counts sends and drops the reply callback, so no
//   reply ever arrives on its own -- benchmarks feed replies in by hand.
// - FakeRandom always returns the lower bound, so election timeouts are
//   exactly `electionTicks` and fully deterministic.

#pragma once

#include <RaftCore/Core/RaftNode.h>
#include <VectorPro/Vector.h>

#include <cstddef>
#include <optional>
#include <string>
#include <utility>

namespace RaftBench {

using namespace RaftCore;
using namespace VectorPro;

// In-memory PersistentState. Starts at term 7, no vote cast.
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

// In-memory Storage over a VectorPro::Vector of terms.
class FakeStorage : public Storage {
  public:
    // Starts with `initialEntries` entries, all of term `term`.
    explicit FakeStorage(std::size_t initialEntries = 0, Term term = 7) {
        for (std::size_t i = 0; i < initialEntries; ++i)
            terms_.push_back(term);
    }

    Status append(Term term, Vector<std::uint8_t>, LogIndex& outIndex) override {
        terms_.push_back(term);
        outIndex = terms_.size();
        return Status::OK;
    }

    Status entryAt(LogIndex index, LogEntry& outEntry) const override {
        if (index == kNoIndex || index > terms_.size())
            return Status::NOT_FOUND;
        outEntry.term = terms_[index - 1];
        return Status::OK;
    }

    Status range(LogIndex fromIndex, LogIndex toIndex,
                 Vector<LogEntry>& outEntries) const override {
        if (fromIndex == kNoIndex || fromIndex > toIndex || toIndex > terms_.size())
            return Status::NOT_FOUND;
        for (LogIndex i = fromIndex; i <= toIndex; ++i) {
            LogEntry entry;
            entry.term = terms_[i - 1];
            outEntries.push_back(std::move(entry));
        }
        return Status::OK;
    }

    Status truncateFrom(LogIndex index) override {
        if (index != kNoIndex && index <= terms_.size()) {
            while (terms_.size() > index - 1)
                terms_.pop_back();
        }
        return Status::OK;
    }

    LogIndex lastIndex() const override {
        return terms_.size();
    }
    Term lastTerm() const override {
        return terms_.size() == 0 ? kNoTerm : terms_[terms_.size() - 1];
    }

  private:
    Vector<Term> terms_;
};

// Counts dispatches and discards the reply callback.
class FakeTransport : public Transport {
  public:
    Status sendRequestVote(const NodeId&, RequestVoteArgs,
                           MoveOnlyFunction<void(RequestVoteReply)>) override {
        ++voteSends;
        return Status::OK;
    }
    Status sendAppendEntries(const NodeId&, AppendEntriesArgs,
                             MoveOnlyFunction<void(AppendEntriesReply)>) override {
        ++appendSends;
        return Status::OK;
    }

    std::size_t voteSends = 0;
    std::size_t appendSends = 0;
};

// Always returns the lower bound: timeouts are exact.
class FakeRandom : public RandomSource {
  public:
    int nextInt(int minInclusive, int) override {
        return minInclusive;
    }
};

// Peer list of `count` peers named "node-2", "node-3", ... (default 4:
// a 5-node cluster together with "node-1").
inline Vector<NodeId> makePeers(std::size_t count = 4) {
    Vector<NodeId> peers;
    for (std::size_t i = 0; i < count; ++i)
        peers.push_back(NodeId("node-" + std::to_string(i + 2)));
    return peers;
}

// One fully wired node: fakes + NodeState + RaftNode, as "node-1" with
// `peerCount` peers (default 4: a 5-node cluster). Starts as Follower at
// term 7 with `logEntries` entries of term 7. Election timeout is
// exactly `electionTicks`; a leader heartbeats every `heartbeatTicks`.
// Non-copyable (holds references between members).
struct Rig {
    explicit Rig(std::size_t logEntries = 0, int electionTicks = 150, std::size_t peerCount = 4,
                 int heartbeatTicks = 50)
        : st(logEntries, 7), node(ps, st, "node-1"),
          raft(node, transport, random, makePeers(peerCount), electionTicks, electionTicks,
               heartbeatTicks) {}

    FakePersistentState ps;
    FakeStorage st;
    FakeTransport transport;
    FakeRandom random;
    NodeState node;
    RaftNode raft;
};

} // namespace RaftBench
