// Minimal smoke test: confirms the installed package's headers are
// reachable and the library links, by constructing a RaftNode on
// in-memory fakes and exercising one real operation (tick it to its
// election timeout and check it started an election).
//
// RaftCore's public API is a set of concrete classes (NodeState, RaftNode)
// built on four interfaces the caller implements (PersistentState,
// Storage, Transport, RandomSource) -- not a templated container shape --
// so this implements the smallest possible version of each rather than
// reusing the pattern from the container libraries' test_package.cpp files.

#include <RaftCore/RaftCore.h>

#include <iostream>

using namespace RaftCore;

namespace {

class MemoryPersistentState : public PersistentState {
public:
    Status setCurrentTerm(Term term) override {
        term_ = term;
        return Status::OK;
    }
    Term currentTerm() const override { return term_; }

    Status setVotedFor(std::optional<NodeId> candidate) override {
        votedFor_ = std::move(candidate);
        return Status::OK;
    }
    std::optional<NodeId> votedFor() const override { return votedFor_; }

private:
    Term term_ = 0;
    std::optional<NodeId> votedFor_;
};

class MemoryStorage : public Storage {
public:
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
    LogIndex lastIndex() const override { return terms_.size(); }
    Term lastTerm() const override {
        return terms_.size() == 0 ? kNoTerm : terms_[terms_.size() - 1];
    }

private:
    Vector<Term> terms_;
};

class CountingTransport : public Transport {
public:
    Status sendRequestVote(const NodeId&, RequestVoteArgs,
                           MoveOnlyFunction<void(RequestVoteReply)>) override {
        ++voteSends;
        return Status::OK;
    }
    Status sendAppendEntries(const NodeId&, AppendEntriesArgs,
                             MoveOnlyFunction<void(AppendEntriesReply)>) override {
        return Status::OK;
    }

    int voteSends = 0;
};

class FixedRandom : public RandomSource {
public:
    int nextInt(int minInclusive, int) override { return minInclusive; }
};

} // namespace

int main() {
    MemoryPersistentState persistent;
    MemoryStorage storage;
    CountingTransport transport;
    FixedRandom random;
    NodeState state(persistent, storage, "node-1");

    Vector<NodeId> peers;
    peers.push_back(NodeId("node-2"));
    peers.push_back(NodeId("node-3"));

    // Election timeout of exactly 3 ticks, heartbeat every 2.
    RaftNode raft(state, transport, random, std::move(peers), 3, 3, 2);

    for (int i = 0; i < 3; ++i) {
        if (raft.tick() != Status::OK) {
            std::cerr << "tick() failed\n";
            return 1;
        }
    }

    // After the timeout the node must have started an election: Candidate,
    // term 1, a RequestVote sent to each of the two peers.
    if (state.role() != Role::Candidate || state.currentTerm() != 1 || transport.voteSends != 2) {
        std::cerr << "RaftCore did not start an election as expected\n";
        return 1;
    }

    std::cout << "RaftCore linked, constructed a node, and ran an election timeout successfully.\n";
    return 0;
}
