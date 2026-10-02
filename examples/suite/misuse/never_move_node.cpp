// Never Move a Node.
//
// Demonstrates:
// - the compiler letting you move or copy a RaftNode
// - why relocating a live node is wrong: its in-flight callbacks point at
//   the address it had when it sent the RPC
// - the fix: a wrapper that cannot be copied or moved, so the mistake
//   becomes a compile error

#include <support/framework.h>

using namespace RaftCore;

namespace {

// PersistentState: the durable currentTerm and votedFor.
class MemoryPersistentState : public PersistentState {
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
    Term term_ = 0;
    std::optional<NodeId> votedFor_;
};

// Storage: the log, kept as a list of terms -- payloads don't matter here.
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
    LogIndex lastIndex() const override {
        return terms_.size();
    }
    Term lastTerm() const override {
        return terms_.size() == 0 ? kNoTerm : terms_[terms_.size() - 1];
    }

  private:
    Vector<Term> terms_;
};

// Transport: sends RPCs to peers. This one accepts every RPC and drops it,
// so no reply ever comes back.
class NullTransport : public Transport {
  public:
    Status sendRequestVote(const NodeId&, RequestVoteArgs,
                           MoveOnlyFunction<void(RequestVoteReply)>) override {
        return Status::OK;
    }
    Status sendAppendEntries(const NodeId&, AppendEntriesArgs,
                             MoveOnlyFunction<void(AppendEntriesReply)>) override {
        return Status::OK;
    }
};

// RandomSource: picks each election timeout. Returning the lower bound
// makes it exact.
class FixedRandom : public RandomSource {
  public:
    int nextInt(int minInclusive, int) override {
        return minInclusive;
    }
};

// The fix: everything a node needs, built in place and pinned there. Copy
// and move are deleted, so any attempt to relocate it is a compile error
// rather than a bug that shows up as a lost vote.
struct PinnedNode {
    PinnedNode()
        : state(persistent, storage, "node-1"),
          raft(state, transport, random, makePeers(), 3, 3, 2) {}

    PinnedNode(const PinnedNode&) = delete;
    PinnedNode& operator=(const PinnedNode&) = delete;
    PinnedNode(PinnedNode&&) = delete;
    PinnedNode& operator=(PinnedNode&&) = delete;

    MemoryPersistentState persistent;
    MemoryStorage storage;
    NullTransport transport;
    FixedRandom random;
    NodeState state;
    RaftNode raft;

  private:
    static Vector<NodeId> makePeers() {
        Vector<NodeId> peers;
        peers.push_back(NodeId("node-2"));
        peers.push_back(NodeId("node-3"));
        return peers;
    }
};

static_assert(!std::is_move_constructible<PinnedNode>::value, "a PinnedNode must not be movable");
static_assert(!std::is_copy_constructible<PinnedNode>::value, "a PinnedNode must not be copyable");

const char* roleName(Role role) {
    switch (role) {
    case Role::Follower:
        return "Follower";
    case Role::Candidate:
        return "Candidate";
    case Role::Leader:
        return "Leader";
    }
    return "?";
}

} // namespace

static void run_examples() {
    // RaftNode declares no copy or move rules of its own, and everything
    // it holds is movable, so the compiler happily lets you write
    //
    //     RaftNode moved(std::move(node));
    //
    // and it compiles without a warning. That is the danger.
    setTitle("Trap: The Compiler Won't Stop You");

    std::cout << "RaftNode is move-constructible : "
              << (std::is_move_constructible<RaftNode>::value ? "yes" : "no") << "\n\n"; // (yes)

    // When a node starts an election it hands the Transport a reply
    // callback for each peer. Each callback holds the node's address
    // (`this`) and a pointer to that peer's slot in the node's peer list,
    // and a Transport may hold it for as long as the RPC is in flight. If
    // the node is moved (or simply relocated, say by being an element of a
    // container that reallocates) the callback still calls the object at
    // the *old* address -- now a moved-from shell, or nothing at all.
    // Replies land on the wrong object, votes and acknowledgements go
    // missing, and there is no error to tell you: the node just never
    // wins, or never commits.
    //
    // Nothing is run here on purpose -- the point is that the failure is
    // silent, so the fix has to make the mistake impossible instead.
    setTitle("Why It Goes Wrong");

    std::cout << "(see the comments above -- this step has no output)\n\n";

    // PinnedNode deletes copy and move (the static_asserts above prove it),
    // so a line like
    //
    //     PinnedNode moved(std::move(node)); // error: use of deleted function
    //
    // fails to compile. Construct the node where it will live: a local
    // variable, a member of a long-lived object, or a heap allocation you
    // never copy out of. Node addresses then stay put for its whole life.
    setTitle("Fix: Pin It in Place");

    PinnedNode node;
    for (int i = 0; i < 3; ++i)
        (void)node.raft.tick();

    std::cout << "node built in place : " << roleName(node.state.role()) << ", term "
              << node.state.currentTerm() << "\n"; // (Candidate, term 1)
}

REGISTER_EXAMPLE_SUITE();
