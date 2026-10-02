// ============================================================
// fuzz/fuzz_raft_rpc.cpp
//
// Adversarial-input fuzzer for a single RaftNode's RPC handlers.
//
// The fuzzer's bytes become a stream of operations against one node
// ("node-1", a Follower at term 0 in a three-node cluster): ticks,
// RequestVote and AppendEntries requests with arbitrary terms,
// indexes and entry lists, RequestVote/AppendEntries replies from
// peers, and (once the node has become Leader) local appends. Most of
// these inputs are NOT valid Raft -- a "leader" claims a prevLogIndex
// the follower never had, a reply is stamped with a term from long ago
// -- which is the point: a node has to survive and stay consistent
// whatever arrives over the wire.
//
// Because the inputs aren't protocol-conformant, the harness only
// asserts properties that must hold for ANY input, checked after
// every single operation so a failure localizes to the call that
// caused it:
//
//   - currentTerm never decreases
//   - commitIndex never decreases
//   - votedFor never changes (or clears) within a single term
//   - a handler's reply carries the node's resulting term
//   - a vote is never granted to a stale-term candidate, and a granted
//     vote is recorded as votedFor
//   - a stale-term AppendEntries is rejected without touching the log
//   - a successful AppendEntries reply never claims a matchIndex
//     beyond what the RPC itself delivered (prevLogIndex + number of
//     entries) -- over-reporting lets a leader count a follower toward
//     a majority for entries it doesn't actually hold
//   - a Leader always has nextIndex >= 1 and a matchIndex slot for
//     every peer
//
// Plus everything the sanitizers catch: out-of-range log access,
// use-after-free, integer overflow in index arithmetic, and so on.
//
// Deliberately NOT covered here: protocol-level safety across several
// nodes (see fuzz_raft_cluster.cpp), and storage failures (these fakes
// never return an error).
// ============================================================

#include <RaftCore/RaftCore.h>

#include <cstdint>
#include <cstdlib>
#include <optional>

using namespace RaftCore;

namespace {

// Aborts (rather than throwing/returning) on a violated invariant so
// libFuzzer captures a minimal, precise reproducer.
void require(bool ok) {
    if (!ok)
        std::abort();
}

// Reads fuzzer bytes one at a time; yields 0 once the input runs out.
class ByteReader {
  public:
    ByteReader(const std::uint8_t* data, std::size_t size) : data_(data), size_(size) {}

    bool done() const {
        return pos_ >= size_;
    }
    std::uint8_t next() {
        return pos_ < size_ ? data_[pos_++] : 0;
    }

  private:
    const std::uint8_t* data_;
    std::size_t size_;
    std::size_t pos_ = 0;
};

// In-memory PersistentState: the durable currentTerm and votedFor.
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

// In-memory Storage: the log, with payloads.
class MemoryStorage : public Storage {
  public:
    Status append(Term term, Vector<std::uint8_t> payload, LogIndex& outIndex) override {
        LogEntry entry;
        entry.term = term;
        entry.payload = std::move(payload);
        entries_.push_back(std::move(entry));
        outIndex = entries_.size();
        return Status::OK;
    }
    Status entryAt(LogIndex index, LogEntry& outEntry) const override {
        if (index == kNoIndex || index > entries_.size())
            return Status::NOT_FOUND;
        outEntry = entries_[index - 1];
        return Status::OK;
    }
    Status range(LogIndex fromIndex, LogIndex toIndex,
                 Vector<LogEntry>& outEntries) const override {
        if (fromIndex == kNoIndex || fromIndex > toIndex || toIndex > entries_.size())
            return Status::NOT_FOUND;
        for (LogIndex i = fromIndex; i <= toIndex; ++i)
            outEntries.push_back(entries_[i - 1]);
        return Status::OK;
    }
    Status truncateFrom(LogIndex index) override {
        if (index != kNoIndex && index <= entries_.size()) {
            while (entries_.size() > index - 1)
                entries_.pop_back();
        }
        return Status::OK;
    }
    LogIndex lastIndex() const override {
        return entries_.size();
    }
    Term lastTerm() const override {
        return entries_.size() == 0 ? kNoTerm : entries_[entries_.size() - 1].term;
    }

  private:
    Vector<LogEntry> entries_;
};

// Transport that accepts every RPC and drops it: replies are fed in by
// the fuzzer instead.
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

class FixedRandom : public RandomSource {
  public:
    int nextInt(int minInclusive, int) override {
        return minInclusive;
    }
};

const char* const kPeers[3] = {"node-2", "node-3", "node-9"}; // node-9 isn't a member.

// A term near the node's current one: from 2 behind to 3 ahead.
Term nearbyTerm(Term current, std::uint8_t byte) {
    const int delta = static_cast<int>(byte % 6) - 2;
    if (delta < 0 && current < static_cast<Term>(-delta))
        return 0;
    return static_cast<Term>(static_cast<std::int64_t>(current) + delta);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (size == 0)
        return 0;

    ByteReader in(data, size);

    MemoryPersistentState persistent;
    MemoryStorage storage;
    NullTransport transport;
    FixedRandom random;
    NodeState state(persistent, storage, "node-1");

    Vector<NodeId> peers;
    peers.push_back(NodeId("node-2"));
    peers.push_back(NodeId("node-3"));
    RaftNode raft(state, transport, random, std::move(peers), 5, 5, 2);

    Term prevTerm = state.currentTerm();
    LogIndex prevCommit = state.commitIndex();
    std::optional<NodeId> prevVote = state.votedFor();

    while (!in.done()) {
        switch (in.next() % 6) {
        case 0: { // tick
            (void)raft.tick();
            break;
        }
        case 1: { // RequestVote request
            RequestVoteArgs args;
            args.term = nearbyTerm(state.currentTerm(), in.next());
            args.candidateId = kPeers[in.next() % 3];
            args.lastLogIndex = in.next() % 9;
            args.lastLogTerm = in.next() % 6;

            const Term termBefore = state.currentTerm();
            RequestVoteReply reply;
            if (raft.handleRequestVote(args, reply) == Status::OK) {
                require(reply.term == state.currentTerm());
                if (args.term < termBefore)
                    require(!reply.voteGranted);
                if (reply.voteGranted) {
                    const std::optional<NodeId> vote = state.votedFor();
                    require(vote.has_value() && *vote == args.candidateId);
                    require(args.term == state.currentTerm());
                }
            }
            break;
        }
        case 2: { // AppendEntries request
            AppendEntriesArgs args;
            args.term = nearbyTerm(state.currentTerm(), in.next());
            args.leaderId = kPeers[in.next() % 3];
            args.prevLogIndex = in.next() % 9;
            args.prevLogTerm = in.next() % 6;
            args.leaderCommit = in.next() % 12;
            const std::size_t entryCount = in.next() % 5;
            for (std::size_t i = 0; i < entryCount; ++i) {
                LogEntry entry;
                entry.term = in.next() % 6;
                args.entries.push_back(std::move(entry));
            }

            const Term argsTerm = args.term;
            const LogIndex prevIndex = args.prevLogIndex;
            const std::size_t delivered = args.entries.size();
            const Term termBefore = state.currentTerm();
            const LogIndex lastBefore = storage.lastIndex();

            AppendEntriesReply reply;
            if (raft.handleAppendEntries(std::move(args), reply) == Status::OK) {
                require(reply.term == state.currentTerm());
                if (argsTerm < termBefore) {
                    // Stale leader: refused, and nothing was written.
                    require(!reply.success);
                    require(storage.lastIndex() == lastBefore);
                }
                if (reply.success) {
                    require(state.currentTerm() == argsTerm);
                    require(storage.lastIndex() >= prevIndex + delivered);
                    // What the RPC proved the follower holds -- no more.
                    require(reply.matchIndex <= prevIndex + delivered);
                }
            }
            break;
        }
        case 3: { // RequestVote reply
            RequestVoteReply reply;
            reply.term = nearbyTerm(state.currentTerm(), in.next());
            reply.voteGranted = (in.next() & 1) != 0;
            raft.handleRequestVoteReply(NodeId(kPeers[in.next() % 3]), reply);
            break;
        }
        case 4: { // AppendEntries reply
            AppendEntriesReply reply;
            reply.term = nearbyTerm(state.currentTerm(), in.next());
            reply.success = (in.next() & 1) != 0;
            // A follower can't have more than the Leader sent it.
            reply.matchIndex = in.next() % (storage.lastIndex() + 1);
            raft.handleAppendEntriesReply(NodeId(kPeers[in.next() % 3]), reply);
            break;
        }
        case 5: { // local append, as a Leader would do for a client command
            const std::uint8_t byte = in.next();
            if (state.role() == Role::Leader) {
                Vector<std::uint8_t> payload;
                payload.push_back(byte);
                LogIndex index = kNoIndex;
                (void)storage.append(state.currentTerm(), std::move(payload), index);
            }
            break;
        }
        default:
            break;
        }

        // Invariants that hold after every operation, for any input.
        require(state.currentTerm() >= prevTerm);
        require(state.commitIndex() >= prevCommit);

        const std::optional<NodeId> vote = state.votedFor();
        if (state.currentTerm() == prevTerm && prevVote.has_value()) {
            // Once a vote is cast in a term it can't change or be withdrawn.
            require(vote.has_value() && *vote == *prevVote);
        }

        if (state.role() == Role::Leader) {
            for (const char* peer : {"node-2", "node-3"}) {
                const NodeId id(peer);
                require(state.leaderState().nextIndex.contains(id));
                require(state.leaderState().nextIndex[id] >= 1);
                require(state.leaderState().matchIndex.contains(id));
            }
        }

        prevTerm = state.currentTerm();
        prevCommit = state.commitIndex();
        prevVote = vote;
    }

    return 0;
}
