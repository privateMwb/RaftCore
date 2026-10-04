#pragma once

// clang-format off
#include <RaftCore/Core/NodeState.h>             // NodeState -- SimulatedTransport's handlers call into it
#include <RaftCore/Core/RaftNode.h>              // RaftNode -- same
#include <RaftCore/Interfaces/PersistentState.h> // PersistentState -- InMemoryPersistentState implements it
#include <RaftCore/Interfaces/RandomSource.h>    // RandomSource -- ScriptedRandomSource implements it
#include <RaftCore/Interfaces/Storage.h>         // Storage -- InMemoryStorage implements it
#include <RaftCore/Interfaces/Transport.h>       // Transport -- SimulatedTransport implements it

#include <FunctionPro/Function.h>          // registered node handlers (copyable, long-lived)
#include <FunctionPro/MoveOnlyFunction.h>  // reply callbacks, per the Transport contract
#include <HashMapPro/HashMap.h>            // node registry
#include <VectorPro/Vector.h>              // entries_, script_, pending-message queues

#include <optional>                        // votedFor_
#include <utility>                         // std::move
// clang-format on

// The four fakes RaftNode/NodeState tests are built against, shared
// across every test .cpp file: InMemoryStorage, InMemoryPersistentState,
// ScriptedRandomSource, SimulatedTransport. Included transitively via
// <support/framework.h> -- a test file doesn't include this directly.

namespace RaftCore::Test {

using namespace FunctionPro;
using namespace HashMapPro;
using namespace VectorPro;

// Trivial in-memory Storage: no durability, no fsync, entries simply
// live in a Vector for the process's lifetime. Never use this as a real
// Storage backing -- a real one is WriteAheadLog-backed and actually
// durable, which is the entire point of the interface.
class InMemoryStorage final : public Storage {
  public:
    [[nodiscard]] Status append(Term term, Vector<std::uint8_t> payload,
                                LogIndex& outIndex) override {
        LogEntry entry;
        entry.term = term;
        entry.payload = std::move(payload);
        entries_.push_back(std::move(entry));
        outIndex = entries_.size();
        return Status::OK;
    }

    [[nodiscard]] Status entryAt(LogIndex index, LogEntry& outEntry) const override {
        if (index == kNoIndex || index > entries_.size())
            return Status::NOT_FOUND;
        outEntry = entries_[index - 1];
        return Status::OK;
    }

    [[nodiscard]] Status range(LogIndex fromIndex, LogIndex toIndex,
                               Vector<LogEntry>& outEntries) const override {
        outEntries.clear();
        if (fromIndex == kNoIndex || fromIndex > toIndex || toIndex > entries_.size())
            return Status::NOT_FOUND;
        for (LogIndex i = fromIndex; i <= toIndex; ++i)
            outEntries.push_back(entries_[i - 1]);
        return Status::OK;
    }

    [[nodiscard]] Status truncateFrom(LogIndex index) override {
        if (index == kNoIndex || index > entries_.size())
            return Status::OK; // No-op, per Storage::truncateFrom()'s documented contract.
        while (entries_.size() >= index)
            entries_.pop_back();
        return Status::OK;
    }

    [[nodiscard]] LogIndex lastIndex() const override {
        return entries_.size();
    }

    [[nodiscard]] Term lastTerm() const override {
        return entries_.empty() ? kNoTerm : entries_[entries_.size() - 1].term;
    }

  private:
    Vector<LogEntry> entries_;
};

// Trivial in-memory PersistentState: no fsync, no durability across a
// real restart -- currentTerm/votedFor simply live as members for the
// process's lifetime. A "restart" test that wants real crash-recovery
// semantics needs to keep the SAME instance across the simulated
// restart (construct a new NodeState/RaftNode on top of it), not a
// fresh one -- a fresh instance means nothing persisted survived.
class InMemoryPersistentState final : public PersistentState {
  public:
    [[nodiscard]] Status setCurrentTerm(Term term) override {
        currentTerm_ = term;
        return Status::OK;
    }

    [[nodiscard]] Term currentTerm() const override {
        return currentTerm_;
    }

    [[nodiscard]] Status setVotedFor(std::optional<NodeId> candidate) override {
        votedFor_ = std::move(candidate);
        return Status::OK;
    }

    [[nodiscard]] std::optional<NodeId> votedFor() const override {
        return votedFor_;
    }

  private:
    Term currentTerm_ = kNoTerm;
    std::optional<NodeId> votedFor_;
};

// Deterministic RandomSource: returns a pre-scripted sequence instead of
// anything actually random, so a test can force a specific interleaving
// (e.g. two nodes' election timeouts landing on the same tick).
class ScriptedRandomSource final : public RandomSource {
  public:
    explicit ScriptedRandomSource(Vector<int> script) : script_(std::move(script)) {}

    /// @attention Ignores minInclusive/maxInclusive once the script has a
    /// value queued -- it's the test's job to script sensible values.
    int nextInt(int minInclusive, int /*maxInclusive*/) override {
        if (nextIndex_ < script_.size())
            return script_[nextIndex_++];
        return minInclusive; // Script exhausted -- still deterministic.
    }

  private:
    Vector<int> script_;
    std::size_t nextIndex_ = 0;
};

// Deterministic in-process Transport: nothing is delivered until a test
// explicitly asks for it, via pump()/pumpNextTo()/dropPendingTo() --
// matching RaftCore's own tick()-driven determinism philosophy. An
// unregistered target is a built-in way to simulate an unreachable or
// partitioned peer.
class SimulatedTransport final : public Transport {
  public:
    using RequestVoteHandler = Function<Status(const RequestVoteArgs&, RequestVoteReply&)>;
    using AppendEntriesHandler = Function<Status(const AppendEntriesArgs&, AppendEntriesReply&)>;

    /// @brief Registers (or replaces) the handlers a message to `id` is delivered to.
    void registerNode(NodeId id, RequestVoteHandler onRequestVote,
                      AppendEntriesHandler onAppendEntries) {
        requestVoteHandlers_[id] = std::move(onRequestVote);
        appendEntriesHandlers_[id] = std::move(onAppendEntries);
    }

    /// @brief Removes `id`'s handlers -- every send to it now simulates unreachable.
    /// @details Reversible: registerNode() again to simulate the partition healing.
    void unregisterNode(const NodeId& id) {
        (void)requestVoteHandlers_.erase(id);
        (void)appendEntriesHandlers_.erase(id);
    }

    [[nodiscard]] Status
    sendRequestVote(const NodeId& target, RequestVoteArgs args,
                    MoveOnlyFunction<void(RequestVoteReply)> onReply) override {
        if (!requestVoteHandlers_.contains(target))
            return Status::NOT_FOUND; // Simulates an unreachable/partitioned peer.
        pendingRequestVotes_.push_back(
            PendingRequestVote{target, std::move(args), std::move(onReply)});
        return Status::OK;
    }

    [[nodiscard]] Status
    sendAppendEntries(const NodeId& target, AppendEntriesArgs args,
                      MoveOnlyFunction<void(AppendEntriesReply)> onReply) override {
        if (!appendEntriesHandlers_.contains(target))
            return Status::NOT_FOUND;
        pendingAppendEntries_.push_back(
            PendingAppendEntries{target, std::move(args), std::move(onReply)});
        return Status::OK;
    }

    /**
     * @brief Delivers only the oldest still-pending message addressed to `target`.
     * @return `true` if a message was delivered, `false` if none was pending for it.
     * @details Lets a test choreograph a specific interleaving across
     * multiple targets (e.g. "let C see candidate A's request before
     * candidate B's, but let D see B's first") -- something plain pump()
     * can't express, since it drains everything in one flat FIFO pass.
     */
    bool pumpNextTo(const NodeId& target) {
        if (auto it = findOldestTo(pendingRequestVotes_, target);
            it != pendingRequestVotes_.end()) {
            if (requestVoteHandlers_.contains(target)) {
                RequestVoteReply reply;
                if (requestVoteHandlers_[target](it->args, reply) == Status::OK)
                    it->onReply(std::move(reply));
            }
            (void)pendingRequestVotes_.erase(it);
            return true;
        }
        if (auto it = findOldestTo(pendingAppendEntries_, target);
            it != pendingAppendEntries_.end()) {
            if (appendEntriesHandlers_.contains(target)) {
                AppendEntriesReply reply;
                if (appendEntriesHandlers_[target](it->args, reply) == Status::OK)
                    it->onReply(std::move(reply));
            }
            (void)pendingAppendEntries_.erase(it);
            return true;
        }
        return false;
    }

    /**
     * @brief Discards (never delivers) the oldest still-pending message addressed to `target`.
     * @return `true` if a message was discarded, `false` if none was pending for it.
     * @details Models a message lost or arbitrarily delayed past
     * relevance -- e.g. forcing a specific candidate's RequestVote to
     * "lose the race" to a particular follower, to engineer a split
     * vote deterministically rather than hoping for one.
     */
    bool dropPendingTo(const NodeId& target) {
        if (auto it = findOldestTo(pendingRequestVotes_, target);
            it != pendingRequestVotes_.end()) {
            (void)pendingRequestVotes_.erase(it);
            return true;
        }
        if (auto it = findOldestTo(pendingAppendEntries_, target);
            it != pendingAppendEntries_.end()) {
            (void)pendingAppendEntries_.erase(it);
            return true;
        }
        return false;
    }

    /**
     * @brief Delivers every message queued as of this call, in FIFO order.
     * @details A handler invoked while delivering can itself queue more
     * messages (e.g. a reply that triggers the next heartbeat) -- those
     * are left for the next pump() call, not delivered within this one.
     */
    void pump() {
        Vector<PendingRequestVote> requestVotes = std::move(pendingRequestVotes_);
        pendingRequestVotes_.clear();
        Vector<PendingAppendEntries> appendEntries = std::move(pendingAppendEntries_);
        pendingAppendEntries_.clear();

        for (std::size_t i = 0; i < requestVotes.size(); ++i) {
            PendingRequestVote& pending = requestVotes[i];
            if (!requestVoteHandlers_.contains(pending.target))
                continue; // Unregistered since send -- simulated crash/partition.
            RequestVoteReply reply;
            if (requestVoteHandlers_[pending.target](pending.args, reply) == Status::OK)
                pending.onReply(std::move(reply));
        }
        for (std::size_t i = 0; i < appendEntries.size(); ++i) {
            PendingAppendEntries& pending = appendEntries[i];
            if (!appendEntriesHandlers_.contains(pending.target))
                continue;
            AppendEntriesReply reply;
            if (appendEntriesHandlers_[pending.target](pending.args, reply) == Status::OK)
                pending.onReply(std::move(reply));
        }
    }

  private:
    struct PendingRequestVote {
        NodeId target;
        RequestVoteArgs args;
        MoveOnlyFunction<void(RequestVoteReply)> onReply;
    };
    struct PendingAppendEntries {
        NodeId target;
        AppendEntriesArgs args;
        MoveOnlyFunction<void(AppendEntriesReply)> onReply;
    };

    /// @brief Finds the first (oldest) entry in `pending` targeting `target`.
    template <typename PendingVector>
    static typename PendingVector::iterator findOldestTo(PendingVector& pending,
                                                         const NodeId& target) {
        for (auto it = pending.begin(); it != pending.end(); ++it) {
            if (it->target == target)
                return it;
        }
        return pending.end();
    }

    HashMap<NodeId, RequestVoteHandler> requestVoteHandlers_;
    HashMap<NodeId, AppendEntriesHandler> appendEntriesHandlers_;
    Vector<PendingRequestVote> pendingRequestVotes_;
    Vector<PendingAppendEntries> pendingAppendEntries_;
};

// A Storage where every fallible method fails, for testing that a
// failure actually propagates up through NodeState/RaftNode rather than
// being silently swallowed. Not meant to model any realistic failure
// mode (a real Storage fails intermittently, not universally) -- just a
// reliable way to force the failure path.
class FailingStorage final : public Storage {
  public:
    [[nodiscard]] Status append(Term, Vector<std::uint8_t>, LogIndex&) override {
        return Status::IO_ERROR;
    }
    [[nodiscard]] Status entryAt(LogIndex, LogEntry&) const override {
        return Status::IO_ERROR;
    }
    [[nodiscard]] Status range(LogIndex, LogIndex, Vector<LogEntry>&) const override {
        return Status::IO_ERROR;
    }
    [[nodiscard]] Status truncateFrom(LogIndex) override {
        return Status::IO_ERROR;
    }
    [[nodiscard]] LogIndex lastIndex() const override {
        return kNoIndex;
    }
    [[nodiscard]] Term lastTerm() const override {
        return kNoTerm;
    }
};

// A PersistentState where every fallible method fails, for the same
// reason as FailingStorage above. currentTerm()/votedFor() aren't
// fallible, so they just report the (never-successfully-set) defaults.
class FailingPersistentState final : public PersistentState {
  public:
    [[nodiscard]] Status setCurrentTerm(Term) override {
        return Status::IO_ERROR;
    }
    [[nodiscard]] Term currentTerm() const override {
        return kNoTerm;
    }
    [[nodiscard]] Status setVotedFor(std::optional<NodeId>) override {
        return Status::IO_ERROR;
    }
    [[nodiscard]] std::optional<NodeId> votedFor() const override {
        return std::nullopt;
    }
};

} // namespace RaftCore::Test
