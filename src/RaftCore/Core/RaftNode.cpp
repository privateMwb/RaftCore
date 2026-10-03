/**
 * @file RaftNode.cpp
 * @brief RaftNode implementation.
 *
 * Contains the implementation of RaftNode's construction, the election
 * timer and heartbeat interval, the RequestVote and AppendEntries RPC
 * handlers, reply handling / vote counting toward becoming Leader, and
 * leader-side replication / commitIndex advancement.
 */

// ============================================================
// Implementation for RaftCore::RaftNode.
// ============================================================
//
//  Sections:
//   1. Construction & Accessors
//   2. Election Timer & Heartbeat
//   3. RequestVote — Receiving
//   4. RequestVote — Replies & Vote Counting
//   5. AppendEntries — Receiving
//   6. AppendEntries — Leader Replication, Replies & Commit
//
// ============================================================

// clang-format off
#include "RaftCore/Core/RaftNode.h" // RaftNode — the class this file implements

#include <algorithm> // std::min — capping commitIndex to the last newly-known-replicated entry
#include <cstddef>   // std::size_t — loop indices and vote/replication-counting arithmetic
#include <utility>   // std::move — moving the peers Vector and entry payloads into place
// clang-format on

namespace RaftCore {

namespace {

// ============================================================
//  Election restriction (Figure 2, RequestVote receiver rule #2)
// ============================================================
//
// "If votedFor is null or candidateId, and candidate's log is at least
// as up-to-date as receiver's log, grant vote." This is the safety
// property that prevents a candidate lacking a committed entry from
// ever becoming leader -- Phase 4 tests this behavior, it doesn't
// introduce it.

bool isCandidateLogUpToDate(Term candidateLastLogTerm, LogIndex candidateLastLogIndex,
                            Term ourLastLogTerm, LogIndex ourLastLogIndex) {
    if (candidateLastLogTerm != ourLastLogTerm)
        return candidateLastLogTerm > ourLastLogTerm;

    return candidateLastLogIndex >= ourLastLogIndex;
}

} // namespace

// ============================================================
//  Section 1 — Construction & Accessors
// ============================================================

RaftNode::RaftNode(NodeState& state, Transport& transport, RandomSource& randomSource,
                   Vector<NodeId> peers, int electionTimeoutMinTicks, int electionTimeoutMaxTicks,
                   int heartbeatIntervalTicks)
    : state_(state), transport_(transport), randomSource_(randomSource), peers_(std::move(peers)),
      electionTimeoutMinTicks_(electionTimeoutMinTicks),
      electionTimeoutMaxTicks_(electionTimeoutMaxTicks),
      heartbeatIntervalTicks_(heartbeatIntervalTicks) {
    resetElectionTimer();
}

NodeState& RaftNode::state() noexcept {
    return state_;
}

const NodeState& RaftNode::state() const noexcept {
    return state_;
}

std::optional<Status> RaftNode::takeLastAsyncError() noexcept {
    std::optional<Status> error = lastAsyncError_;
    lastAsyncError_.reset();
    return error;
}

// ============================================================
//  Section 2 — Election Timer & Heartbeat
// ============================================================

void RaftNode::resetElectionTimer() {
    ticksSinceReset_ = 0;
    currentElectionTimeoutTicks_ =
        randomSource_.nextInt(electionTimeoutMinTicks_, electionTimeoutMaxTicks_);
}

Status RaftNode::startNewElection() {
    if (Status status = state_.startElection(); status != Status::OK) {
        // Term/vote/role are all unchanged on failure (see
        // NodeState::startElection()) -- still re-roll the timer so a
        // persistent fsync failure doesn't turn into a tight retry loop
        // on every single tick.
        resetElectionTimer();
        return status;
    }

    resetElectionTimer();
    votesReceivedFrom_.clear();

    RequestVoteArgs args;
    args.term = state_.currentTerm();
    args.candidateId = state_.selfId();
    args.lastLogIndex = state_.log().lastIndex();
    args.lastLogTerm = state_.log().lastTerm();

    for (std::size_t i = 0; i < peers_.size(); ++i) {
        const NodeId& peer = peers_[i];

        // A dispatch failure to one peer (e.g. an unknown target) isn't
        // fatal to the election -- that peer's vote just never arrives,
        // and the randomized timeout already covers the case where not
        // every peer is reachable. Not surfaced through this method's
        // Status, which is reserved for the persistence failure above.
        // Capturing a pointer rather than a copy of `peer`: FunctionPro
        // requires the stored callable to be nothrow-move-constructible
        // (MoveOnlyFunction.h), and a raw pointer trivially satisfies
        // that regardless of NodeId's own move-constructor guarantees.
        // Safe because `peer` aliases an element of `peers_`, which this
        // RaftNode never reallocates after construction, and callbacks
        // already require this RaftNode (and so `peers_`) to outlive them.
        (void)transport_.sendRequestVote(peer, args,
                                         [this, peerPtr = &peer](RequestVoteReply reply) {
                                             handleRequestVoteReply(*peerPtr, reply);
                                         });
    }

    return Status::OK;
}

Status RaftNode::observeTermAndResetTimerIfStale(Term rpcTerm) {
    const Term termBefore = state_.currentTerm();

    if (Status status = state_.observeTerm(rpcTerm); status != Status::OK)
        return status;

    if (state_.currentTerm() > termBefore)
        resetElectionTimer();

    return Status::OK;
}

Status RaftNode::becomeLeaderAndStartReplicating() {
    state_.becomeLeader(peers_);
    ticksSinceHeartbeat_ = 0;

    // The Figure 8 fix: a leader only ever directly commits an entry
    // from its own current term, so it needs one before anything it
    // inherited can become safely committable.
    LogIndex noOpIndex;
    if (Status status =
            state_.log().append(state_.currentTerm(), Vector<std::uint8_t>{}, noOpIndex);
        status != Status::OK)
        return status;

    return replicateToAllPeers();
}

Status RaftNode::tick() {
    if (state_.role() == Role::Leader) {
        ++ticksSinceHeartbeat_;

        if (ticksSinceHeartbeat_ < heartbeatIntervalTicks_)
            return Status::OK;

        ticksSinceHeartbeat_ = 0;
        return replicateToAllPeers();
    }

    ++ticksSinceReset_;

    if (ticksSinceReset_ < currentElectionTimeoutTicks_)
        return Status::OK;

    // Covers both Followers timing out waiting for a leader, and
    // Candidates timing out waiting for a majority (the split-vote
    // case) -- Figure 2 applies the same rule to both.
    return startNewElection();
}

// ============================================================
//  Section 3 — RequestVote — Receiving
// ============================================================

Status RaftNode::handleRequestVote(const RequestVoteArgs& args, RequestVoteReply& outReply) {
    if (args.term < state_.currentTerm()) {
        // Stale candidate -- Figure 2's rule #1. Not an error; just no vote.
        outReply.term = state_.currentTerm();
        outReply.voteGranted = false;
        return Status::OK;
    }

    if (Status status = observeTermAndResetTimerIfStale(args.term); status != Status::OK)
        return status;

    const std::optional<NodeId> votedFor = state_.votedFor();
    const bool alreadyVotedForSomeoneElse = votedFor.has_value() && *votedFor != args.candidateId;
    const bool candidateIsUpToDate = isCandidateLogUpToDate(
        args.lastLogTerm, args.lastLogIndex, state_.log().lastTerm(), state_.log().lastIndex());

    if (!alreadyVotedForSomeoneElse && candidateIsUpToDate) {
        if (Status status = state_.grantVoteTo(args.candidateId); status != Status::OK)
            return status;

        // Granting a vote is itself evidence of a live candidate in the
        // cluster -- reset so this node doesn't immediately turn around
        // and compete with the very candidate it just voted for.
        resetElectionTimer();
        outReply.voteGranted = true;
    } else {
        outReply.voteGranted = false;
    }

    outReply.term = state_.currentTerm();
    return Status::OK;
}

// ============================================================
//  Section 4 — RequestVote — Replies & Vote Counting
// ============================================================

void RaftNode::handleRequestVoteReply(const NodeId& peer, const RequestVoteReply& reply) {
    if (Status status = observeTermAndResetTimerIfStale(reply.term); status != Status::OK) {
        lastAsyncError_ = status;
        return;
    }

    if (state_.role() != Role::Candidate)
        return; // Already moved on (won, stepped down, or a newer election started).

    if (reply.term != state_.currentTerm())
        return; // Reply from a since-abandoned election.

    if (!reply.voteGranted)
        return;

    if (votesReceivedFrom_.contains(peer))
        return; // Already counted -- a retried RPC, most likely.

    votesReceivedFrom_.insert(peer, true);

    // +1 for this node's own vote, cast in startNewElection().
    const std::size_t votesGranted = votesReceivedFrom_.size() + 1;
    const std::size_t clusterSize = peers_.size() + 1;
    const std::size_t majority = clusterSize / 2 + 1;

    if (votesGranted >= majority) {
        if (Status status = becomeLeaderAndStartReplicating(); status != Status::OK)
            lastAsyncError_ = status;
    }
}

// ============================================================
//  Section 5 — AppendEntries — Receiving
// ============================================================

Status RaftNode::handleAppendEntries(AppendEntriesArgs args, AppendEntriesReply& outReply) {
    if (args.term < state_.currentTerm()) {
        // Stale leader -- Figure 2's rule #1. Not an error; just rejected.
        outReply.term = state_.currentTerm();
        outReply.success = false;
        outReply.matchIndex = kNoIndex;
        return Status::OK;
    }

    if (Status status = observeTermAndResetTimerIfStale(args.term); status != Status::OK)
        return status;

    // A valid AppendEntries from the current (or newly-adopted) term's
    // leader is itself a heartbeat -- reset unconditionally, not just
    // when observeTermAndResetTimerIfStale() already did because the
    // term changed. Also step down if this node was competing (Figure
    // 2: "AppendEntries received from new leader: convert to follower").
    resetElectionTimer();
    if (state_.role() != Role::Follower)
        state_.stepDownToFollower();

    // Log consistency check (Figure 2 receiver rule #2).
    if (args.prevLogIndex != kNoIndex) {
        LogEntry previousEntry;
        Status readStatus = state_.log().entryAt(args.prevLogIndex, previousEntry);

        if (readStatus == Status::NOT_FOUND ||
            (readStatus == Status::OK && previousEntry.term != args.prevLogTerm)) {
            outReply.term = state_.currentTerm();
            outReply.success = false;
            outReply.matchIndex = state_.log().lastIndex();
            return Status::OK;
        }
        if (readStatus != Status::OK)
            return readStatus; // A real storage failure, not just a mismatch.
    }

    // Reconcile: find the first new entry that's missing or conflicts
    // with what's already logged, truncate from there, and append
    // everything from that point on (Figure 2 receiver rules #3-4).
    // Entries that already match exactly are left alone, so a retried
    // RPC doesn't re-append what's already durable.
    for (std::size_t i = 0; i < args.entries.size(); ++i) {
        const LogIndex index = args.prevLogIndex + 1 + i;
        LogEntry existingEntry;
        Status readStatus = state_.log().entryAt(index, existingEntry);

        if (readStatus == Status::OK && existingEntry.term == args.entries[i].term)
            continue;

        if (readStatus == Status::OK) {
            if (Status status = state_.log().truncateFrom(index); status != Status::OK)
                return status;
        } else if (readStatus != Status::NOT_FOUND) {
            return readStatus; // A real storage failure.
        }

        for (std::size_t j = i; j < args.entries.size(); ++j) {
            LogIndex appendedIndex;
            Status status = state_.log().append(args.entries[j].term,
                                                std::move(args.entries[j].payload), appendedIndex);
            if (status != Status::OK)
                return status;
        }
        break;
    }

    const LogIndex lastNewEntry = static_cast<LogIndex>(args.prevLogIndex + args.entries.size());
    // Figure 2 receiver rule #5. commitIndex never moves backward: a
    // heartbeat's lastNewEntry can sit below what's already committed.
    const LogIndex newCommit = std::min(args.leaderCommit, lastNewEntry);
    if (newCommit > state_.commitIndex())
        state_.setCommitIndex(newCommit);

    outReply.term = state_.currentTerm();
    outReply.success = true;
    outReply.matchIndex = lastNewEntry;
    return Status::OK;
}

// ============================================================
//  Section 6 — AppendEntries — Leader Replication, Replies & Commit
// ============================================================

Status RaftNode::replicateTo(const NodeId& peer) {
    const LogIndex nextIndex = state_.leaderState().nextIndex[peer];
    const LogIndex prevLogIndex = nextIndex - 1;

    Term prevLogTerm = kNoTerm;
    if (prevLogIndex != kNoIndex) {
        LogEntry previousEntry;
        if (Status status = state_.log().entryAt(prevLogIndex, previousEntry); status != Status::OK)
            return status; // Our own log should always have this -- a real failure.
        prevLogTerm = previousEntry.term;
    }

    AppendEntriesArgs args;
    args.term = state_.currentTerm();
    args.leaderId = state_.selfId();
    args.prevLogIndex = prevLogIndex;
    args.prevLogTerm = prevLogTerm;
    args.leaderCommit = state_.commitIndex();

    if (nextIndex <= state_.log().lastIndex()) {
        if (Status status = state_.log().range(nextIndex, state_.log().lastIndex(), args.entries);
            status != Status::OK)
            return status;
    }

    // A dispatch failure to this one peer isn't fatal -- same reasoning
    // as startNewElection()'s per-peer sends. Retried on the next
    // heartbeat regardless.
    // Same reasoning as startNewElection()'s RequestVote callback: capture
    // a stable pointer into peers_, not a copy of `peer`, so the closure
    // is trivially nothrow-move-constructible regardless of NodeId's own
    // move-constructor guarantees.
    (void)transport_.sendAppendEntries(peer, args,
                                       [this, peerPtr = &peer](AppendEntriesReply reply) {
                                           handleAppendEntriesReply(*peerPtr, reply);
                                       });

    return Status::OK;
}

Status RaftNode::replicateToAllPeers() {
    Status firstFailure = Status::OK;

    for (std::size_t i = 0; i < peers_.size(); ++i) {
        if (Status status = replicateTo(peers_[i]);
            status != Status::OK && firstFailure == Status::OK)
            firstFailure = status;
    }

    return firstFailure;
}

void RaftNode::tryAdvanceCommitIndex() {
    if (state_.role() != Role::Leader)
        return;

    const LogIndex lastIndex = state_.log().lastIndex();
    const std::size_t clusterSize = peers_.size() + 1;
    const std::size_t majority = clusterSize / 2 + 1;

    for (LogIndex candidate = lastIndex; candidate > state_.commitIndex(); --candidate) {
        LogEntry entry;
        if (state_.log().entryAt(candidate, entry) != Status::OK)
            continue; // Shouldn't happen for an index within our own log; skip defensively.

        // Figure 2's safety rule: only ever directly commit an entry
        // from this node's own current term. Entries from earlier terms
        // become safely committed as a side effect, via the log
        // matching property, once a later same-term entry is.
        if (entry.term != state_.currentTerm())
            continue;

        std::size_t replicatedCount = 1; // This leader's own log counts as replicated to itself.
        for (std::size_t i = 0; i < peers_.size(); ++i) {
            const NodeId& peer = peers_[i];
            if (state_.leaderState().matchIndex.contains(peer) &&
                state_.leaderState().matchIndex[peer] >= candidate)
                ++replicatedCount;
        }

        if (replicatedCount >= majority) {
            // Scanning downward means this is the highest qualifying
            // index -- take it and stop.
            state_.setCommitIndex(candidate);
            return;
        }
    }
}

void RaftNode::handleAppendEntriesReply(const NodeId& peer, const AppendEntriesReply& reply) {
    if (Status status = observeTermAndResetTimerIfStale(reply.term); status != Status::OK) {
        lastAsyncError_ = status;
        return;
    }

    if (state_.role() != Role::Leader)
        return; // Stepped down since sending, or a newer election happened.

    if (reply.term != state_.currentTerm())
        return; // Reply from a since-abandoned term.

    if (reply.success) {
        state_.leaderState().matchIndex[peer] = reply.matchIndex;
        state_.leaderState().nextIndex[peer] = reply.matchIndex + 1;
        tryAdvanceCommitIndex();
        return;
    }

    // Log-consistency check failed on the follower's side -- back off
    // by one and retry immediately, rather than waiting for the next
    // heartbeat. Never below 1: prevLogIndex 0 (kNoIndex) is already
    // the most conservative possible starting point.
    LogIndex& nextIndex = state_.leaderState().nextIndex[peer];
    if (nextIndex > 1)
        --nextIndex;

    (void)replicateTo(peer); // Same non-fatal-dispatch-failure reasoning as elsewhere.
}

} // namespace RaftCore
