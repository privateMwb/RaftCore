/**
 * @file            RaftNode.h
 *
 * @date            2026-9-29
 *
 * @version         0.1.0
 *
 * @copyright       Copyright (c) 2026 privateMWB
 *                  All rights reserved.
 *
 * @attention       This source is released under the MIT license
 *                  SPDX-License-Identifier: MIT
 *                  <http://opensource.org/licenses/MIT>
 */

#pragma once

// clang-format off
#include <RaftCore/Common/Status.h>       // Status
#include <RaftCore/Common/Types.h>        // Term, LogIndex, NodeId
#include <RaftCore/Core/NodeState.h>      // NodeState
#include <RaftCore/Interfaces/RandomSource.h> // RandomSource
#include <RaftCore/Interfaces/Transport.h>    // Transport, RequestVoteArgs/Reply
#include <HashMapPro/HashMap.h>           // votesReceivedFrom_
#include <VectorPro/Vector.h>             // peers_
#include <optional>                       // takeLastAsyncError()'s return
// clang-format on

// Phase 2: leader election. Phase 3: log replication and commitment.
// Owns nothing Phase 0/1 didn't already define -- NodeState for state,
// Transport for RPCs, RandomSource for timeout jitter -- and adds the
// orchestration: a tick()-driven election timeout and heartbeat
// interval (per the roadmap's tick-not-clock decision from Phase 0),
// the RequestVote/AppendEntries RPC handlers (including the election
// restriction and the log-consistency/conflict-resolution rules --
// Phase 4 verifies these behaviors with tests, it does not introduce
// them), counting RequestVote replies toward a majority to become
// Leader, and counting AppendEntries replies toward advancing
// commitIndex. On election, appends a no-op entry before the first
// heartbeat (the Figure 8 fix) so entries from earlier terms can
// eventually be committed safely. Does not yet drain committed entries
// to a StateMachine -- that's Phase 5's client-interaction surface.

namespace RaftCore {

using namespace HashMapPro;
using namespace VectorPro;

/**
 * @brief Drives one Raft node's election and replication behavior via
 * tick() and RPC handlers.
 * @attention Single-threaded by design: tick() and the handle*() methods
 * are meant to be called from one thread (the app's own event loop),
 * matching the Phase 0 decision to make RaftCore tick-driven rather than
 * spawn its own timer threads. Nothing here is safe to call concurrently
 * from multiple threads.
 */
class RaftNode {
  public:
    /**
     * @param state This node's state container. Must outlive the RaftNode.
     * @param transport RPC transport. Must outlive the RaftNode.
     * @param randomSource Timeout jitter source. Must outlive the RaftNode.
     * @param peers The other cluster members, not including `state.selfId()`.
     * @param electionTimeoutMinTicks Inclusive lower bound on the randomized
     * election timeout, in units of however often the caller intends to call tick().
     * @param electionTimeoutMaxTicks Inclusive upper bound. Must be `>= electionTimeoutMinTicks`.
     * @param heartbeatIntervalTicks How often, in ticks, a Leader resends
     * AppendEntries to every peer. Must be well under `electionTimeoutMinTicks`
     * on every other node in the cluster, or followers will time out between
     * heartbeats -- RaftCore has no way to enforce that across nodes it
     * doesn't know about; getting the ratio right is the caller's job.
     */
    RaftNode(NodeState& state, Transport& transport, RandomSource& randomSource,
             Vector<NodeId> peers, int electionTimeoutMinTicks, int electionTimeoutMaxTicks,
             int heartbeatIntervalTicks);

    /**
     * @brief Advances the election timeout, or the heartbeat interval, by one tick.
     * @return `Status::OK` unless starting a new election, becoming leader
     * (appending the no-op entry), or reading the log while sending a
     * heartbeat hit a persistence failure. On a failed election attempt,
     * the timeout is retried on a later tick, having already been
     * re-randomized. On a failed heartbeat, the interval simply retries
     * next tick -- no state has been corrupted by a failed read.
     * @details While this node is Leader: no election timeout (Figure 2)
     * -- instead, sends AppendEntries to every peer once the heartbeat
     * interval elapses. While Follower or Candidate: starts a new
     * election once the (randomized) election timeout elapses --
     * startNewElection() covers both the ordinary follower-timeout case
     * and a candidate's own split-vote timeout, per Figure 2.
     */
    [[nodiscard]] Status tick();

    /**
     * @brief Handles an incoming RequestVote RPC.
     * @param args The request, as received from `args.candidateId` over the transport.
     * @param outReply Set to this node's reply on `Status::OK`. Left
     * unspecified (do not send it) on any other Status.
     * @return `Status::OK` on success. Only a persistence failure (while
     * adopting a newer term, or while durably recording a granted vote)
     * returns anything else.
     */
    [[nodiscard]] Status handleRequestVote(const RequestVoteArgs& args, RequestVoteReply& outReply);

    /**
     * @brief Handles a reply to a RequestVote RPC this node previously sent.
     * @param peer Which peer replied.
     * @param reply The reply.
     * @details Stale replies are silently ignored: from a peer already
     * counted this election, from a term that no longer matches
     * currentTerm(), or received after this node is no longer Candidate.
     * On reaching a majority (including this node's own vote), becomes
     * Leader. A persistence failure while adopting a newer term the reply
     * carries is recorded rather than thrown away -- see takeLastAsyncError().
     */
    void handleRequestVoteReply(const NodeId& peer, const RequestVoteReply& reply);

    /**
     * @brief Handles an incoming AppendEntries RPC (heartbeat or replication).
     * @param args The request, as received from `args.leaderId` over the transport.
     * @param outReply Set to this node's reply on `Status::OK`. Left
     * unspecified (do not send it) on any other Status.
     * @return `Status::OK` on success. Only a persistence failure (while
     * adopting a newer term, reading the log for the consistency check,
     * truncating a conflicting suffix, or appending new entries) returns
     * anything else.
     * @details Implements Figure 2's AppendEntries receiver rules in
     * order: stale-term rejection; term adoption and stepping down to
     * Follower (a valid AppendEntries is itself a heartbeat, so the
     * election timer resets here regardless of whether the term
     * actually changed); the previous-entry consistency check; conflict
     * detection and truncation; appending whatever's left; and advancing
     * commitIndex to at most the last newly-known-replicated index.
     */
    [[nodiscard]] Status handleAppendEntries(AppendEntriesArgs args, AppendEntriesReply& outReply);

    /**
     * @brief Handles a reply to an AppendEntries RPC this node previously sent.
     * @param peer Which peer replied.
     * @param reply The reply.
     * @details On success, advances that peer's matchIndex/nextIndex and
     * attempts to advance commitIndex (only ever directly committing an
     * entry from this node's own current term -- Figure 2's safety
     * rule; earlier-term entries become safely committed as a side
     * effect once a later same-term entry is). On failure, backs
     * nextIndex off by one and retries immediately rather than waiting
     * for the next heartbeat. Stale replies (wrong term, no longer
     * Leader) are ignored the same way handleRequestVoteReply() ignores
     * its own stale replies.
     */
    void handleAppendEntriesReply(const NodeId& peer, const AppendEntriesReply& reply);

    /**
     * @brief Returns and clears the last error from an async reply-handling path.
     * @details handleRequestVoteReply() has no return channel of its own --
     * it is invoked from a Transport reply callback with a fixed `void(Reply)`
     * signature (Phase 0) -- so a persistence failure encountered there is
     * stashed here instead of being silently dropped. Callers that care
     * about surfacing such failures should poll this after driving the
     * event loop; callers that don't will simply never see anything here.
     */
    [[nodiscard]] std::optional<Status> takeLastAsyncError() noexcept;

    /// @brief This node's own state container, as given to the constructor.
    [[nodiscard]] NodeState& state() noexcept;
    [[nodiscard]] const NodeState& state() const noexcept;

  private:
    /// @brief Re-rolls the election timeout and zeroes the tick counter.
    void resetElectionTimer();

    /// @brief The Figure-2 "Candidates" rule: bump term, vote self, become Candidate.
    [[nodiscard]] Status startNewElection();

    /**
     * @brief The Figure-2 "All Servers" rule, plus this node's own timer reset.
     * @details Identical to NodeState::observeTerm() except it also resets
     * the election timer when (and only when) `rpcTerm` was actually newer
     * -- a node that just heard from a legitimately higher-term peer
     * shouldn't immediately start competing with it.
     */
    [[nodiscard]] Status observeTermAndResetTimerIfStale(Term rpcTerm);

    /**
     * @brief Transitions to Leader and starts replicating: initializes
     * per-peer state, appends a no-op entry, and sends the first round
     * of AppendEntries immediately.
     * @details The no-op-on-election step is the Figure 8 fix: a leader
     * only ever directly commits entries from its own current term, so
     * without an entry of its own, older entries it inherited could
     * remain forever uncommitted (or worse, get silently overwritten by
     * a future leader) even after being replicated to a majority.
     */
    [[nodiscard]] Status becomeLeaderAndStartReplicating();

    /**
     * @brief Sends this Leader's next AppendEntries to `peer`, based on
     * that peer's current nextIndex.
     * @return `Status::OK` once dispatch was attempted, regardless of
     * whether the transport actually managed to reach `peer` -- an
     * unreachable peer isn't fatal, the same as in startNewElection().
     * Only a failure to read this node's own log returns anything else.
     */
    [[nodiscard]] Status replicateTo(const NodeId& peer);

    /**
     * @brief Sends replicateTo() to every peer, e.g. on the heartbeat interval.
     * @return The first non-OK Status from replicateTo(), if any, else `Status::OK`.
     */
    [[nodiscard]] Status replicateToAllPeers();

    /**
     * @brief Advances commitIndex as far as a majority-plus-current-term rule allows.
     * @details Scans downward from the log's last index so the first (and
     * therefore highest) qualifying index found is used directly --
     * everything below it is safely implied by the log matching property,
     * per Figure 2's leader commitIndex rule. No-op if this node isn't Leader.
     */
    void tryAdvanceCommitIndex();

    NodeState& state_;
    Transport& transport_;
    RandomSource& randomSource_;
    Vector<NodeId> peers_;

    int electionTimeoutMinTicks_;
    int electionTimeoutMaxTicks_;
    int currentElectionTimeoutTicks_ = 0;
    int ticksSinceReset_ = 0;

    int heartbeatIntervalTicks_;
    int ticksSinceHeartbeat_ = 0;

    /// @brief Peers whose granted vote has already been counted this election.
    HashMap<NodeId, bool> votesReceivedFrom_;

    std::optional<Status> lastAsyncError_;
};

} // namespace RaftCore
