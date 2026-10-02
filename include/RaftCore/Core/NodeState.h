/**
 * @file            NodeState.h
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
#include <RaftCore/Common/Status.h>          // Status
#include <RaftCore/Common/Types.h>           // Term, LogIndex, NodeId
#include <RaftCore/Interfaces/PersistentState.h> // PersistentState
#include <RaftCore/Interfaces/Storage.h>         // Storage
#include <HashMapPro/HashMap.h>              // nextIndex/matchIndex
#include <VectorPro/Vector.h>                // becomeLeader()'s peer list
#include <cstdint>                           // Role's underlying type
#include <optional>                          // leaderState_ only exists while Leader
// clang-format on

// Phase 1: Follower/Candidate/Leader role tracking, and the
// persistent-vs-volatile state split the Raft paper's Figure 2 lays
// out. No election or replication logic lives here -- this is the
// state container Phase 2 (election) and Phase 3 (replication) drive.

namespace RaftCore {

using namespace HashMapPro;
using namespace VectorPro;

/// @brief The three roles a Raft node can be in.
enum class Role : std::uint8_t {
    Follower,
    Candidate,
    Leader,
};

/**
 * @brief Leader-only volatile state (Figure 2's "Volatile state on leaders").
 * @details Reinitialized after every election, never persisted.
 */
struct LeaderVolatileState {
    /// @brief For each peer, index of the next log entry to send it.
    HashMap<NodeId, LogIndex> nextIndex;
    /// @brief For each peer, index of the highest entry known replicated to it.
    HashMap<NodeId, LogIndex> matchIndex;
};

/**
 * @brief Holds one node's role plus its persistent and volatile Raft state.
 * @attention Owns no storage itself -- `persistentState` and `storage` are
 * the Phase 0 interfaces this composes over, so NodeState stays testable
 * against in-memory fakes exactly like they are.
 */
class NodeState {
  public:
    /**
     * @param persistentState Durable currentTerm/votedFor store. Must outlive this NodeState.
     * @param storage Log storage. Must outlive this NodeState.
     * @param selfId This node's own identity, used when voting for itself on election.
     */
    NodeState(PersistentState& persistentState, Storage& storage, NodeId selfId);

    /// @brief This node's own identity, as given to the constructor.
    [[nodiscard]] const NodeId& selfId() const noexcept;

    /// @brief Current role. Starts as Follower.
    [[nodiscard]] Role role() const noexcept;

    /// @brief The log this node's Storage backs. Read/append/truncate go through here.
    [[nodiscard]] Storage& log() noexcept;
    [[nodiscard]] const Storage& log() const noexcept;

    // --- Persistent state (Figure 2: currentTerm, votedFor; the log itself is `log()`) ---

    [[nodiscard]] Term currentTerm() const;
    [[nodiscard]] std::optional<NodeId> votedFor() const;

    /**
     * @brief The "All Servers" rule: if `rpcTerm` is newer than ours, adopt it and step down.
     * @param rpcTerm Term seen on an incoming RPC or reply.
     * @return `Status::OK` whether or not `rpcTerm` was newer -- this is a
     * no-op, not a failure, when `rpcTerm <= currentTerm()`. Only a
     * genuine persistence failure while adopting a newer term returns
     * anything else.
     * @details On adopting a newer term: currentTerm is set to `rpcTerm`,
     * votedFor is cleared (a new term means no vote has been cast in it
     * yet), role becomes Follower, and any leader-only state is dropped.
     * This is the single place term/role stepdown happens -- Phase 2/3/4
     * call this rather than mutating currentTerm directly.
     */
    [[nodiscard]] Status observeTerm(Term rpcTerm);

    /**
     * @brief Records a vote cast for `candidate` in the current term.
     * @param candidate Peer being voted for.
     * @return `Status::OK` on success. Only a persistence failure returns
     * anything else -- on failure, no vote has been recorded.
     * @details Unlike observeTerm()/startElection(), this does not touch
     * currentTerm or role: it exists purely for RequestVote handling
     * (Phase 2), where a Follower grants a vote to a candidate already in
     * its own current term. Caller (RaftNode) is responsible for having
     * already checked the vote-granting rules -- this method does not
     * re-check whether a vote was already cast this term.
     */
    [[nodiscard]] Status grantVoteTo(NodeId candidate);

    /**
     * @brief Converts this node to Candidate and starts a new election term.
     * @return `Status::OK` on success. Only a persistence failure while
     * durably recording the term bump and self-vote returns anything else --
     * on failure, neither the term nor the vote nor the role have changed.
     * @details Per Figure 2's "Candidates" rules: increments currentTerm,
     * votes for `selfId()`, and sets role to Candidate. Does not send any
     * RequestVote RPCs or reset an election timer -- that orchestration is
     * Phase 2's, not this state container's.
     */
    [[nodiscard]] Status startElection();

    /**
     * @brief Converts this node to Leader and reinitializes leader-only state.
     * @param peers The other cluster members (not including `selfId()`).
     * @details nextIndex is set to `log().lastIndex() + 1` for every peer;
     * matchIndex is set to `kNoIndex` for every peer, per Figure 2.
     * @attention Caller's responsibility to only call this from Candidate
     * after actually winning an election (Phase 2) -- this method itself
     * does not check role() and does not re-verify the votes.
     */
    void becomeLeader(const Vector<NodeId>& peers);

    /**
     * @brief Steps down to Follower without a term change.
     * @details For the case where a candidate or leader discovers a valid
     * current leader (Figure 2's "AppendEntries received from new leader:
     * convert to follower"), which is a role change but not necessarily a
     * newer term -- unlike observeTerm(), which is for the term-based rule.
     * Drops leader-only state if this node was Leader.
     */
    void stepDownToFollower() noexcept;

    // --- Volatile state, all servers (Figure 2) ---

    [[nodiscard]] LogIndex commitIndex() const noexcept;

    /**
     * @param index New commit index.
     * @attention Precondition: `index >= commitIndex()`. commitIndex only
     * ever advances (Figure 2); enforcing that is Phase 3's job when it
     * computes the new value; this setter trusts its caller.
     */
    void setCommitIndex(LogIndex index) noexcept;

    [[nodiscard]] LogIndex lastApplied() const noexcept;

    /// @attention Same monotonic-only precondition as setCommitIndex(), and
    /// additionally `index <= commitIndex()` -- can't apply what isn't committed.
    void setLastApplied(LogIndex index) noexcept;

    // --- Volatile state, leaders only (Figure 2) ---

    /**
     * @return This node's leader-only replication-tracking state.
     * @attention Precondition: `role() == Role::Leader`. Calling this
     * otherwise is a caller bug, not a recoverable condition -- there is
     * no leader state to return when this node isn't the leader.
     */
    [[nodiscard]] LeaderVolatileState& leaderState() noexcept;
    [[nodiscard]] const LeaderVolatileState& leaderState() const noexcept;

  private:
    PersistentState& persistentState_;
    Storage& storage_;
    NodeId selfId_;
    Role role_ = Role::Follower;
    LogIndex commitIndex_ = kNoIndex;
    LogIndex lastApplied_ = kNoIndex;
    std::optional<LeaderVolatileState> leaderState_;
};

} // namespace RaftCore
