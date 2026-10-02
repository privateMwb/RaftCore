/**
 * @file NodeState.cpp
 * @brief NodeState implementation.
 *
 * Contains the implementation of NodeState's construction and identity,
 * persistent-state delegation and role transitions, and the
 * all-servers/leaders-only volatile state accessors.
 */

// ============================================================
// Implementation for RaftCore::NodeState.
// ============================================================
//
//  Sections:
//   1. Construction, Identity & Log Access
//   2. Persistent State & Role Transitions
//   3. Volatile State — All Servers
//   4. Volatile State — Leaders Only
//
// ============================================================

// clang-format off
#include <RaftCore/Core/NodeState.h> // NodeState — the class this file implements

#include <utility> // std::move — moving selfId_ in and the leader-state map into place
// clang-format on

namespace RaftCore {

// ============================================================
//  Section 1 — Construction, Identity & Log Access
// ============================================================

NodeState::NodeState(PersistentState& persistentState, Storage& storage, NodeId selfId)
    : persistentState_(persistentState), storage_(storage), selfId_(std::move(selfId)) {}

const NodeId& NodeState::selfId() const noexcept {
    return selfId_;
}

Role NodeState::role() const noexcept {
    return role_;
}

Storage& NodeState::log() noexcept {
    return storage_;
}

const Storage& NodeState::log() const noexcept {
    return storage_;
}

// ============================================================
//  Section 2 — Persistent State & Role Transitions
// ============================================================

Term NodeState::currentTerm() const {
    return persistentState_.currentTerm();
}

std::optional<NodeId> NodeState::votedFor() const {
    return persistentState_.votedFor();
}

Status NodeState::observeTerm(Term rpcTerm) {
    if (rpcTerm <= currentTerm())
        return Status::OK;

    if (Status status = persistentState_.setCurrentTerm(rpcTerm); status != Status::OK)
        return status;

    // A new term means no vote has been cast in it yet — clear before
    // reporting success, so a caller that observes OK never sees a
    // stale vote left over from the previous term.
    if (Status status = persistentState_.setVotedFor(std::nullopt); status != Status::OK)
        return status;

    role_ = Role::Follower;
    leaderState_.reset();
    return Status::OK;
}

Status NodeState::grantVoteTo(NodeId candidate) {
    return persistentState_.setVotedFor(std::move(candidate));
}

Status NodeState::startElection() {
    const Term newTerm = currentTerm() + 1;

    if (Status status = persistentState_.setCurrentTerm(newTerm); status != Status::OK)
        return status;

    if (Status status = persistentState_.setVotedFor(selfId_); status != Status::OK)
        return status;

    role_ = Role::Candidate;
    leaderState_.reset();
    return Status::OK;
}

void NodeState::becomeLeader(const Vector<NodeId>& peers) {
    LeaderVolatileState state;
    const LogIndex nextIndexForPeers = storage_.lastIndex() + 1;

    for (std::size_t i = 0; i < peers.size(); ++i) {
        const NodeId& peer = peers[i];
        state.nextIndex[peer] = nextIndexForPeers;
        state.matchIndex[peer] = kNoIndex;
    }

    leaderState_ = std::move(state);
    role_ = Role::Leader;
}

void NodeState::stepDownToFollower() noexcept {
    role_ = Role::Follower;
    leaderState_.reset();
}

// ============================================================
//  Section 3 — Volatile State — All Servers
// ============================================================

LogIndex NodeState::commitIndex() const noexcept {
    return commitIndex_;
}

void NodeState::setCommitIndex(LogIndex index) noexcept {
    commitIndex_ = index;
}

LogIndex NodeState::lastApplied() const noexcept {
    return lastApplied_;
}

void NodeState::setLastApplied(LogIndex index) noexcept {
    lastApplied_ = index;
}

// ============================================================
//  Section 4 — Volatile State — Leaders Only
// ============================================================

LeaderVolatileState& NodeState::leaderState() noexcept {
    return *leaderState_;
}

const LeaderVolatileState& NodeState::leaderState() const noexcept {
    return *leaderState_;
}

} // namespace RaftCore
