/**
 * @file            Transport.h
 *
 * @date            2026-9-29
 *
 * @version         1.0.0
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
#include <RaftCore/Common/Status.h>  // Status
#include <RaftCore/Common/Types.h>   // Term, LogIndex, NodeId, LogEntry
#include <FunctionPro/MoveOnlyFunction.h> // reply callbacks
#include <VectorPro/Vector.h> // AppendEntriesArgs::entries
// clang-format on

// RequestVote/AppendEntries messages and the RPC-transport interface.
// RaftCore never names FalconHTTP directly -- the real app binds a
// FalconHTTP-backed Transport; tests bind a simulated, deterministic one
// (Phase 7's partition/crash/split-vote/conflict tests all run on top of
// that simulated transport, never real sockets or sleeps).

namespace RaftCore {

using namespace FunctionPro;
using namespace VectorPro;

struct RequestVoteArgs {
    Term term = kNoTerm;
    NodeId candidateId;
    LogIndex lastLogIndex = kNoIndex;
    Term lastLogTerm = kNoTerm;
};

struct RequestVoteReply {
    Term term = kNoTerm;
    bool voteGranted = false;
};

struct AppendEntriesArgs {
    Term term = kNoTerm;
    NodeId leaderId;
    LogIndex prevLogIndex = kNoIndex;
    Term prevLogTerm = kNoTerm;
    Vector<LogEntry> entries;
    LogIndex leaderCommit = kNoIndex;
};

struct AppendEntriesReply {
    Term term = kNoTerm;
    bool success = false;
    /// Follower's resulting last-log-index on success; lets the leader
    /// advance nextIndex/matchIndex without a second round trip.
    LogIndex matchIndex = kNoIndex;
};

/**
 * @brief Sends RPCs to other nodes and delivers their replies asynchronously.
 * @attention Real networks don't block the caller for a reply, and a
 * simulated transport needs to be able to drop, delay, or reorder
 * replies for the Phase 7 partition tests -- hence callback-based, not a
 * blocking call-and-return signature. A target that never replies
 * (partitioned) simply never invokes its callback; RaftCore's own
 * election-timeout logic is what gives up waiting, not the transport.
 * The `Status` return is for immediate dispatch failure only (e.g. an
 * unknown target); the RPC's actual outcome travels in the reply.
 * @attention Callbacks are `MoveOnlyFunction`, not `Function`/`std::function`:
 * an implementation must hold onto `onReply` until the async reply
 * actually arrives (`FunctionRef` would dangle -- the caller's lambda is
 * typically a temporary that doesn't outlive this call), but there is no
 * reason to require it be copy-constructible along the way.
 */
class Transport {
  public:
    virtual ~Transport() = default;

    [[nodiscard]] virtual Status
    sendRequestVote(const NodeId& target, RequestVoteArgs args,
                    MoveOnlyFunction<void(RequestVoteReply)> onReply) = 0;

    [[nodiscard]] virtual Status
    sendAppendEntries(const NodeId& target, AppendEntriesArgs args,
                      MoveOnlyFunction<void(AppendEntriesReply)> onReply) = 0;
};

} // namespace RaftCore
