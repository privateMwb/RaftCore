/**
 * @file            PersistentState.h
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
#include <RaftCore/Common/Status.h> // Status
#include <RaftCore/Common/Types.h>  // Term, NodeId
#include <optional>                 // votedFor's "haven't voted this term" case
// clang-format on

// Durable storage for currentTerm and votedFor. Kept separate from
// Storage: the Raft paper's Figure 2 lists currentTerm/votedFor/log as
// the full persistent state, but the log has different access patterns
// (append/range/truncate) than these two scalars (read-modify-fsync-
// return). Folding them into Storage would force every implementation
// to also be a key-value store.

namespace RaftCore {

/**
 * @brief currentTerm and votedFor, durable across restarts and crashes.
 * @attention Both setters must be durable (fsync'd) before returning
 * `Status::OK`. RaftCore updates persistent state and then replies to
 * the RPC, in that order, trusting the write already survived a crash by
 * the time the reply is sent. A non-durable implementation silently
 * breaks Raft's safety properties (a node could re-grant a vote after
 * crashing and forgetting it already voted this term).
 */
class PersistentState {
  public:
    virtual ~PersistentState() = default;

    /// @return `Status::OK` on success, `Status::IO_ERROR` on failure.
    [[nodiscard]] virtual Status setCurrentTerm(Term term) = 0;

    virtual Term currentTerm() const = 0;

    /**
     * @param candidate Empty optional means "no vote cast this term".
     * @return `Status::OK` on success, `Status::IO_ERROR` on failure.
     */
    [[nodiscard]] virtual Status setVotedFor(std::optional<NodeId> candidate) = 0;

    virtual std::optional<NodeId> votedFor() const = 0;
};

} // namespace RaftCore
