/**
 * @file            StateMachine.h
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
#include <RaftCore/Common/Types.h>   // LogIndex
#include <VectorPro/Vector.h> // payload bytes
// clang-format on

// Interface committed log entries are applied to. Added on top of the
// roadmap's original two abstractions (Storage, Transport): Phase 5
// (client interaction) is unimplementable without somewhere for
// committed entries to go. The real app implements this over whatever
// it's actually replicating; tests implement it as a trivial recorder
// that just remembers what was applied, in order.

namespace RaftCore {

using namespace VectorPro;

/**
 * @brief Receives committed entries, strictly in log order, exactly once.
 * @attention RaftCore guarantees apply() is called for index N only
 * after every index < N has already been applied, and only once per
 * index -- including across a restart (RaftCore tracks lastApplied
 * itself; a StateMachine implementation must not need to deduplicate).
 * Deliberately `void`, not `Status`-returning: a failing state-machine
 * apply is not something Raft's consensus layer can react to or retry
 * (the entry is already committed cluster-wide) -- it is fatal to the
 * process, unlike the recoverable I/O failures Storage/PersistentState/
 * Transport report via Status.
 */
class StateMachine {
  public:
    virtual ~StateMachine() = default;

    virtual void apply(LogIndex index, const Vector<std::uint8_t>& payload) = 0;
};

} // namespace RaftCore
