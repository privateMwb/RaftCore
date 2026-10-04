/**
 * @file            RaftCore.h
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
#include <RaftCore/Common/Status.h>          // Status
#include <RaftCore/Common/Types.h>           // Term, LogIndex, NodeId, LogEntry
#include <RaftCore/Interfaces/Storage.h>         // log storage interface
#include <RaftCore/Interfaces/PersistentState.h> // currentTerm/votedFor interface
#include <RaftCore/Interfaces/Transport.h>       // RPC messages + transport interface
#include <RaftCore/Interfaces/StateMachine.h>    // committed-entry sink interface
#include <RaftCore/Interfaces/RandomSource.h>    // election-timeout jitter interface
#include <RaftCore/Core/NodeState.h>             // role + persistent/volatile state container
#include <RaftCore/Core/RaftNode.h>              // election-driving orchestrator
// clang-format on

// Umbrella header for the RaftCore library. Phase 0 (abstractions),
// Phase 1 (NodeState), and Phase 2 (leader election) only --
// replication/safety logic and the client-facing surface land in later
// phases and get added here as they do.

namespace rain {
using namespace RaftCore;
} // namespace rain
