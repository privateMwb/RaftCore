/**
 * @file            Types.h
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
#include <VectorPro/Vector.h> // Vector
#include <cstdint>            // std::uint64_t, std::uint8_t
#include <string>             // NodeId
// clang-format on

// Core value types shared by every RaftCore interface (Storage,
// PersistentState, Transport, StateMachine). Phase 0 vocabulary only --
// no consensus logic lives here.

namespace RaftCore {

using namespace VectorPro;

using Term = std::uint64_t;
using LogIndex = std::uint64_t;

/// 0 is reserved: no term has occurred yet / no vote cast this term.
inline constexpr Term kNoTerm = 0;
/// Raft log indices are 1-based; 0 means "no entries yet".
inline constexpr LogIndex kNoIndex = 0;

/**
 * @brief Node identity as the Transport interface understands it.
 * @details Deliberately an opaque string rather than an int or a
 * FalconHTTP-specific address type -- keeps RaftCore decoupled from any
 * concrete transport. The real app's Transport implementation owns the
 * string -> endpoint mapping.
 */
using NodeId = std::string;

/**
 * @brief One log entry as RaftCore sees it.
 * @details `payload` is an opaque byte blob -- RaftCore never interprets
 * it, only stores/replicates/hands it to StateMachine::apply(). Uses
 * VectorPro::Vector<uint8_t>, the ecosystem's own dynamic array, in
 * place of std::vector.
 */
struct LogEntry {
    Term term = kNoTerm;
    Vector<std::uint8_t> payload;
};

} // namespace RaftCore
