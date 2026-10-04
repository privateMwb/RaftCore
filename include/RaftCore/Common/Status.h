/**
 * @file            Status.h
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

// RaftCore's own Status type. RaftCore is not a MiniDB submodule
// consumer -- it must not depend on MiniDB::Common::Type.h, so every
// Storage/PersistentState/Transport failure mode is reported through
// this local enum instead.

namespace RaftCore {

enum class Status {
    OK,
    IO_ERROR,
    NOT_FOUND,
    OUT_OF_MEMORY,
    PARSE_ERROR,
};

} // namespace RaftCore
