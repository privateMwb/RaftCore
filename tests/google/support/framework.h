#pragma once

// Single include that pulls in the entire test framework — suite
// files include only this to get the library under test, testing
// macros, output helpers, and suite registration.

#include <RaftCore/RaftCore.h> // the class under the test.

#include "reference.h" //