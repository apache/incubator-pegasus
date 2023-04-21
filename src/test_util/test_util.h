// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// These macros and functions are inspired by Apache Kudu.

#pragma once

#include <functional>

#include "utils/test_macros.h"

namespace pegasus {

#define ASSERT_EVENTUALLY(expr)                                                                    \
    do {                                                                                           \
        AssertEventually(expr);                                                                    \
        NO_PENDING_FATALS();                                                                       \
    } while (0)

#define ASSERT_IN_TIME(expr, sec)                                                                  \
    do {                                                                                           \
        AssertEventually(expr, sec);                                                               \
        NO_PENDING_FATALS();                                                                       \
    } while (0)

#define ASSERT_IN_TIME_WITH_FIXED_INTERVAL(expr, sec)                                              \
    do {                                                                                           \
        AssertEventually(expr, sec, WaitBackoff::NONE);                                            \
        NO_PENDING_FATALS();                                                                       \
    } while (0)

#define WAIT_IN_TIME(expr, sec)                                                                    \
    do {                                                                                           \
        WaitCondition(expr, sec);                                                                  \
        NO_PENDING_FATALS();                                                                       \
    } while (0)

// Wait until 'f()' succeeds without adding any GTest 'fatal failures'.
// For example:
//
//   AssertEventually([]() {
//     ASSERT_GT(ReadValueOfMetric(), 10);
//   });
//
// The function is run in a loop with optional back-off.
//
// To check whether AssertEventually() eventually succeeded, call
// NO_PENDING_FATALS() afterward, or use ASSERT_EVENTUALLY() which performs
// this check automatically.
enum class WaitBackoff
{
    // Use exponential back-off while looping, capped at one second.
    EXPONENTIAL,

    // Sleep for a millisecond while looping.
    NONE,
};
void AssertEventually(const std::function<void(void)> &f,
                      int timeout_sec = 30,
                      WaitBackoff backoff = WaitBackoff::EXPONENTIAL);

// Wait until 'f()' succeeds or timeout, there is no GTest 'fatal failures'
// regardless failed or timeout.
void WaitCondition(const std::function<bool(void)> &f,
                   int timeout_sec = 30,
                   WaitBackoff backoff = WaitBackoff::EXPONENTIAL);
} // namespace pegasus
