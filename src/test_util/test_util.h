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

#include <fmt/core.h>
#include <gtest/gtest.h>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>

#include "runtime/api_layer1.h"
#include "utils/env.h"
// IWYU refused to include "utils/defer.h" everywhere, both in .h and .cpp files.
// However, once "utils/defer.h" is not included, it is inevitable that compilation
// will fail since dsn::defer is referenced. Thus force IWYU to keep it.
#include "utils/defer.h" // IWYU pragma: keep
#include "utils/flags.h"
#include "utils/test_macros.h"

namespace dsn {
namespace replication {
class file_meta;
} // namespace replication
} // namespace dsn

DSN_DECLARE_bool(encrypt_data_at_rest);

// Save the current value of a flag and restore it at the end of the function.
#define PRESERVE_FLAG(name)                                                                        \
    auto PRESERVED_FLAGS_##name = FLAGS_##name;                                                    \
    auto PRESERVED_FLAGS_##name##_cleanup =                                                        \
        dsn::defer([PRESERVED_FLAGS_##name]() { FLAGS_##name = PRESERVED_FLAGS_##name; })

namespace pegasus {

// A base parameterized test class for testing enable/disable encryption at rest.
class encrypt_data_test_base : public testing::TestWithParam<bool>
{
public:
    encrypt_data_test_base()
    {
        FLAGS_encrypt_data_at_rest = GetParam();
        // The size of an actual encrypted file should plus kEncryptionHeaderkSize bytes if consider
        // it as kNonSensitive.
        if (FLAGS_encrypt_data_at_rest) {
            _extra_encrypted_file_size = dsn::utils::kEncryptionHeaderkSize;
        }
    }

    uint64_t extra_encrypted_file_size() const { return _extra_encrypted_file_size; }

private:
    uint64_t _extra_encrypted_file_size = 0;
};

class stop_watch
{
public:
    stop_watch() { _start_ms = dsn_now_ms(); }
    void stop_and_output(const std::string &msg)
    {
        auto duration_ms =
            std::chrono::duration_cast<std::chrono::duration<double>>(
                std::chrono::milliseconds(static_cast<int64_t>(dsn_now_ms() - _start_ms)))
                .count();
        fmt::print(stdout, "{}, cost {} ms\n", msg, duration_ms);
    }

private:
    uint64_t _start_ms = 0;
};

void create_local_test_file(const std::string &full_name, dsn::replication::file_meta *fm);

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
        AssertEventually(expr, sec, ::pegasus::WaitBackoff::NONE);                                 \
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
