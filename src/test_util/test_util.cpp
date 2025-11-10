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

#include "test_util.h"

#include <gtest/gtest-spi.h>
#include <chrono>
#include <thread>

#include "gtest/gtest.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "runtime/api_layer1.h"
#include "utils/defer.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"

namespace pegasus {

void local_test_file::create(const std::string &path,
                             const std::string &content,
                             std::shared_ptr<local_test_file> &file)
{
    const auto status =
        rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive),
                                   rocksdb::Slice(content),
                                   path,
                                   /* should_sync */ true);
    ASSERT_TRUE(status.ok()) << status.ToString();

    dsn::replication::file_meta meta;
    meta.name = path;
    ASSERT_TRUE(
        dsn::utils::filesystem::file_size(path, dsn::utils::FileDataType::kSensitive, meta.size));
    ASSERT_EQ(dsn::ERR_OK, dsn::utils::filesystem::md5sum(path, meta.md5));

    file = std::shared_ptr<local_test_file>(new local_test_file(meta), deleter);
}

void local_test_file::create(const std::string &path, std::shared_ptr<local_test_file> &file)
{
    return create(path, "write some data.", file);
}

local_test_file::local_test_file(const dsn::replication::file_meta &meta) : _file_meta(meta) {}

local_test_file::~local_test_file()
{
    // We don't check whether returning ture, since the dir where the file is located may have
    // been removed.
    dsn::utils::filesystem::remove_path(_file_meta.name);
}

void AssertEventually(const std::function<void(void)> &f, int timeout_sec, WaitBackoff backoff)
{
    // TODO(yingchun): should use mono time
    uint64_t deadline = dsn_now_s() + timeout_sec;
    {
        // Disable gtest's "on failure" behavior, or else the assertion failures
        // inside our attempts will cause the test to end even though we would
        // like to retry.
        bool old_break_on_failure = testing::FLAGS_gtest_break_on_failure;
        bool old_throw_on_failure = testing::FLAGS_gtest_throw_on_failure;
        auto c = dsn::defer([old_break_on_failure, old_throw_on_failure]() {
            testing::FLAGS_gtest_break_on_failure = old_break_on_failure;
            testing::FLAGS_gtest_throw_on_failure = old_throw_on_failure;
        });
        testing::FLAGS_gtest_break_on_failure = false;
        testing::FLAGS_gtest_throw_on_failure = false;

        for (int attempts = 0; dsn_now_s() < deadline; attempts++) {
            // Capture any assertion failures within this scope (i.e. from their function)
            // into 'results'
            testing::TestPartResultArray results;
            testing::ScopedFakeTestPartResultReporter reporter(
                testing::ScopedFakeTestPartResultReporter::INTERCEPT_ONLY_CURRENT_THREAD, &results);
            f();

            // Determine whether their function produced any new test failure results.
            bool has_failures = false;
            for (int i = 0; i < results.size(); i++) {
                has_failures |= results.GetTestPartResult(i).failed();
            }
            if (!has_failures) {
                return;
            }

            // If they had failures, sleep and try again.
            int sleep_ms = 0;
            switch (backoff) {
            case WaitBackoff::EXPONENTIAL:
                sleep_ms = (attempts < 10) ? (1 << attempts) : 1000;
                break;
            case WaitBackoff::NONE:
                sleep_ms = 1000;
                break;
            default:
                LOG_FATAL("Unknown backoff type");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        }
    }

    // If we ran out of time looping, run their function one more time
    // without capturing its assertions. This way the assertions will
    // propagate back out to the normal test reporter. Of course it's
    // possible that it will pass on this last attempt, but that's OK
    // too, since we aren't trying to be that strict about the deadline.
    f();
    if (testing::Test::HasFatalFailure()) {
        ADD_FAILURE() << "Timed out waiting for assertion to pass.";
    }
}

void WaitCondition(const std::function<bool(void)> &f, int timeout_sec, WaitBackoff backoff)
{
    uint64_t deadline = dsn_now_s() + timeout_sec;
    for (int attempts = 0; dsn_now_s() < deadline; attempts++) {
        if (f()) {
            break;
        }
        int sleep_ms = 0;
        switch (backoff) {
        case WaitBackoff::EXPONENTIAL:
            sleep_ms = (attempts < 10) ? (1 << attempts) : 1000;
            break;
        case WaitBackoff::NONE:
            sleep_ms = 1000;
            break;
        default:
            LOG_FATAL("Unknown backoff type");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    }
}

} // namespace pegasus
