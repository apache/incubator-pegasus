/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <cstdlib>
#include <string>
#include <vector>
#include <map>

#include "utils/fmt_logging.h"
#include "client/replication_ddl_client.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include <gtest/gtest.h>
#include "include/pegasus/client.h"
#include <unistd.h>

#include "base/pegasus_const.h"
#include "base/pegasus_utils.h"
#include "test/function_test/utils/global_env.h"
#include "shell/commands.h"
#include "test/function_test/utils/utils.h"
#include "test/function_test/utils/test_util.h"

using namespace ::pegasus;
using std::map;
using std::string;
using std::vector;

class copy_data_test : public test_util
{
public:
    void SetUp() override
    {
        test_util::SetUp();
        ASSERT_NO_FATAL_FAILURE(create_table_and_get_client());
        ASSERT_NO_FATAL_FAILURE(fill_data());
    }

    void TearDown() override
    {
        test_util::TearDown();
        ASSERT_EQ(dsn::ERR_OK, ddl_client_->drop_app(source_app_name, 0));
        ASSERT_EQ(dsn::ERR_OK, ddl_client_->drop_app(destination_app_name, 0));
    }

    void verify_data()
    {
        pegasus_client::scan_options options;
        vector<pegasus_client::pegasus_scanner *> scanners;
        ASSERT_EQ(PERR_OK, destination_client_->get_unordered_scanners(INT_MAX, options, scanners));

        string hash_key;
        string sort_key;
        string value;
        map<string, map<string, string>> actual_data;
        for (auto scanner : scanners) {
            ASSERT_NE(nullptr, scanner);
            int ret = PERR_OK;
            while (PERR_OK == (ret = (scanner->next(hash_key, sort_key, value)))) {
                check_and_put(actual_data, hash_key, sort_key, value);
            }
            ASSERT_EQ(PERR_SCAN_COMPLETE, ret);
            delete scanner;
        }

        ASSERT_NO_FATAL_FAILURE(compare(expect_data_, actual_data));
    }

    void create_table_and_get_client()
    {
        ASSERT_EQ(
            dsn::ERR_OK,
            ddl_client_->create_app(source_app_name, "pegasus", default_partitions, 3, {}, false));
        ASSERT_EQ(dsn::ERR_OK,
                  ddl_client_->create_app(
                      destination_app_name, "pegasus", default_partitions, 3, {}, false));
        srouce_client_ =
            pegasus_client_factory::get_client(cluster_name_.c_str(), source_app_name.c_str());
        ASSERT_NE(nullptr, srouce_client_);
        destination_client_ =
            pegasus_client_factory::get_client(cluster_name_.c_str(), destination_app_name.c_str());
        ASSERT_NE(nullptr, destination_client_);
    }

    // REQUIRED: 'buffer_' has been filled with random chars.
    const string random_string() const
    {
        int pos = random() % sizeof(buffer_);
        unsigned int length = random() % sizeof(buffer_) + 1;
        if (pos + length < sizeof(buffer_)) {
            return string(buffer_ + pos, length);
        } else {
            return string(buffer_ + pos, sizeof(buffer_) - pos) +
                   string(buffer_, length + pos - sizeof(buffer_));
        }
    }

    void fill_data()
    {
        srandom((unsigned int)time(nullptr));
        for (auto &c : buffer_) {
            c = CCH[random() % strlen(CCH)];
        }

        string hash_key;
        string sort_key;
        string value;
        while (expect_data_[empty_hash_key].size() < 1000) {
            sort_key = random_string();
            value = random_string();
            ASSERT_EQ(PERR_OK, srouce_client_->set(empty_hash_key, sort_key, value))
                << "hash_key=" << hash_key << ", sort_key=" << sort_key;
            expect_data_[empty_hash_key][sort_key] = value;
        }

        while (expect_data_.size() < 500) {
            hash_key = random_string();
            while (expect_data_[hash_key].size() < 10) {
                sort_key = random_string();
                value = random_string();
                ASSERT_EQ(PERR_OK, srouce_client_->set(hash_key, sort_key, value))
                    << "hash_key=" << hash_key << ", sort_key=" << sort_key;
                expect_data_[hash_key][sort_key] = value;
            }
        }
    }

protected:
    static const char CCH[];
    const string empty_hash_key = "";
    const string source_app_name = "copy_data_source_table";
    const string destination_app_name = "copy_data_destination_table";

    const int max_batch_count = 500;
    const int timeout_ms = 5000;
    const int max_multi_set_concurrency = 20;
    const int default_partitions = 4;

    char buffer_[256];
    map<string, map<string, string>> expect_data_;

    pegasus_client *srouce_client_;
    pegasus_client *destination_client_;
};
const char copy_data_test::CCH[] =
    "_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

TEST_F(copy_data_test, EMPTY_HASH_KEY_COPY)
{
    LOG_INFO("TESTING_COPY_DATA, EMPTY HASH_KEY COPY ....");

    pegasus_client::scan_options options;
    options.return_expire_ts = true;
    vector<pegasus::pegasus_client::pegasus_scanner *> raw_scanners;
    ASSERT_EQ(PERR_OK, srouce_client_->get_unordered_scanners(INT_MAX, options, raw_scanners));

    LOG_INFO("open source app scanner succeed, partition_count = {}", raw_scanners.size());

    vector<pegasus::pegasus_client::pegasus_scanner_wrapper> scanners;
    for (auto raw_scanner : raw_scanners) {
        ASSERT_NE(nullptr, raw_scanner);
        scanners.push_back(raw_scanner->get_smart_wrapper());
    }
    raw_scanners.clear();

    int split_count = scanners.size();
    LOG_INFO("prepare scanners succeed, split_count = {}", split_count);

    std::atomic_bool error_occurred(false);
    vector<std::unique_ptr<scan_data_context>> contexts;

    for (int i = 0; i < split_count; i++) {
        scan_data_context *context = new scan_data_context(SCAN_AND_MULTI_SET,
                                                           i,
                                                           max_batch_count,
                                                           timeout_ms,
                                                           scanners[i],
                                                           destination_client_,
                                                           nullptr,
                                                           &error_occurred,
                                                           max_multi_set_concurrency);
        contexts.emplace_back(context);
        dsn::tasking::enqueue(LPC_SCAN_DATA, nullptr, std::bind(scan_multi_data_next, context));
    }

    // wait thread complete
    int sleep_seconds = 0;
    while (sleep_seconds < 120) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        sleep_seconds++;
        int completed_split_count = 0;
        for (int i = 0; i < split_count; i++) {
            if (contexts[i]->split_completed.load()) {
                completed_split_count++;
            }
        }
        if (completed_split_count == split_count) {
            break;
        }
    }

    ASSERT_FALSE(error_occurred.load()) << "error occurred, processing terminated or timeout!";
    ASSERT_NO_FATAL_FAILURE(verify_data());
}
