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

#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/service_api_c.h>
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

static const char CCH[] = "_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static const int max_batch_count = 500;
static const int timeout_ms = 5000;
static const int max_multi_set_concurrency = 20;
static const int default_partitions = 4;
static const string empty_hash_key = "";
static const string source_app_name = "copy_data_source_table";
static const string destination_app_name = "copy_data_destination_table";
static char buffer[256];
static map<string, map<string, string>> base_data;
static pegasus_client *srouce_client;
static pegasus_client *destination_client;

class copy_data_test : public test_util
{
public:
    void SetUp() override
    {
        test_util::SetUp();
        create_table_and_get_client();
        fill_data();
    }

    void TearDown() override
    {
        test_util::TearDown();
        ASSERT_EQ(dsn::ERR_OK, ddl_client->drop_app(source_app_name, 0));
        ASSERT_EQ(dsn::ERR_OK, ddl_client->drop_app(destination_app_name, 0));
    }

    void verify_data()
    {
        pegasus_client::scan_options options;
        vector<pegasus_client::pegasus_scanner *> scanners;
        int ret = destination_client->get_unordered_scanners(INT_MAX, options, scanners);
        ASSERT_EQ(0, ret) << "Error occurred when getting scanner. error="
                          << destination_client->get_error_string(ret);

        string hash_key;
        string sort_key;
        string value;
        map<string, map<string, string>> data;
        for (auto scanner : scanners) {
            ASSERT_NE(nullptr, scanner);
            while (PERR_OK == (ret = (scanner->next(hash_key, sort_key, value)))) {
                check_and_put(data, hash_key, sort_key, value);
            }
            ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                               << destination_client->get_error_string(ret);
            delete scanner;
        }

        compare(data, base_data);

        ddebug_f("Data and base_data are the same.");
    }

    void create_table_and_get_client()
    {
        dsn::error_code err;
        err = ddl_client->create_app(source_app_name, "pegasus", default_partitions, 3, {}, false);
        ASSERT_EQ(dsn::ERR_OK, err);

        err = ddl_client->create_app(
            destination_app_name, "pegasus", default_partitions, 3, {}, false);
        ASSERT_EQ(dsn::ERR_OK, err);

        srouce_client = pegasus_client_factory::get_client("mycluster", source_app_name.c_str());
        destination_client =
            pegasus_client_factory::get_client("mycluster", destination_app_name.c_str());
    }

    // REQUIRED: 'buffer' has been filled with random chars.
    const string random_string()
    {
        int pos = random() % sizeof(buffer);
        buffer[pos] = CCH[random() % strlen(CCH)];
        unsigned int length = random() % sizeof(buffer) + 1;
        if (pos + length < sizeof(buffer)) {
            return string(buffer + pos, length);
        } else {
            return string(buffer + pos, sizeof(buffer) - pos) +
                   string(buffer, length + pos - sizeof(buffer));
        }
    }

    void fill_data()
    {
        ddebug_f("FILLING_DATA...");

        srandom((unsigned int)time(nullptr));
        for (auto &c : buffer) {
            c = CCH[random() % strlen(CCH)];
        }

        string hash_key;
        string sort_key;
        string value;
        while (base_data[empty_hash_key].size() < 1000) {
            sort_key = random_string();
            value = random_string();
            int ret = srouce_client->set(empty_hash_key, sort_key, value);
            ASSERT_EQ(PERR_OK, ret) << "Error occurred when set, hash_key=" << hash_key
                                    << ", sort_key=" << sort_key
                                    << ", error=" << srouce_client->get_error_string(ret);
            base_data[empty_hash_key][sort_key] = value;
        }

        while (base_data.size() < 500) {
            hash_key = random_string();
            while (base_data[hash_key].size() < 10) {
                sort_key = random_string();
                value = random_string();
                int ret = srouce_client->set(hash_key, sort_key, value);
                ASSERT_EQ(PERR_OK, ret) << "Error occurred when set, hash_key=" << hash_key
                                        << ", sort_key=" << sort_key
                                        << ", error=" << srouce_client->get_error_string(ret);
                base_data[hash_key][sort_key] = value;
            }
        }

        ddebug_f("Data filled.");
    }
};

TEST_F(copy_data_test, EMPTY_HASH_KEY_COPY)
{
    ddebug_f("TESTING_COPY_DATA, EMPTY HASH_KEY COPY ....");

    pegasus_client::scan_options options;
    options.return_expire_ts = true;
    vector<pegasus::pegasus_client::pegasus_scanner *> raw_scanners;
    int ret = srouce_client->get_unordered_scanners(INT_MAX, options, raw_scanners);
    ASSERT_EQ(pegasus::PERR_OK, ret) << "Error occurred when getting scanner. error="
                                     << srouce_client->get_error_string(ret);

    ddebug_f("open source app scanner succeed, partition_count = {}", raw_scanners.size());

    vector<pegasus::pegasus_client::pegasus_scanner_wrapper> scanners;
    for (auto raw_scanner : raw_scanners) {
        ASSERT_NE(nullptr, raw_scanner);
        scanners.push_back(raw_scanner->get_smart_wrapper());
    }
    raw_scanners.clear();

    int split_count = scanners.size();
    ddebug_f("prepare scanners succeed, split_count = {}", split_count);

    std::atomic_bool error_occurred(false);
    vector<std::unique_ptr<scan_data_context>> contexts;

    for (int i = 0; i < split_count; i++) {
        scan_data_context *context = new scan_data_context(SCAN_AND_MULTI_SET,
                                                           i,
                                                           max_batch_count,
                                                           timeout_ms,
                                                           scanners[i],
                                                           destination_client,
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

    verify_data();

    ddebug_f("finished copy data test..");
}
