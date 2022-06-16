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

#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/service_api_c.h>
#include <geo/lib/geo_client.h>
#include <gtest/gtest.h>
#include <pegasus/client.h>
#include <unistd.h>

#include "base/pegasus_const.h"
#include "base/pegasus_utils.h"
#include "global_env.h"
#include "shell/commands.h"
#include "utils.h"

using namespace ::pegasus;

extern std::shared_ptr<dsn::replication::replication_ddl_client> ddl_client;
static const char CCH[] = "_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static const int max_batch_count = 500;
static const int timeout_ms = 5000;
static const int max_multi_set_concurrency = 20;
static const int default_partitions = 4;
static const std::string empty_hash_key = "";
static const std::string srouce_app_name = "copy_data_source";
static const std::string destination_app_name = "copy_data_destination";
static char buffer[256];
static std::map<std::string, std::map<std::string, std::string>> base_data;
static pegasus_client *srouce_client;
static pegasus_client *destination_client;

static void verify_data()
{
    pegasus_client::scan_options options;
    std::vector<pegasus_client::pegasus_scanner *> scanners;
    int ret = destination_client->get_unordered_scanners(INT_MAX, options, scanners);
    ASSERT_EQ(0, ret) << "Error occurred when getting scanner. error="
                      << destination_client->get_error_string(ret);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    std::map<std::string, std::map<std::string, std::string>> data;
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

    ddebug("Data and base_data are the same.");
}

static void create_table_and_get_client()
{
    dsn::error_code err;
    err = ddl_client->create_app(srouce_app_name, "pegasus", default_partitions, 3, {}, false);
    ASSERT_EQ(dsn::ERR_OK, err);

    err = ddl_client->create_app(destination_app_name, "pegasus", default_partitions, 3, {}, false);
    ASSERT_EQ(dsn::ERR_OK, err);

    srouce_client = pegasus_client_factory::get_client("mycluster", srouce_app_name.c_str());
    destination_client =
        pegasus_client_factory::get_client("mycluster", destination_app_name.c_str());
}

// REQUIRED: 'buffer' has been filled with random chars.
static const std::string random_string()
{
    int pos = random() % sizeof(buffer);
    buffer[pos] = CCH[random() % sizeof(CCH)];
    unsigned int length = random() % sizeof(buffer) + 1;
    if (pos + length < sizeof(buffer)) {
        return std::string(buffer + pos, length);
    } else {
        return std::string(buffer + pos, sizeof(buffer) - pos) +
               std::string(buffer, length + pos - sizeof(buffer));
    }
}

static void fill_data()
{
    ddebug("FILLING_DATA...");

    srandom((unsigned int)time(nullptr));
    for (auto &c : buffer) {
        c = CCH[random() % sizeof(CCH)];
    }

    std::string hash_key;
    std::string sort_key;
    std::string value;
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

    ddebug("Data filled.");
}

class copy_data_test : public testing::Test
{
public:
    static void SetUpTestCase()
    {
        ddebug("SetUp...");
        create_table_and_get_client();
        fill_data();
    }

    static void TearDownTestCase()
    {
        ddebug("TearDown...");
        chdir(global_env::instance()._pegasus_root.c_str());
        system("./run.sh clear_onebox");
        system("./run.sh start_onebox -w");
        chdir(global_env::instance()._working_dir.c_str());
    }
};

TEST_F(copy_data_test, EMPTY_HASH_KEY_COPOY)
{
    ddebug("TESTING_COPY_DATA, EMPTY HAS_HKEY COPOY ....");

    pegasus_client::scan_options options;
    options.return_expire_ts = true;
    std::vector<pegasus::pegasus_client::pegasus_scanner *> raw_scanners;
    int ret = srouce_client->get_unordered_scanners(INT_MAX, options, raw_scanners);
    ASSERT_EQ(pegasus::PERR_OK, ret) << "Error occurred when getting scanner. error="
                                     << srouce_client->get_error_string(ret);

    ddebug("INFO: open source app scanner succeed, partition_count = %d\n",
           (int)raw_scanners.size());

    std::vector<pegasus::pegasus_client::pegasus_scanner_wrapper> scanners;
    for (auto raw_scanner : raw_scanners) {
        ASSERT_NE(nullptr, raw_scanner);
        scanners.push_back(raw_scanner->get_smart_wrapper());
    }
    raw_scanners.clear();

    int split_count = scanners.size();
    ddebug("INFO: prepare scanners succeed, split_count = %d\n", split_count);

    std::atomic_bool error_occurred(false);
    std::vector<std::unique_ptr<scan_data_context>> contexts;
    std::unique_ptr<pegasus::geo::geo_client> geo_client;

    for (int i = 0; i < split_count; i++) {
        scan_data_context *context = new scan_data_context(SCAN_AND_MULTI_SET,
                                                           i,
                                                           max_batch_count,
                                                           timeout_ms,
                                                           scanners[i],
                                                           destination_client,
                                                           geo_client.get(),
                                                           &error_occurred,
                                                           max_multi_set_concurrency);
        contexts.emplace_back(context);
        dsn::tasking::enqueue(LPC_SCAN_DATA, nullptr, std::bind(scan_multi_data_next, context));
    }

    // wait thread complete
    int sleep_seconds = 0;
    long last_total_rows = 0;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        sleep_seconds++;
        int completed_split_count = 0;
        long cur_total_rows = 0;
        for (int i = 0; i < split_count; i++) {
            cur_total_rows += contexts[i]->split_rows.load();
            completed_split_count++;
        }
        if (error_occurred.load()) {
            ddebug("processed for %d seconds, (%d/%d) splits, total %ld rows, last second "
                   "%ld rows, error occurred, terminating..",
                   sleep_seconds,
                   completed_split_count,
                   split_count,
                   cur_total_rows,
                   cur_total_rows - last_total_rows);
        } else {
            ddebug("processed for %d seconds, (%d/%d) splits, total %ld rows, last second "
                   "%ld rows",
                   sleep_seconds,
                   completed_split_count,
                   split_count,
                   cur_total_rows,
                   cur_total_rows - last_total_rows);
        }
        if (completed_split_count == split_count)
            break;
        last_total_rows = cur_total_rows;
    }

    ASSERT_EQ(false, error_occurred.load()) << "error occurred, processing terminated";

    long total_rows = 0;
    for (int i = 0; i < split_count; i++) {
        ddebug("split[%d]: %ld rows", i, contexts[i]->split_rows.load());
        total_rows += contexts[i]->split_rows.load();
    }

    // fill_data count = 1000 + (500 - 1) * 10
    ASSERT_EQ(5990, total_rows) << "Copy total " << total_rows << " rows. Not 5990 !!!";

    verify_data();

    ddebug("finished copy data test..");
}
