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

#include <vector>
#include <bitset>
#include <thread>
#include <iostream>
#include <cstdio>
#include <unistd.h>
#include <chrono>
#include <thread>
#include <atomic>
#include <memory>
#include <sys/time.h>

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/rpc_address.h"

#include "pegasus/client.h"
#include "data_verifier.h"
#include "utils/fmt_logging.h"

using namespace std;
using namespace ::pegasus;

static pegasus_client *client = nullptr;
static string app_name;
static string pegasus_cluster_name;
static uint32_t set_and_get_timeout_milliseconds;
static int set_thread_count = 0;

static std::atomic_llong set_next(0);
static int get_thread_count = 0;
static std::vector<long long> set_thread_setting_id;

static const char *set_next_key = "set_next";
static const char *check_max_key = "check_max";
static const char *hash_key_prefix = "kill_test_hash_key_";
static const char *sort_key_prefix = "kill_test_sort_key_";
static const char *value_prefix = "kill_test_value_";
static const long stat_batch = 100000;
static const long stat_min_pos = 0;
static const long stat_p90_pos = stat_batch - stat_batch / 10 - 1;
static const long stat_p99_pos = stat_batch - stat_batch / 100 - 1;
static const long stat_p999_pos = stat_batch - stat_batch / 1000 - 1;
static const long stat_p9999_pos = stat_batch - stat_batch / 10000 - 1;
static const long stat_max_pos = stat_batch - 1;

// return time in us.
long get_time()
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

long long get_min_thread_setting_id()
{
    long long id = set_thread_setting_id[0];
    for (int i = 1; i < set_thread_count; ++i) {
        if (set_thread_setting_id[i] < id)
            id = set_thread_setting_id[i];
    }
    return id;
}

void do_set(int thread_id)
{
    char buf[1024];
    std::string hash_key;
    std::string sort_key;
    std::string value;
    long long id = 0;
    int try_count = 0;
    long stat_count = 0;
    std::vector<long> stat_time;
    stat_time.resize(stat_batch);
    long last_time = get_time();
    while (true) {
        if (try_count == 0) {
            id = set_next++;
            set_thread_setting_id[thread_id] = id;
            sprintf(buf, "%s%lld", hash_key_prefix, id);
            hash_key.assign(buf);
            sprintf(buf, "%s%lld", sort_key_prefix, id);
            sort_key.assign(buf);
            sprintf(buf, "%s%lld", value_prefix, id);
            value.assign(buf);
        }
        pegasus_client::internal_info info;
        int ret =
            client->set(hash_key, sort_key, value, set_and_get_timeout_milliseconds, 0, &info);
        if (ret == PERR_OK) {
            long cur_time = get_time();
            LOG_INFO("SetThread[%d]: set succeed: id=%lld, try=%d, time=%ld (gpid=%d.%d, "
                     "decree=%lld, server=%s)",
                     thread_id,
                     id,
                     try_count,
                     (cur_time - last_time),
                     info.app_id,
                     info.partition_index,
                     info.decree,
                     info.server.c_str());
            stat_time[stat_count++] = cur_time - last_time;
            if (stat_count == stat_batch) {
                std::sort(stat_time.begin(), stat_time.end());
                long total_time = 0;
                for (auto t : stat_time)
                    total_time += t;
                LOG_INFO("SetThread[%d]: set statistics: count=%lld, min=%lld, P90=%lld, P99=%lld, "
                         "P999=%lld, P9999=%lld, max=%lld, avg=%lld",
                         thread_id,
                         stat_count,
                         stat_time[stat_min_pos],
                         stat_time[stat_p90_pos],
                         stat_time[stat_p99_pos],
                         stat_time[stat_p999_pos],
                         stat_time[stat_p9999_pos],
                         stat_time[stat_max_pos],
                         total_time / stat_batch);
                stat_count = 0;
            }
            last_time = cur_time;
            try_count = 0;
        } else {
            LOG_ERROR("SetThread[%d]: set failed: id=%lld, try=%d, ret=%d, error=%s (gpid=%d.%d, "
                      "decree=%lld, server=%s)",
                      thread_id,
                      id,
                      try_count,
                      ret,
                      client->get_error_string(ret),
                      info.app_id,
                      info.partition_index,
                      info.decree,
                      info.server.c_str());
            try_count++;
            if (try_count > 3) {
                sleep(1);
            }
        }
    }
}

// for each round:
// - loop from range [start_id, end_id]
void do_get_range(int thread_id, int round_id, long long start_id, long long end_id)
{
    LOG_INFO(
        "GetThread[%d]: round(%d): start get range [%u,%u]", thread_id, round_id, start_id, end_id);
    char buf[1024];
    std::string hash_key;
    std::string sort_key;
    std::string value;
    long long id = start_id;
    int try_count = 0;
    long stat_count = 0;
    std::vector<long> stat_time;
    stat_time.resize(stat_batch);
    long last_time = get_time();
    while (id <= end_id) {
        if (try_count == 0) {
            sprintf(buf, "%s%lld", hash_key_prefix, id);
            hash_key.assign(buf);
            sprintf(buf, "%s%lld", sort_key_prefix, id);
            sort_key.assign(buf);
            sprintf(buf, "%s%lld", value_prefix, id);
            value.assign(buf);
        }
        pegasus_client::internal_info info;
        std::string get_value;
        int ret =
            client->get(hash_key, sort_key, get_value, set_and_get_timeout_milliseconds, &info);
        if (ret == PERR_OK || ret == PERR_NOT_FOUND) {
            long cur_time = get_time();
            if (ret == PERR_NOT_FOUND) {
                LOG_FATAL("GetThread[%d]: round(%d): get not found: id=%lld, try=%d, time=%ld "
                          "(gpid=%d.%d, server=%s), and exit",
                          thread_id,
                          round_id,
                          id,
                          try_count,
                          (cur_time - last_time),
                          info.app_id,
                          info.partition_index,
                          info.server.c_str());
                exit(-1);
            } else if (value != get_value) {
                LOG_FATAL("GetThread[%d]: round(%d): get mismatched: id=%lld, try=%d, time=%ld, "
                          "expect_value=%s, real_value=%s (gpid=%d.%d, server=%s), and exit",
                          thread_id,
                          round_id,
                          id,
                          try_count,
                          (cur_time - last_time),
                          value.c_str(),
                          get_value.c_str(),
                          info.app_id,
                          info.partition_index,
                          info.server.c_str());
                exit(-1);
            } else {
                LOG_DEBUG("GetThread[%d]: round(%d): get succeed: id=%lld, try=%d, time=%ld "
                          "(gpid=%d.%d, server=%s)",
                          thread_id,
                          round_id,
                          id,
                          try_count,
                          (cur_time - last_time),
                          info.app_id,
                          info.partition_index,
                          info.server.c_str());
                stat_time[stat_count++] = cur_time - last_time;
                if (stat_count == stat_batch) {
                    std::sort(stat_time.begin(), stat_time.end());
                    long total_time = 0;
                    for (auto t : stat_time)
                        total_time += t;
                    LOG_INFO("GetThread[%d]: get statistics: count=%lld, min=%lld, P90=%lld, "
                             "P99=%lld, P999=%lld, P9999=%lld, max=%lld, avg=%lld",
                             thread_id,
                             stat_count,
                             stat_time[stat_min_pos],
                             stat_time[stat_p90_pos],
                             stat_time[stat_p99_pos],
                             stat_time[stat_p999_pos],
                             stat_time[stat_p9999_pos],
                             stat_time[stat_max_pos],
                             total_time / stat_batch);
                    stat_count = 0;
                }
            }
            last_time = cur_time;
            try_count = 0;
            id++;
        } else {
            LOG_ERROR("GetThread[%d]: round(%d): get failed: id=%lld, try=%d, ret=%d, error=%s "
                      "(gpid=%d.%d, server=%s)",
                      thread_id,
                      round_id,
                      id,
                      try_count,
                      ret,
                      client->get_error_string(ret),
                      info.app_id,
                      info.partition_index,
                      info.server.c_str());
            try_count++;
            if (try_count > 3) {
                sleep(1);
            }
        }
    }
    LOG_INFO("GetThread[%d]: round(%d): finish get range [%u,%u]",
             thread_id,
             round_id,
             start_id,
             end_id);
}

void do_check(int thread_count)
{
    int round_id = 1;
    while (true) {
        long long range_end = get_min_thread_setting_id() - 1;
        if (range_end < thread_count) {
            sleep(1);
            continue;
        }
        LOG_INFO("CheckThread: round(%d): start check round, range_end=%lld", round_id, range_end);
        long start_time = get_time();
        std::vector<std::thread> worker_threads;
        long long piece_count = range_end / thread_count;
        for (int i = 0; i < thread_count; ++i) {
            long long start_id = piece_count * i;
            long long end_id = (i == thread_count - 1) ? range_end : (piece_count * (i + 1) - 1);
            worker_threads.emplace_back(do_get_range, i, round_id, start_id, end_id);
        }
        for (auto &t : worker_threads) {
            t.join();
        }
        long finish_time = get_time();
        LOG_INFO(
            "CheckThread: round(%d): finish check round, range_end=%lld, total_time=%ld seconds",
            round_id,
            range_end,
            (finish_time - start_time) / 1000000);

        // update check_max
        while (true) {
            char buf[1024];
            sprintf(buf, "%lld", range_end);
            int ret = client->set(check_max_key, "", buf, set_and_get_timeout_milliseconds);
            if (ret == PERR_OK) {
                LOG_INFO("CheckThread: round(%d): update \"%s\" succeed: check_max=%lld",
                         round_id,
                         check_max_key,
                         range_end);
                break;
            } else {
                LOG_ERROR("CheckThread: round(%d): update \"%s\" failed: check_max=%lld, ret=%d, "
                          "error=%s",
                          round_id,
                          check_max_key,
                          range_end,
                          ret,
                          client->get_error_string(ret));
            }
        }

        round_id++;
    }
}

void do_mark()
{
    char buf[1024];
    long last_time = get_time();
    long long old_id = 0;
    std::string value;
    while (true) {
        sleep(1);
        long long new_id = get_min_thread_setting_id();
        CHECK_GE(new_id, old_id);
        if (new_id == old_id) {
            continue;
        }
        sprintf(buf, "%lld", new_id);
        value.assign(buf);
        int ret = client->set(set_next_key, "", value, set_and_get_timeout_milliseconds);
        if (ret == PERR_OK) {
            long cur_time = get_time();
            LOG_INFO("MarkThread: update \"%s\" succeed: set_next=%lld, time=%ld",
                     set_next_key,
                     new_id,
                     (cur_time - last_time));
            old_id = new_id;
        } else {
            LOG_ERROR("MarkThread: update \"%s\" failed: set_next=%lld, ret=%d, error=%s",
                      set_next_key,
                      new_id,
                      ret,
                      client->get_error_string(ret));
        }
    }
}

void verifier_initialize(const char *config_file)
{
    const char *section = "pegasus.killtest";
    if (!pegasus_client_factory::initialize(config_file)) {
        exit(-1);
    }

    app_name = dsn_config_get_value_string(
        section, "verify_app_name", "temp", "verify app name"); // default using temp
    pegasus_cluster_name =
        dsn_config_get_value_string(section, "pegasus_cluster_name", "", "pegasus cluster name");
    if (pegasus_cluster_name.empty()) {
        LOG_ERROR("Should config the cluster name for verifier");
        exit(-1);
    }
    client = pegasus_client_factory::get_client(pegasus_cluster_name.c_str(), app_name.c_str());
    if (client == nullptr) {
        LOG_ERROR("Initialize the _client failed");
        exit(-1);
    }

    set_and_get_timeout_milliseconds = (uint32_t)dsn_config_get_value_uint64(
        section, "set_and_get_timeout_milliseconds", 3000, "set and get timeout milliseconds");
    set_thread_count =
        (uint32_t)dsn_config_get_value_uint64(section, "set_thread_count", 5, "set thread count");
    get_thread_count = (uint32_t)dsn_config_get_value_uint64(
        section, "get_thread_count", set_thread_count * 4, "get thread count");
}

void verifier_start()
{
    // check the set_next
    while (true) {
        std::string set_next_value;
        int ret = client->get(set_next_key, "", set_next_value, set_and_get_timeout_milliseconds);
        if (ret == PERR_OK) {
            long long i = atoll(set_next_value.c_str());
            if (i == 0 && !set_next_value.empty()) {
                LOG_ERROR("MainThread: read \"%s\" failed: value_str=%s",
                          set_next_key,
                          set_next_value.c_str());
                exit(-1);
            }
            LOG_INFO("MainThread: read \"%s\" succeed: value=%lld", set_next_key, i);
            set_next.store(i);
            break;
        } else if (ret == PERR_NOT_FOUND) {
            LOG_INFO("MainThread: read \"%s\" not found, init set_next to 0", set_next_key);
            set_next.store(0);
            break;
        } else {
            LOG_ERROR("MainThread: read \"%s\" failed: error=%s",
                      set_next_key,
                      client->get_error_string(ret));
        }
    }
    set_thread_setting_id.resize(set_thread_count);

    std::vector<std::thread> set_threads;
    for (int i = 0; i < set_thread_count; ++i) {
        set_threads.emplace_back(do_set, i);
    }
    std::thread mark_thread(do_mark);

    // start several threads to read data from pegasus cluster and check data correctness,
    // block until the check failed
    do_check(get_thread_count);

    mark_thread.join();
    for (auto &t : set_threads) {
        t.join();
    }
}
