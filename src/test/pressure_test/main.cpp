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

#include <iostream>
#include <string>
#include <vector>
#include <atomic>

#include "utils/api_utilities.h"
#include "runtime/api_layer1.h"
#include "utils/fmt_logging.h"
#include "utils/rand.h"
#include "runtime/task/async_calls.h"

#include "pegasus/client.h"

using namespace std;
using namespace ::pegasus;

DEFINE_TASK_CODE(LPC_DEFAUT_TASK, TASK_PRIORITY_COMMON, dsn::THREAD_POOL_DEFAULT)

static int32_t hashkey_len;
static int32_t sortkey_len;
static int32_t value_len;

// generate hashkey/sortkey between [0, ****key_limit]
static int64_t hashkey_limit;
static int64_t sortkey_limit;

// for app
static pegasus_client *pg_client = nullptr;
static string cluster_name;
static string app_name;
static int32_t qps = 0;
static string op_name; // set/get/scan/del
// fill string in prefix, until with size(len)
std::string fill_string(const std::string &str, int len)
{
    std::string res(len - str.size(), 'a');
    return (res + str);
}

std::string get_hashkey()
{
    std::string key = to_string(dsn::rand::next_u64(0, hashkey_limit));
    if (key.size() >= hashkey_len) {
        return key;
    } else {
        return fill_string(key, hashkey_len);
    }
}

std::string get_sortkey()
{
    std::string key = to_string(dsn::rand::next_u64(0, sortkey_limit));
    if (key.size() >= sortkey_len) {
        return key;
    } else {
        return fill_string(key, sortkey_len);
    }
}

// generate base on hashkey and sortkey
std::string get_value(const std::string &hashkey, const std::string &sortkey, int len)
{
    std::string res;
    if (len <= 0) {
        return res;
    }
    if (hashkey.size() >= len) {
        return hashkey.substr(0, len);
    } else {
        res += hashkey;
        len -= hashkey.size();
    }

    if (sortkey.size() >= len) {
        return (res + sortkey.substr(0, len));
    } else {
        res += sortkey;
        len -= sortkey.size();
        return (res + get_value(hashkey, sortkey, len));
    }
}

bool verify(const std::string &hashkey, const std::string &sortkey, const std::string &value)
{
    return (value == get_value(hashkey, sortkey, value_len));
}

void test_set(int32_t qps)
{
    atomic_int qps_quota(qps);
    ::dsn::task_ptr quota_task = ::dsn::tasking::enqueue_timer(
        LPC_DEFAUT_TASK, nullptr, [&]() { qps_quota.store(qps); }, chrono::seconds(1));
    LOG_INFO("start to test set, with qps(%d)", qps);
    while (true) {
        if (qps_quota.load() >= 10) {
            qps_quota.fetch_sub(10);
            int cnt = 10;
            while (cnt > 0) {
                std::string hashkey = get_hashkey();
                std::string sortkey = get_sortkey();
                std::string value = get_value(hashkey, sortkey, value_len);
                pg_client->async_set(hashkey, sortkey, value);
                cnt -= 1;
            }
        }
    }
    quota_task->cancel(false);
}

void test_get(int32_t qps)
{
    atomic_int qps_quota(qps);
    dsn::task_ptr quota_task = dsn::tasking::enqueue_timer(
        LPC_DEFAUT_TASK, nullptr, [&]() { qps_quota.store(qps); }, chrono::seconds(1));

    LOG_INFO("start to test get, with qps(%d)", qps);
    while (true) {
        if (qps_quota.load() >= 10) {
            qps_quota.fetch_sub(10);
            int cnt = 10;
            while (cnt > 0) {
                string hashkey = get_hashkey();
                string sortkey = get_sortkey();
                string value;
                pg_client->async_get(
                    hashkey,
                    sortkey,
                    [hashkey, sortkey](int ec, string &&val, pegasus_client::internal_info &&info) {
                        if (ec == PERR_OK) {
                            CHECK(verify(hashkey, sortkey, val),
                                  "hashkey({}) - sortkey({}) - value({}), but value({})",
                                  hashkey,
                                  sortkey,
                                  get_value(hashkey, sortkey, value_len),
                                  val);
                        } else if (ec == PERR_NOT_FOUND) {
                            // don't output info
                            // LOG_WARNING("hashkey(%s) - sortkey(%s) doesn't exist in the server",
                            //    hashkey.c_str(), sortkey.c_str());
                        } else if (ec == PERR_TIMEOUT) {
                            LOG_WARNING("access server failed with err(%s)",
                                        pg_client->get_error_string(ec));
                        }
                    });
                cnt -= 1;
            }
        }
    }
    quota_task->cancel(false);
}

void test_del(int32_t qps)
{
    atomic_int qps_quota(qps);
    dsn::task_ptr quota_task = dsn::tasking::enqueue_timer(
        LPC_DEFAUT_TASK, nullptr, [&]() { qps_quota.store(qps); }, chrono::seconds(1));

    LOG_INFO("start to test get, with qps(%d)", qps);
    while (true) {
        if (qps_quota.load() >= 10) {
            qps_quota.fetch_sub(10);
            int cnt = 10;
            while (cnt > 0) {
                string hashkey = get_hashkey();
                string sortkey = get_sortkey();
                pg_client->async_del(
                    hashkey,
                    sortkey,
                    [hashkey, sortkey](int ec, pegasus_client::internal_info &&info) {
                        CHECK(ec == PERR_OK || ec == PERR_NOT_FOUND || ec == PERR_TIMEOUT,
                              "del hashkey({}) - sortkey({}) failed with err({})",
                              hashkey,
                              sortkey,
                              pg_client->get_error_string(ec));
                    });
                cnt -= 1;
            }
        }
    }
    quota_task->cancel(false);
}

void test_scan(int32_t qps) { CHECK(false, "not implemented"); }

static std::map<std::string, std::function<void(int32_t)>> _all_funcs;

void initialize()
{
    _all_funcs.insert(std::make_pair("set", std::bind(test_set, std::placeholders::_1)));
    _all_funcs.insert(std::make_pair("get", std::bind(test_get, std::placeholders::_1)));
    _all_funcs.insert(std::make_pair("del", std::bind(test_del, std::placeholders::_1)));
    _all_funcs.insert(std::make_pair("scan", std::bind(test_scan, std::placeholders::_1)));
}

int main(int argc, const char **argv)
{
    if (argc != 2) {
        cout << "Usage: " << argv[0] << " <config_file>" << endl;
        return -1;
    }

    if (!pegasus_client_factory::initialize(argv[1])) {
        cout << "Initialize pegasus_client load " << argv[1] << " file failed" << endl;
        return -1;
    }
    initialize();
    LOG_INFO("Initialize client and load config.ini succeed");
    cluster_name =
        dsn_config_get_value_string("pressureclient", "cluster_name", "onebox", "cluster name");

    app_name = dsn_config_get_value_string("pressureclient", "app_name", "temp", "app name");

    qps =
        (int32_t)dsn_config_get_value_uint64("pressureclient", "qps", 0, "qps of pressure client");

    op_name = dsn_config_get_value_string("pressureclient", "operation_name", "", "operation name");

    hashkey_limit =
        (int64_t)dsn_config_get_value_uint64("pressureclient", "hashkey_limit", 0, "hashkey limit");

    sortkey_limit =
        (int64_t)dsn_config_get_value_uint64("pressureclient", "sortkey_limit", 0, "sortkey limit");

    hashkey_len =
        (int32_t)dsn_config_get_value_uint64("pressureclient", "hashkey_len", 64, "hashkey length");

    sortkey_len =
        (int32_t)dsn_config_get_value_uint64("pressureclient", "sortkey_len", 64, "sortkey length");

    value_len =
        (int32_t)dsn_config_get_value_uint64("pressureclient", "value_len", 64, "value length");

    CHECK_GT(qps, 0);
    CHECK(!op_name.empty(), "must assign operation name");

    LOG_INFO("pressureclient %s qps = %d", op_name.c_str(), qps);

    pg_client = pegasus_client_factory::get_client(cluster_name.c_str(), app_name.c_str());
    CHECK_NOTNULL(pg_client, "initialize pg_client failed");

    auto it = _all_funcs.find(op_name);
    if (it != _all_funcs.end()) {
        LOG_INFO("start pressureclient with %s qps(%d)", op_name.c_str(), qps);
        it->second(qps);
    } else {
        CHECK(false, "Unknown operation name({})", op_name);
    }
    return 0;
}
