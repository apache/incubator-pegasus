// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <iostream>
#include <string>
#include <vector>
#include <atomic>

#include <dsn/c/api_utilities.h>
#include <dsn/c/api_layer1.h>
#include <dsn/utility/rand.h>
#include <dsn/tool-api/async_calls.h>

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
    ddebug("start to test set, with qps(%d)", qps);
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

    ddebug("start to test get, with qps(%d)", qps);
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
                            if (!verify(hashkey, sortkey, val)) {
                                dassert(false,
                                        "hashkey(%s) - sortkey(%s) - value(%s), but value(%s)",
                                        hashkey.c_str(),
                                        sortkey.c_str(),
                                        get_value(hashkey, sortkey, value_len).c_str(),
                                        val.c_str());
                            }
                        } else if (ec == PERR_NOT_FOUND) {
                            // don't output info
                            // dwarn("hashkey(%s) - sortkey(%s) doesn't exist in the server",
                            //    hashkey.c_str(), sortkey.c_str());
                        } else if (ec == PERR_TIMEOUT) {
                            dwarn("access server failed with err(%s)",
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

    ddebug("start to test get, with qps(%d)", qps);
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
                        if (ec != PERR_OK && ec != PERR_NOT_FOUND && ec != PERR_TIMEOUT) {
                            dassert(false,
                                    "del hashkey(%s) - sortkey(%s) failed with err(%s)",
                                    hashkey.c_str(),
                                    sortkey.c_str(),
                                    pg_client->get_error_string(ec));
                        }
                    });
                cnt -= 1;
            }
        }
    }
    quota_task->cancel(false);
}

void test_scan(int32_t qps) { dassert(false, "not implemented"); }

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
    ddebug("Initialize client and load config.ini succeed");
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

    dassert(qps > 0, "qps must GT 0, but qps(%d)", qps);
    dassert(!op_name.empty(), "must assign operation name");

    ddebug("pressureclient %s qps = %d", op_name.c_str(), qps);

    pg_client = pegasus_client_factory::get_client(cluster_name.c_str(), app_name.c_str());

    dassert(pg_client != nullptr, "initialize pg_client failed");

    auto it = _all_funcs.find(op_name);
    if (it != _all_funcs.end()) {
        ddebug("start pressureclient with %s qps(%d)", op_name.c_str(), qps);
        it->second(qps);
    } else {
        dassert(false, "Unknown operation name(%s)", op_name.c_str());
    }
    return 0;
}
