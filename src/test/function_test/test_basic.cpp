// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <cstdlib>
#include <string>
#include <vector>
#include <climits>
#include <map>

#include <dsn/service_api_c.h>
#include <unistd.h>
#include <pegasus/client.h>
#include <gtest/gtest.h>
#include <atomic>

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "function.test.basic"

using namespace ::pegasus;

extern pegasus_client *client;
typedef pegasus_client::internal_info internal_info;

TEST(basic, set_get_del)
{
    ASSERT_STREQ("mycluster", client->get_cluster_name());

    // set
    int ret = client->set("basic_test_hash_key_1", "basic_test_sort_key_1", "basic_test_value_1");
    ASSERT_EQ(PERR_OK, ret);

    // exist
    ret = client->exist("basic_test_hash_key_1", "basic_test_sort_key_1");
    ASSERT_EQ(PERR_OK, ret);

    ret = client->exist("basic_test_hash_key_1", "basic_test_sort_key_2");
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // sortkey_count
    int64_t count;
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, count);

    // get
    std::string new_value_str;
    ret = client->get("basic_test_hash_key_1", "basic_test_sort_key_1", new_value_str);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ("basic_test_value_1", new_value_str);

    ret = client->get("basic_test_hash_key_1", "basic_test_sort_key_2", new_value_str);
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // del
    ret = client->del("basic_test_hash_key_1", "basic_test_sort_key_1");
    ASSERT_EQ(PERR_OK, ret);

    // exist
    ret = client->exist("basic_test_hash_key_1", "basic_test_sort_key_1");
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // sortkey_count
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, count);

    // get
    ret = client->get("basic_test_hash_key_1", "basic_test_sort_key_1", new_value_str);
    ASSERT_EQ(PERR_NOT_FOUND, ret);
}

TEST(basic, multi_set_get_del)
{
    // multi_set
    std::map<std::string, std::string> kvs;
    kvs["basic_test_sort_key_1"] = "basic_test_value_1";
    kvs["basic_test_sort_key_2"] = "basic_test_value_2";
    kvs["basic_test_sort_key_3"] = "basic_test_value_3";
    kvs["basic_test_sort_key_4"] = "basic_test_value_4";
    int ret = client->multi_set("basic_test_hash_key_1", kvs);
    ASSERT_EQ(PERR_OK, ret);

    // sortkey_count
    int64_t count;
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(4, count);

    // multi_get
    std::set<std::string> sortkeys;
    sortkeys.insert("basic_test_sort_key_0");
    sortkeys.insert("basic_test_sort_key_1");
    sortkeys.insert("basic_test_sort_key_2");
    sortkeys.insert("basic_test_sort_key_3");
    sortkeys.insert("basic_test_sort_key_4");
    std::map<std::string, std::string> new_kvs;
    ret = client->multi_get("basic_test_hash_key_1", sortkeys, new_kvs);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(4, new_kvs.size());
    auto it = new_kvs.begin();
    ASSERT_EQ("basic_test_sort_key_1", it->first);
    ASSERT_EQ("basic_test_value_1", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_2", it->first);
    ASSERT_EQ("basic_test_value_2", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_3", it->first);
    ASSERT_EQ("basic_test_value_3", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_4", it->first);
    ASSERT_EQ("basic_test_value_4", it->second);

    // multi_get with limit count
    new_kvs.clear();
    ret = client->multi_get("basic_test_hash_key_1", sortkeys, new_kvs, 1);
    ASSERT_EQ(PERR_INCOMPLETE, ret);
    ASSERT_EQ(1, new_kvs.size());
    it = new_kvs.begin();
    ASSERT_EQ("basic_test_sort_key_1", it->first);
    ASSERT_EQ("basic_test_value_1", it->second);

    // multi_get with empty sortkeys
    sortkeys.clear();
    new_kvs.clear();
    ret = client->multi_get("basic_test_hash_key_1", sortkeys, new_kvs);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(4, new_kvs.size());
    it = new_kvs.begin();
    ASSERT_EQ("basic_test_sort_key_1", it->first);
    ASSERT_EQ("basic_test_value_1", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_2", it->first);
    ASSERT_EQ("basic_test_value_2", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_3", it->first);
    ASSERT_EQ("basic_test_value_3", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_4", it->first);
    ASSERT_EQ("basic_test_value_4", it->second);

    // multi_get_sortkeys with no limit count
    sortkeys.clear();
    ret = client->multi_get_sortkeys("basic_test_hash_key_1", sortkeys, -1);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(4, sortkeys.size());
    auto it2 = sortkeys.begin();
    ASSERT_EQ("basic_test_sort_key_1", *it2);
    it2++;
    ASSERT_EQ("basic_test_sort_key_2", *it2);
    it2++;
    ASSERT_EQ("basic_test_sort_key_3", *it2);
    it2++;
    ASSERT_EQ("basic_test_sort_key_4", *it2);

    // multi_get_sortkeys with limit count
    sortkeys.clear();
    ret = client->multi_get_sortkeys("basic_test_hash_key_1", sortkeys, 1);
    ASSERT_EQ(PERR_INCOMPLETE, ret);
    ASSERT_EQ(1, sortkeys.size());
    it2 = sortkeys.begin();
    ASSERT_EQ("basic_test_sort_key_1", *it2);

    // multi_del with empty sortkeys
    sortkeys.clear();
    int64_t deleted_count;
    ret = client->multi_del("basic_test_hash_key_1", sortkeys, deleted_count);
    ASSERT_EQ(PERR_INVALID_VALUE, ret);

    // multi_del
    sortkeys.clear();
    sortkeys.insert("basic_test_sort_key_0");
    sortkeys.insert("basic_test_sort_key_1");
    sortkeys.insert("basic_test_sort_key_2");
    ret = client->multi_del("basic_test_hash_key_1", sortkeys, deleted_count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(3, deleted_count);

    // sortkey_count
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, count);

    // check deleted
    sortkeys.clear();
    new_kvs.clear();
    ret = client->multi_get("basic_test_hash_key_1", sortkeys, new_kvs);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, new_kvs.size());
    it = new_kvs.begin();
    ASSERT_EQ("basic_test_sort_key_3", it->first);
    ASSERT_EQ("basic_test_value_3", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_4", it->first);
    ASSERT_EQ("basic_test_value_4", it->second);

    // multi_del
    sortkeys.clear();
    sortkeys.insert("basic_test_sort_key_3");
    sortkeys.insert("basic_test_sort_key_4");
    ret = client->multi_del("basic_test_hash_key_1", sortkeys, deleted_count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, deleted_count);

    // sortkey_count
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, count);
}

TEST(basic, set_get_del_async)
{
    std::atomic<bool> callbacked(false);
    int ret = 0;
    std::string new_value_str;
    // set_async
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_set("basic_test_hash_key_1",
                      "basic_test_sort_key_1",
                      "basic_test_value_1",
                      [&](int err, internal_info &&info) {
                          ASSERT_EQ(PERR_OK, err);
                          ASSERT_GT(info.app_id, 0);
                          ASSERT_GT(info.partition_index, 0);
                          ASSERT_GT(info.decree, 0);
                          ASSERT_FALSE(info.server.empty());
                          callbacked.store(true, std::memory_order_seq_cst);
                      });
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // exist
    ret = client->exist("basic_test_hash_key_1", "basic_test_sort_key_1");
    ASSERT_EQ(PERR_OK, ret);

    ret = client->exist("basic_test_hash_key_1", "basic_test_sort_key_2");
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // sortkey_count
    int64_t count;
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(1, count);

    // get_async
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_get("basic_test_hash_key_1",
                      "basic_test_sort_key_1",
                      [&](int err, std::string &&value, internal_info &&info) {
                          ASSERT_EQ(PERR_OK, err);
                          ASSERT_GT(info.app_id, 0);
                          ASSERT_GT(info.partition_index, 0);
                          ASSERT_EQ(info.decree, -1);
                          ASSERT_FALSE(info.server.empty());
                          ASSERT_EQ("basic_test_value_1", value);
                          callbacked.store(true, std::memory_order_seq_cst);
                      });
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    callbacked.store(false, std::memory_order_seq_cst);
    client->async_get("basic_test_hash_key_1",
                      "basic_test_sort_key_2",
                      [&](int err, std::string &&value, internal_info &&info) {
                          ASSERT_EQ(PERR_NOT_FOUND, err);
                          ASSERT_GT(info.app_id, 0);
                          ASSERT_GT(info.partition_index, 0);
                          ASSERT_EQ(info.decree, -1);
                          ASSERT_FALSE(info.server.empty());
                          callbacked.store(true, std::memory_order_seq_cst);
                      });
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // del_async
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_del(
        "basic_test_hash_key_1", "basic_test_sort_key_1", [&](int err, internal_info &&info) {
            ASSERT_EQ(PERR_OK, err);
            ASSERT_GT(info.app_id, 0);
            ASSERT_GT(info.partition_index, 0);
            ASSERT_GT(info.decree, 0);
            ASSERT_FALSE(info.server.empty());
            callbacked.store(true, std::memory_order_seq_cst);
        });
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // exist
    ret = client->exist("basic_test_hash_key_1", "basic_test_sort_key_1");
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // sortkey_count
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, count);

    // get -- finally, using get_sync to get the key-value.
    ret = client->get("basic_test_hash_key_1", "basic_test_sort_key_1", new_value_str);
    ASSERT_EQ(PERR_NOT_FOUND, ret);
}

TEST(basic, multi_set_get_del_async)
{
    std::atomic<bool> callbacked(false);
    int ret = 0;
    std::map<std::string, std::string> new_kvs;
    // multi_set_async
    std::map<std::string, std::string> kvs;
    kvs["basic_test_sort_key_1"] = "basic_test_value_1";
    kvs["basic_test_sort_key_2"] = "basic_test_value_2";
    kvs["basic_test_sort_key_3"] = "basic_test_value_3";
    kvs["basic_test_sort_key_4"] = "basic_test_value_4";
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_set("basic_test_hash_key_1", kvs, [&](int err, internal_info &&info) {
        ASSERT_EQ(PERR_OK, err);
        ASSERT_GT(info.app_id, 0);
        ASSERT_GT(info.partition_index, 0);
        ASSERT_GT(info.decree, 0);
        ASSERT_FALSE(info.server.empty());
        callbacked.store(true, std::memory_order_seq_cst);
    });
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // sortkey_count
    int64_t count;
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(4, count);

    // multi_get_async
    std::set<std::string> sortkeys;
    sortkeys.insert("basic_test_sort_key_0");
    sortkeys.insert("basic_test_sort_key_1");
    sortkeys.insert("basic_test_sort_key_2");
    sortkeys.insert("basic_test_sort_key_3");
    sortkeys.insert("basic_test_sort_key_4");

    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_get(
        "basic_test_hash_key_1",
        sortkeys,
        [&](int err, std::map<std::string, std::string> &&values, internal_info &&info) {
            ASSERT_EQ(PERR_OK, err);
            ASSERT_GT(info.app_id, 0);
            ASSERT_GT(info.partition_index, 0);
            ASSERT_EQ(info.decree, -1);
            ASSERT_FALSE(info.server.empty());
            ASSERT_EQ(4, values.size());
            auto it = values.begin();
            ASSERT_EQ("basic_test_sort_key_1", it->first);
            ASSERT_EQ("basic_test_value_1", it->second);
            it++;
            ASSERT_EQ("basic_test_sort_key_2", it->first);
            ASSERT_EQ("basic_test_value_2", it->second);
            it++;
            ASSERT_EQ("basic_test_sort_key_3", it->first);
            ASSERT_EQ("basic_test_value_3", it->second);
            it++;
            ASSERT_EQ("basic_test_sort_key_4", it->first);
            ASSERT_EQ("basic_test_value_4", it->second);
            callbacked.store(true, std::memory_order_seq_cst);
        });
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // multi_get_async with limit count
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_get(
        "basic_test_hash_key_1",
        sortkeys,
        [&](int err, std::map<std::string, std::string> &&values, internal_info &&info) {
            ASSERT_EQ(PERR_INCOMPLETE, err);
            ASSERT_GT(info.app_id, 0);
            ASSERT_GT(info.partition_index, 0);
            ASSERT_EQ(info.decree, -1);
            ASSERT_FALSE(info.server.empty());
            ASSERT_EQ(1, values.size());
            auto it = values.begin();
            ASSERT_EQ("basic_test_sort_key_1", it->first);
            ASSERT_EQ("basic_test_value_1", it->second);
            callbacked.store(true, std::memory_order_seq_cst);
        },
        1);
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // multi_get with empty sortkeys
    sortkeys.clear();
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_get(
        "basic_test_hash_key_1",
        sortkeys,
        [&](int err, std::map<std::string, std::string> &&values, internal_info &&info) {
            ASSERT_EQ(PERR_OK, err);
            ASSERT_GT(info.app_id, 0);
            ASSERT_GT(info.partition_index, 0);
            ASSERT_EQ(info.decree, -1);
            ASSERT_FALSE(info.server.empty());
            ASSERT_EQ(4, values.size());
            auto it = values.begin();
            ASSERT_EQ("basic_test_sort_key_1", it->first);
            ASSERT_EQ("basic_test_value_1", it->second);
            it++;
            ASSERT_EQ("basic_test_sort_key_2", it->first);
            ASSERT_EQ("basic_test_value_2", it->second);
            it++;
            ASSERT_EQ("basic_test_sort_key_3", it->first);
            ASSERT_EQ("basic_test_value_3", it->second);
            it++;
            ASSERT_EQ("basic_test_sort_key_4", it->first);
            ASSERT_EQ("basic_test_value_4", it->second);
            callbacked.store(true, std::memory_order_seq_cst);
        });
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // multi_get_sortkeys_async with limit count
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_get_sortkeys(
        "basic_test_hash_key_1",
        [&](int err, std::set<std::string> &&sortkeys, internal_info &&info) {
            ASSERT_EQ(PERR_INCOMPLETE, err);
            ASSERT_GT(info.app_id, 0);
            ASSERT_GT(info.partition_index, 0);
            ASSERT_EQ(info.decree, -1);
            ASSERT_FALSE(info.server.empty());
            ASSERT_EQ(1, sortkeys.size());
            auto it = sortkeys.begin();
            ASSERT_EQ("basic_test_sort_key_1", *it);
            callbacked.store(true, std::memory_order_seq_cst);
        },
        1);
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // multi_get_sortkeys_async with no limit count
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_get_sortkeys(
        "basic_test_hash_key_1",
        [&](int err, std::set<std::string> &&sortkeys, internal_info &&info) {
            ASSERT_EQ(PERR_OK, err);
            ASSERT_GT(info.app_id, 0);
            ASSERT_GT(info.partition_index, 0);
            ASSERT_EQ(info.decree, -1);
            ASSERT_FALSE(info.server.empty());
            ASSERT_EQ(4, sortkeys.size());
            auto it = sortkeys.begin();
            ASSERT_EQ("basic_test_sort_key_1", *it);
            it++;
            ASSERT_EQ("basic_test_sort_key_2", *it);
            it++;
            ASSERT_EQ("basic_test_sort_key_3", *it);
            it++;
            ASSERT_EQ("basic_test_sort_key_4", *it);
            callbacked.store(true, std::memory_order_seq_cst);
        },
        -1);
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // multi_del_async with empty sortkeys
    sortkeys.clear();
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_del("basic_test_hash_key_1",
                            sortkeys,
                            [&](int err, int64_t deleted_count, internal_info &&info) {
                                ASSERT_EQ(PERR_INVALID_VALUE, err);
                                callbacked.store(true, std::memory_order_seq_cst);
                            });
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // multi_del_async
    sortkeys.clear();
    sortkeys.insert("basic_test_sort_key_0");
    sortkeys.insert("basic_test_sort_key_1");
    sortkeys.insert("basic_test_sort_key_2");
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_del("basic_test_hash_key_1",
                            sortkeys,
                            [&](int err, int64_t deleted_count, internal_info &&info) {
                                ASSERT_EQ(PERR_OK, err);
                                ASSERT_GT(info.app_id, 0);
                                ASSERT_GT(info.partition_index, 0);
                                ASSERT_GT(info.decree, 0);
                                ASSERT_FALSE(info.server.empty());
                                ASSERT_EQ(3, deleted_count);
                                callbacked.store(true, std::memory_order_seq_cst);
                            });
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // sortkey_count
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, count);

    // check deleted  --- using multi_get to check.
    sortkeys.clear();
    new_kvs.clear();
    ret = client->multi_get("basic_test_hash_key_1", sortkeys, new_kvs);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(2, new_kvs.size());
    auto it = new_kvs.begin();
    ASSERT_EQ("basic_test_sort_key_3", it->first);
    ASSERT_EQ("basic_test_value_3", it->second);
    it++;
    ASSERT_EQ("basic_test_sort_key_4", it->first);
    ASSERT_EQ("basic_test_value_4", it->second);

    // multi_del_async
    sortkeys.clear();
    sortkeys.insert("basic_test_sort_key_3");
    sortkeys.insert("basic_test_sort_key_4");
    callbacked.store(false, std::memory_order_seq_cst);
    client->async_multi_del("basic_test_hash_key_1",
                            sortkeys,
                            [&](int err, int64_t deleted_count, internal_info &&info) {
                                ASSERT_EQ(PERR_OK, err);
                                ASSERT_GT(info.app_id, 0);
                                ASSERT_GT(info.partition_index, 0);
                                ASSERT_GT(info.decree, 0);
                                ASSERT_FALSE(info.server.empty());
                                ASSERT_EQ(2, deleted_count);
                                callbacked.store(true, std::memory_order_seq_cst);
                            });
    while (!callbacked.load(std::memory_order_seq_cst))
        usleep(100);

    // sortkey_count
    ret = client->sortkey_count("basic_test_hash_key_1", count);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(0, count);
}

void test_basic_global_init() {}
