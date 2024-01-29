/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "perf_counter/perf_counters.h"

#include <stdio.h>
#include <map>

#include "common/json_helper.h"
#include "gtest/gtest.h"
#include "perf_counter/perf_counter.h"
#include "perf_counter/perf_counter_utils.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "utils/blob.h"

namespace dsn {

TEST(perf_counters_test, counter_create_remove)
{
    perf_counter_ptr p;

    p = perf_counters::instance().get_global_counter(
        "app", "test", "number_counter", COUNTER_TYPE_NUMBER, "", false);
    ASSERT_EQ(nullptr, p);
    p = perf_counters::instance().get_global_counter(
        "app", "test", "number_counter", COUNTER_TYPE_NUMBER, "", true);
    ASSERT_NE(nullptr, p);
    p = perf_counters::instance().get_global_counter(
        "app", "test", "number_counter", COUNTER_TYPE_NUMBER, "", false);
    ASSERT_NE(nullptr, p);

    p = perf_counters::instance().get_global_counter(
        "app", "test", "volatile_number_counter", COUNTER_TYPE_VOLATILE_NUMBER, "", false);
    ASSERT_EQ(nullptr, p);
    p = perf_counters::instance().get_global_counter(
        "app", "test", "volatile_number_counter", COUNTER_TYPE_VOLATILE_NUMBER, "", true);
    ASSERT_NE(nullptr, p);
    p = perf_counters::instance().get_global_counter(
        "app", "test", "volatile_number_counter", COUNTER_TYPE_VOLATILE_NUMBER, "", false);
    ASSERT_NE(nullptr, p);

    p = perf_counters::instance().get_global_counter(
        "app", "test", "rate_counter", COUNTER_TYPE_RATE, "", false);
    ASSERT_EQ(nullptr, p);
    p = perf_counters::instance().get_global_counter(
        "app", "test", "rate_counter", COUNTER_TYPE_RATE, "", true);
    ASSERT_NE(nullptr, p);
    p = perf_counters::instance().get_global_counter(
        "app", "test", "rate_counter", COUNTER_TYPE_RATE, "", false);
    ASSERT_NE(nullptr, p);

    ASSERT_FALSE(perf_counters::instance().remove_counter("number_counter"));
    ASSERT_FALSE(perf_counters::instance().remove_counter("unexist_counter"));

    ASSERT_TRUE(perf_counters::instance().remove_counter("app*test*number_counter"));
    ASSERT_TRUE(perf_counters::instance().remove_counter("app*test*number_counter"));
    ASSERT_FALSE(perf_counters::instance().remove_counter("app*test*number_counter"));
    p = perf_counters::instance().get_global_counter(
        "app", "test", "number_counter", COUNTER_TYPE_NUMBER, "", false);
    ASSERT_EQ(nullptr, p);

    ASSERT_TRUE(perf_counters::instance().remove_counter("app*test*volatile_number_counter"));
    ASSERT_TRUE(perf_counters::instance().remove_counter("app*test*volatile_number_counter"));
    ASSERT_FALSE(perf_counters::instance().remove_counter("app*test*volatile_number_counter"));
    p = perf_counters::instance().get_global_counter(
        "app", "test", "volatile_number_counter", COUNTER_TYPE_VOLATILE_NUMBER, "", false);
    ASSERT_EQ(nullptr, p);

    ASSERT_TRUE(perf_counters::instance().remove_counter("app*test*rate_counter"));
    ASSERT_TRUE(perf_counters::instance().remove_counter("app*test*rate_counter"));
    ASSERT_FALSE(perf_counters::instance().remove_counter("app*test*rate_counter"));
    p = perf_counters::instance().get_global_counter(
        "app", "test", "rate_counter", COUNTER_TYPE_RATE, "", false);
    ASSERT_EQ(nullptr, p);

    p = perf_counters::instance().get_global_counter(
        "app", "test", "unexist_counter", COUNTER_TYPE_NUMBER, "", false);
    ASSERT_EQ(nullptr, p);
    ASSERT_FALSE(perf_counters::instance().remove_counter("app*test*unexist_counter"));
}

template <typename K, typename V>
bool check_map_contains(const std::map<K, V> &super, const std::map<K, V> &sub)
{
    for (const auto &kv : sub) {
        auto it = super.find(kv.first);
        if (it == super.end()) {
            return false;
        }
        if (it->second != kv.second) {
            return false;
        }
    }
    return true;
}

TEST(perf_counters_test, snapshot)
{
    std::map<std::string, dsn_perf_counter_type_t> expected;

    std::map<std::string, dsn_perf_counter_type_t> counter_keys;
    perf_counters::instance().take_snapshot();
    perf_counters::snapshot_iterator iter =
        [&counter_keys](const perf_counters::counter_snapshot &cs) mutable {
            counter_keys.emplace(cs.name, cs.type);
        };

    counter_keys.clear();
    expected = {
        {"replica*server*memused.virt(MB)", COUNTER_TYPE_NUMBER},
        {"replica*server*memused.res(MB)", COUNTER_TYPE_NUMBER},
    };
    perf_counters::instance().iterate_snapshot(iter);
    // in the beginning, builtin counters are in counter_list
    ASSERT_TRUE(check_map_contains(counter_keys, expected));

    dsn::perf_counter_wrapper c1;
    c1.init_global_counter("a", "s", "test_counter", COUNTER_TYPE_NUMBER, "");
    dsn::perf_counter_wrapper c2;
    c2.init_global_counter("a", "s", "test_counter", COUNTER_TYPE_NUMBER, "");

    dsn::perf_counter_wrapper c3;
    c3.init_global_counter("b", "s", "test_counter", COUNTER_TYPE_VOLATILE_NUMBER, "");
    dsn::perf_counter_wrapper c4;
    c4.init_global_counter("b", "s", "test_counter", COUNTER_TYPE_VOLATILE_NUMBER, "");

    // snapshot will contain new counters
    perf_counters::instance().take_snapshot();
    counter_keys.clear();
    expected = {
        {"replica*server*memused.virt(MB)", COUNTER_TYPE_NUMBER},
        {"replica*server*memused.res(MB)", COUNTER_TYPE_NUMBER},
        {"a*s*test_counter", COUNTER_TYPE_NUMBER},
        {"b*s*test_counter", COUNTER_TYPE_VOLATILE_NUMBER},
    };
    perf_counters::instance().iterate_snapshot(iter);
    ASSERT_TRUE(check_map_contains(counter_keys, expected));

    dsn::perf_counter_wrapper c5;
    c5.init_global_counter("c", "s", "test_counter", COUNTER_TYPE_RATE, "");
    dsn::perf_counter_wrapper c6;
    c6.init_global_counter("c", "s", "test_counter", COUNTER_TYPE_RATE, "");

    dsn::perf_counter_wrapper c7;
    c7.init_global_counter("d", "s", "test_counter", COUNTER_TYPE_NUMBER_PERCENTILES, "");
    dsn::perf_counter_wrapper c8;
    c8.init_global_counter("d", "s", "test_counter", COUNTER_TYPE_NUMBER_PERCENTILES, "");

    // new counters won't be contained in snapshot if you don't call "take snapshot"
    counter_keys.clear();
    expected = {
        {"replica*server*memused.virt(MB)", COUNTER_TYPE_NUMBER},
        {"replica*server*memused.res(MB)", COUNTER_TYPE_NUMBER},
        {"a*s*test_counter", COUNTER_TYPE_NUMBER},
        {"b*s*test_counter", COUNTER_TYPE_VOLATILE_NUMBER},
    };
    perf_counters::instance().iterate_snapshot(iter);
    ASSERT_TRUE(check_map_contains(counter_keys, expected));
    ASSERT_TRUE(counter_keys.find("c*s*test_counter") == counter_keys.end());
    ASSERT_TRUE(counter_keys.find("d*s*test_counter") == counter_keys.end());

    // after taking snapshot, new counters will be contained
    counter_keys.clear();
    expected = {
        {"replica*server*memused.virt(MB)", COUNTER_TYPE_NUMBER},
        {"replica*server*memused.res(MB)", COUNTER_TYPE_NUMBER},
        {"a*s*test_counter", COUNTER_TYPE_NUMBER},
        {"b*s*test_counter", COUNTER_TYPE_VOLATILE_NUMBER},
        {"c*s*test_counter", COUNTER_TYPE_RATE},
        {"d*s*test_counter", COUNTER_TYPE_NUMBER_PERCENTILES},
    };
    perf_counters::instance().take_snapshot();
    perf_counters::instance().iterate_snapshot(iter);
    ASSERT_TRUE(check_map_contains(counter_keys, expected));

    c1.clear();
    c2.clear();
    c3.clear();
    c4.clear();

    // although remove counters, but snapshot won't been affected if you don't call take snapshot
    counter_keys.clear();
    perf_counters::instance().iterate_snapshot(iter);
    expected = {
        {"replica*server*memused.virt(MB)", COUNTER_TYPE_NUMBER},
        {"replica*server*memused.res(MB)", COUNTER_TYPE_NUMBER},
        {"a*s*test_counter", COUNTER_TYPE_NUMBER},
        {"b*s*test_counter", COUNTER_TYPE_VOLATILE_NUMBER},
        {"c*s*test_counter", COUNTER_TYPE_RATE},
        {"d*s*test_counter", COUNTER_TYPE_NUMBER_PERCENTILES},
    };
    ASSERT_TRUE(check_map_contains(counter_keys, expected));

    // after take snapshot, removed counters will be removed in snapshot
    perf_counters::instance().take_snapshot();
    counter_keys.clear();
    perf_counters::instance().iterate_snapshot(iter);
    expected = {
        {"replica*server*memused.virt(MB)", COUNTER_TYPE_NUMBER},
        {"replica*server*memused.res(MB)", COUNTER_TYPE_NUMBER},
        {"c*s*test_counter", COUNTER_TYPE_RATE},
        {"d*s*test_counter", COUNTER_TYPE_NUMBER_PERCENTILES},
    };
    ASSERT_TRUE(check_map_contains(counter_keys, expected));
    ASSERT_TRUE(counter_keys.find("a*s*test_counter") == counter_keys.end());
    ASSERT_TRUE(counter_keys.find("b*s*test_counter") == counter_keys.end());

    // query snapshot
    std::vector<std::string> target_keys = {
        "a*s*test_counter", "c*s*test_counter", "b*s*test_counter", "d*s*test_counter"};
    expected = {
        {"c*s*test_counter", COUNTER_TYPE_RATE},
        {"d*s*test_counter", COUNTER_TYPE_NUMBER_PERCENTILES},
    };

    counter_keys.clear();
    perf_counters::instance().query_snapshot(target_keys, iter, nullptr);
    ASSERT_EQ(2, counter_keys.size());
    ASSERT_EQ(expected, counter_keys);

    counter_keys.clear();
    std::vector<bool> found;
    perf_counters::instance().query_snapshot(target_keys, iter, &found);
    ASSERT_EQ(4, found.size());
    std::vector<bool> expected_found = {false, true, false, true};
    ASSERT_EQ(expected_found, found);
    ASSERT_EQ(expected, counter_keys);
}

TEST(perf_counters_test, query_snapshot_by_regexp)
{
    dsn::perf_counter_wrapper c1;
    c1.init_global_counter("a", "s", "test_counter", COUNTER_TYPE_NUMBER, "");
    dsn::perf_counter_wrapper c2;
    c2.init_global_counter("a", "s", "test_counter", COUNTER_TYPE_NUMBER, "");

    dsn::perf_counter_wrapper c3;
    c3.init_global_counter("b", "s", "test_counter", COUNTER_TYPE_VOLATILE_NUMBER, "");
    dsn::perf_counter_wrapper c4;
    c4.init_global_counter("b", "s", "test_counter", COUNTER_TYPE_VOLATILE_NUMBER, "");

    dsn::perf_counter_wrapper c5;
    c5.init_global_counter("c", "s", "test_counter", COUNTER_TYPE_RATE, "");
    dsn::perf_counter_wrapper c6;
    c6.init_global_counter("c", "s", "test_counter", COUNTER_TYPE_RATE, "");

    dsn::perf_counter_wrapper c7;
    c7.init_global_counter("d", "s", "test_counter", COUNTER_TYPE_NUMBER_PERCENTILES, "");
    dsn::perf_counter_wrapper c8;
    c8.init_global_counter("d", "s", "test_counter", COUNTER_TYPE_NUMBER_PERCENTILES, "");

    perf_counters::instance().take_snapshot();
    std::string result = perf_counters::instance().list_snapshot_by_regexp({".*\\*s\\*.*"});

    dsn::perf_counter_info info;
    dsn::json::json_forwarder<dsn::perf_counter_info>::decode(
        dsn::blob(result.c_str(), 0, result.size()), info);
    ASSERT_STREQ("OK", info.result.c_str());
    ASSERT_GT(info.timestamp, 0);
    ASSERT_TRUE(!info.timestamp_str.empty());
    printf("got timestamp: %s\n", info.timestamp_str.c_str());
    ASSERT_EQ(4 + 1, info.counters.size()); // add 1 for p999 counter

    std::map<std::string, std::string> expected = {
        {"a*s*test_counter", dsn_counter_type_to_string(COUNTER_TYPE_NUMBER)},
        {"b*s*test_counter", dsn_counter_type_to_string(COUNTER_TYPE_VOLATILE_NUMBER)},
        {"c*s*test_counter", dsn_counter_type_to_string(COUNTER_TYPE_RATE)},
        {"d*s*test_counter", dsn_counter_type_to_string(COUNTER_TYPE_NUMBER_PERCENTILES)},
        {"d*s*test_counter.p999", dsn_counter_type_to_string(COUNTER_TYPE_NUMBER_PERCENTILES)},
    };
    std::map<std::string, std::string> actual;
    for (const dsn::perf_counter_metric &m : info.counters) {
        actual.emplace(m.name, m.type);
    }
    ASSERT_EQ(expected, actual);

    result = perf_counters::instance().list_snapshot_by_regexp({"hahaha"});
    dsn::json::json_forwarder<dsn::perf_counter_info>::decode(
        dsn::blob(result.c_str(), 0, result.size()), info);
    ASSERT_STREQ("OK", info.result.c_str());
    ASSERT_GT(info.timestamp, 0);
    ASSERT_TRUE(!info.timestamp_str.empty());
    printf("got timestamp: %s\n", info.timestamp_str.c_str());
    ASSERT_TRUE(info.counters.empty());

    result = perf_counters::instance().list_snapshot_by_regexp({""});
    dsn::json::json_forwarder<dsn::perf_counter_info>::decode(
        dsn::blob(result.c_str(), 0, result.size()), info);
    ASSERT_STREQ("OK", info.result.c_str());
    ASSERT_GT(info.timestamp, 0);
    ASSERT_TRUE(!info.timestamp_str.empty());
    printf("got timestamp: %s\n", info.timestamp_str.c_str());
    ASSERT_TRUE(info.counters.empty());
}

} // namespace dsn
