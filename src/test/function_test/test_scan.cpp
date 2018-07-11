// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <cstdlib>
#include <string>
#include <vector>
#include <map>

#include <dsn/service_api_c.h>
#include <unistd.h>
#include <pegasus/client.h>
#include <gtest/gtest.h>

using namespace ::pegasus;

extern pegasus_client *client;
static const char CCH[] = "_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static char buffer[256];
static std::map<std::string, std::map<std::string, std::string>> base;
static std::string expected_hash_key;

static const std::string random_string()
{
    int pos = rand() % sizeof(buffer);
    buffer[pos] = CCH[rand() % sizeof(CCH)];
    int length = rand() % sizeof(buffer) + 1;
    if (pos + length < sizeof(buffer))
        return std::string(buffer + pos, length);
    else
        return std::string(buffer + pos, sizeof(buffer) - pos) +
               std::string(buffer, length + pos - sizeof(buffer));
}

static void check_and_put(std::map<std::string, std::map<std::string, std::string>> &data,
                          const std::string &hash_key,
                          const std::string &sort_key,
                          const std::string &value)
{
    auto it1 = data.find(hash_key);
    if (it1 != data.end()) {
        auto it2 = it1->second.find(sort_key);
        ASSERT_EQ(it1->second.end(), it2)
            << "Duplicate: hash_key=" << hash_key << ", sort_key=" << sort_key
            << ", old_value=" << it2->second << ", new_value=" << value;
    }
    data[hash_key][sort_key] = value;
}

static void check_and_put(std::map<std::string, std::string> &data,
                          const std::string &hash_key,
                          const std::string &sort_key,
                          const std::string &value)
{
    auto it1 = data.find(sort_key);
    ASSERT_EQ(data.end(), it1) << "Duplicate: hash_key=" << hash_key << ", sort_key=" << sort_key
                               << ", old_value=" << it1->second << ", new_value=" << value;
    data[sort_key] = value;
}

static void compare(const std::map<std::string, std::string> &data,
                    const std::map<std::string, std::string> &base,
                    const std::string hash_key)
{
    for (auto it1 = data.begin(), it2 = base.begin();; ++it1, ++it2) {
        if (it1 == data.end()) {
            ASSERT_EQ(base.end(), it2) << "Only in base: hash_key=" << hash_key
                                       << ", sort_key=" << it2->first << ", value=" << it2->second;
            break;
        }
        ASSERT_NE(base.end(), it2) << "Only in data: hash_key=" << hash_key
                                   << ", sort_key=" << it1->first << ", value=" << it1->second;
        ASSERT_EQ(*it2, *it1) << "Diff: hash_key=" << hash_key << ", data_sort_key=" << it1->first
                              << ", data_value=" << it1->second << ", base_sort_key=" << it2->first
                              << ", base_value=" << it2->second;
    }

    dinfo("Data and base are the same.");
}

static void compare(std::map<std::string, std::map<std::string, std::string>> &data,
                    std::map<std::string, std::map<std::string, std::string>> &base)
{
    for (auto it1 = data.begin(), it2 = base.begin();; ++it1, ++it2) {
        if (it1 == data.end()) {
            ASSERT_EQ(base.end(), it2) << "Only in base: hash_key=" << it2->first;
            break;
        }
        ASSERT_NE(base.end(), it2) << "Only in data: hash_key=" << it1->first;
        ASSERT_EQ(it1->first, it2->first) << "Diff: data_hash_key=" << it1->first
                                          << ", base_hash_key=" << it2->first;
        for (auto it3 = it1->second.begin(), it4 = it2->second.begin();; ++it3, ++it4) {
            if (it3 == it1->second.end()) {
                ASSERT_EQ(it2->second.end(), it4) << "Only in base: hash_key=" << it2->first
                                                  << ", sort_key=" << it4->first
                                                  << ", value=" << it4->second;
                break;
            }
            ASSERT_NE(it2->second.end(), it4) << "Only in data: hash_key=" << it1->first
                                              << ", sort_key=" << it3->first
                                              << ", value=" << it3->second;
            ASSERT_EQ(*it3, *it4) << "Diff: hash_key=" << it1->first
                                  << ", data_sort_key=" << it3->first
                                  << ", data_value=" << it3->second
                                  << ", base_sort_key=" << it4->first
                                  << ", base_value=" << it4->second;
        }
    }

    dinfo("Data and base are the same.");
}

static void clear_database()
{
    ddebug("CLEARING_DATABASE...");
    pegasus_client::scan_options option;
    std::vector<pegasus_client::pegasus_scanner *> scanners;
    int ret = client->get_unordered_scanners(1, option, scanners);
    ASSERT_EQ(0, ret) << "Error occurred when get scanners, error="
                      << client->get_error_string(ret);
    ASSERT_EQ(1, scanners.size());
    ASSERT_NE(nullptr, scanners[0]);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    while (!(ret = (scanners[0]->next(hash_key, sort_key, value)))) {
        int r = client->del(hash_key, sort_key);
        ASSERT_EQ(0, r) << "Error occurred when del, hash_key=" << hash_key
                        << ", sort_key=" << sort_key << ", error=" << client->get_error_string(r);
    }
    delete scanners[0];

    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when next() in clearing database. error="
                                       << client->get_error_string(ret);
    ret = client->get_unordered_scanners(1, option, scanners);
    ASSERT_EQ(0, ret) << "Error occurred when get scanners, error="
                      << client->get_error_string(ret);
    ASSERT_EQ(1, scanners.size());
    ASSERT_NE(nullptr, scanners[0]);

    ret = scanners[0]->next(hash_key, sort_key, value);
    delete scanners[0];
    ASSERT_NE(0, ret) << "Database is cleared but not empty, hash_key=" << hash_key
                      << ", sort_key=" << sort_key;
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when clearing database. error="
                                       << client->get_error_string(ret);
    ddebug("Database cleared.");
}

class test_scan : public testing::Test
{
public:
    virtual void SetUp()
    {
        ddebug("SetUp...");
        clear_database();

        srand(time(nullptr));
        for (int i = 0; i < sizeof(buffer); i++)
            buffer[i] = CCH[rand() % sizeof(CCH)];

        expected_hash_key = random_string();
        std::string hash_key;
        std::string sort_key;
        std::string value;
        for (int i = 0; i < 1000 || base[expected_hash_key].size() < 1000; i++) {
            sort_key = random_string();
            value = random_string();
            client->set(expected_hash_key, sort_key, value);
            base[expected_hash_key][sort_key] = value;
        }

        for (int i = 0; i < 1000 || base.size() < 1000; i++) {
            hash_key = random_string();
            for (int j = 0; j < 10 || base[hash_key].size() < 10; j++) {
                sort_key = random_string();
                value = random_string();

                client->set(hash_key, sort_key, value);
                base[hash_key][sort_key] = value;
            }
        }
    }

    virtual void TearDown() override
    {
        ddebug("TearDown...");
        clear_database();
    }
};

TEST(test_scan, ALL_SORT_KEY)
{
    ddebug("TESTING_HASH_SCAN, ALL SORT_KEYS ....");
    pegasus_client::scan_options options;
    std::map<std::string, std::string> data;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = client->get_scanner(expected_hash_key, "", "", options, scanner);
    ASSERT_EQ(0, ret) << "Error occurred when getting scanner. error="
                      << client->get_error_string(ret);
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    while (!(ret = (scanner->next(hash_key, sort_key, value)))) {
        ASSERT_EQ(expected_hash_key, hash_key);
        check_and_put(data, expected_hash_key, sort_key, value);
    }
    delete scanner;
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client->get_error_string(ret);
    compare(data, base[expected_hash_key], expected_hash_key);
}

TEST(test_scan, BOUND_INCLUSIVE)
{
    ddebug("TESTING_HASH_SCAN, [start, stop]...");
    auto it1 = base[expected_hash_key].begin();
    for (int i = random() % 500; i >= 0; i--)
        ++it1;
    std::string start = it1->first;
    auto it2 = it1;
    for (int i = random() % 400 + 50; i >= 0; i--)
        ++it2;
    std::string stop = it2->first;

    pegasus_client::scan_options options;
    options.start_inclusive = true;
    options.stop_inclusive = true;
    std::map<std::string, std::string> data;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = client->get_scanner(expected_hash_key, start, stop, options, scanner);
    ASSERT_EQ(0, ret) << "Error occurred when getting scanner. error="
                      << client->get_error_string(ret);
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    while (!(ret = (scanner->next(hash_key, sort_key, value)))) {
        ASSERT_EQ(expected_hash_key, hash_key);
        check_and_put(data, expected_hash_key, sort_key, value);
    }
    delete scanner;
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client->get_error_string(ret);
    ++it2;
    compare(data, std::map<std::string, std::string>(it1, it2), expected_hash_key);
}

TEST(test_scan, BOUND_EXCLUSIVE)
{
    ddebug("TESTING_HASH_SCAN, (start, stop)...");
    auto it1 = base[expected_hash_key].begin();
    for (int i = random() % 500; i >= 0; i--)
        ++it1;
    std::string start = it1->first;
    auto it2 = it1;
    for (int i = random() % 400 + 50; i >= 0; i--)
        ++it2;
    std::string stop = it2->first;

    pegasus_client::scan_options options;
    options.start_inclusive = false;
    options.stop_inclusive = false;
    std::map<std::string, std::string> data;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = client->get_scanner(expected_hash_key, start, stop, options, scanner);
    ASSERT_EQ(0, ret) << "Error occurred when getting scanner. error="
                      << client->get_error_string(ret);
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    while (!(ret = (scanner->next(hash_key, sort_key, value)))) {
        ASSERT_EQ(expected_hash_key, hash_key);
        check_and_put(data, expected_hash_key, sort_key, value);
    }
    delete scanner;
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client->get_error_string(ret);
    ++it1;
    compare(data, std::map<std::string, std::string>(it1, it2), expected_hash_key);
}

TEST(test_scan, ONE_POINT)
{
    ddebug("TESTING_HASH_SCAN, [start, start]...");
    auto it1 = base[expected_hash_key].begin();
    for (int i = random() % 800; i >= 0; i--)
        ++it1;
    std::string start = it1->first;

    pegasus_client::scan_options options;
    options.start_inclusive = true;
    options.stop_inclusive = true;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = client->get_scanner(expected_hash_key, start, start, options, scanner);
    ASSERT_EQ(0, ret) << "Error occurred when getting scanner. error="
                      << client->get_error_string(ret);
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    ret = scanner->next(hash_key, sort_key, value);
    ASSERT_EQ(0, ret) << "Error occurred when scan. error=" << client->get_error_string(ret);
    ASSERT_EQ(expected_hash_key, hash_key);
    ASSERT_EQ(start, sort_key);
    ASSERT_EQ(it1->second, value);
    ret = scanner->next(hash_key, sort_key, value);
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client->get_error_string(ret);
    delete scanner;
}

TEST(test_scan, HALF_INCLUSIVE)
{
    ddebug("TESTING_HASH_SCAN, [start, start)...");
    auto it1 = base[expected_hash_key].begin();
    for (int i = random() % 800; i >= 0; i--)
        ++it1;
    std::string start = it1->first;

    pegasus_client::scan_options options;
    options.start_inclusive = true;
    options.stop_inclusive = false;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = client->get_scanner(expected_hash_key, start, start, options, scanner);
    ASSERT_EQ(0, ret) << "Error occurred when getting scanner. error="
                      << client->get_error_string(ret);
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    ret = scanner->next(hash_key, sort_key, value);
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client->get_error_string(ret);
    delete scanner;
}

TEST(test_scan, VOID_SPAN)
{
    ddebug("TESTING_HASH_SCAN, [stop, start]...");
    auto it1 = base[expected_hash_key].begin();
    for (int i = random() % 500; i >= 0; i--)
        ++it1;
    std::string start = it1->first;
    for (int i = random() % 400 + 50; i >= 0; i--)
        ++it1;
    std::string stop = it1->first;

    pegasus_client::scan_options options;
    options.start_inclusive = true;
    options.stop_inclusive = true;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = client->get_scanner(expected_hash_key, stop, start, options, scanner);
    ASSERT_EQ(0, ret) << "Error occurred when getting scanner. error="
                      << client->get_error_string(ret);
    ASSERT_NE(nullptr, scanner);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    ret = scanner->next(hash_key, sort_key, value);
    ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                       << client->get_error_string(ret);
    delete scanner;
}

TEST(test_scan, OVERALL)
{
    ddebug("TEST OVERALL_SCAN...");
    pegasus_client::scan_options options;
    std::map<std::string, std::map<std::string, std::string>> data;
    std::vector<pegasus_client::pegasus_scanner *> scanners;
    int ret = client->get_unordered_scanners(3, options, scanners);
    ASSERT_EQ(0, ret) << "Error occurred when getting scanner. error="
                      << client->get_error_string(ret);
    ASSERT_LE(scanners.size(), 3);

    std::string hash_key;
    std::string sort_key;
    std::string value;
    for (int i = scanners.size() - 1; i >= 0; i--) {
        pegasus_client::pegasus_scanner *scanner = scanners[i];
        ASSERT_NE(nullptr, scanner);
        while (!(ret = (scanner->next(hash_key, sort_key, value))))
            check_and_put(data, hash_key, sort_key, value);
        ASSERT_EQ(PERR_SCAN_COMPLETE, ret) << "Error occurred when scan. error="
                                           << client->get_error_string(ret);
        delete scanner;
    }
    compare(data, base);
}
