// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "geo/src/geo_client.h"
#include <gtest/gtest.h>
#include <dsn/utility/strings.h>
#include <s2/s2cap.h>
#include <s2/s2testing.h>
#include <dsn/utility/string_conv.h>

namespace pegasus {
namespace geo {

// value format:
// "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|-0.356396|39.469644|24.043028|4.15921|0|-1"
auto extractor = [](const std::string &value, S2LatLng &latlng) {
    std::vector<std::string> data;
    dsn::utils::split_args(value.c_str(), data, '|');
    if (data.size() <= 6) {
        return pegasus::PERR_INVALID_VALUE;
    }

    std::string lat = data[4];
    std::string lng = data[5];
    latlng = S2LatLng::FromDegrees(strtod(lat.c_str(), nullptr), strtod(lng.c_str(), nullptr));

    return pegasus::PERR_OK;
};

class geo_client_test : public ::testing::Test
{
public:
    void SetUp() override
    {
        _geo_client =
            pegasus::geo::geo_client("config.ini", "onebox", "temp", "temp_geo", extractor);
    }

    void TearDown() override {}

public:
    pegasus::geo::geo_client _geo_client;
};

TEST_F(geo_client_test, set)
{
    double lat_degrees = 12.345;
    double lng_degrees = 67.890;
    std::string test_hash_key = "test_hash_key_set";
    std::string test_sort_key = "test_sort_key_set";
    std::string test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                             std::to_string(lat_degrees) + "|" + std::to_string(lng_degrees) +
                             "|24.043028|4.15921|0|-1";

    // geo set
    int ret = _geo_client.set(test_hash_key, test_sort_key, test_value);
    ASSERT_EQ(ret, pegasus::PERR_OK);

    // get from common db
    std::string value;
    ret = _geo_client._common_data_client->get(test_hash_key, test_sort_key, value);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_EQ(value, test_value);

    // search the inserted data
    {
        std::list<geo::SearchResult> result;
        ret = _geo_client.search_radial(
            test_hash_key, test_sort_key, 1, 1, geo::geo_client::SortType::random, 500, result);
        ASSERT_EQ(ret, pegasus::PERR_OK);
        ASSERT_EQ(result.size(), 1);
        ASSERT_LE(result.front().distance, 0.000001);
        ASSERT_EQ(result.front().hash_key, test_hash_key);
        ASSERT_EQ(result.front().sort_key, test_sort_key);
        ASSERT_EQ(result.front().value, test_value);
    }

    {
        std::list<geo::SearchResult> result;
        ret = _geo_client.search_radial(
            lat_degrees, lng_degrees, 1, 1, geo::geo_client::SortType::random, 500, result);
        ASSERT_EQ(ret, pegasus::PERR_OK);
        ASSERT_EQ(result.size(), 1);
        ASSERT_LE(result.front().distance, 0.000001);
        ASSERT_EQ(result.front().hash_key, test_hash_key);
        ASSERT_EQ(result.front().sort_key, test_sort_key);
        ASSERT_EQ(result.front().value, test_value);
    }
}

TEST_F(geo_client_test, set_geo_data)
{
    double lat_degrees = 56.789;
    double lng_degrees = 12.345;
    std::string test_hash_key = "test_hash_key_set_geo_data";
    std::string test_sort_key = "test_sort_key_set_geo_data";
    std::string test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                             std::to_string(lat_degrees) + "|" + std::to_string(lng_degrees) +
                             "|24.043028|4.15921|0|-1";

    // geo set_geo_data
    int ret = _geo_client.set_geo_data(test_hash_key, test_sort_key, test_value);
    ASSERT_EQ(ret, pegasus::PERR_OK);

    // get from common db
    std::string value;
    ret = _geo_client._common_data_client->get(test_hash_key, test_sort_key, value);
    ASSERT_EQ(ret, pegasus::PERR_NOT_FOUND);

    // search the inserted data
    std::list<geo::SearchResult> result;
    ret = _geo_client.search_radial(
        test_hash_key, test_sort_key, 1, 1, geo::geo_client::SortType::random, 500, result);
    ASSERT_EQ(ret, pegasus::PERR_NOT_FOUND);

    ret = _geo_client.search_radial(
        lat_degrees, lng_degrees, 1, 1, geo::geo_client::SortType::random, 500, result);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_EQ(result.size(), 1);
    ASSERT_LE(result.front().distance, 0.000001);
    ASSERT_EQ(result.front().hash_key, test_hash_key);
    ASSERT_EQ(result.front().sort_key, test_sort_key);
    ASSERT_EQ(result.front().value, test_value);
}

TEST_F(geo_client_test, normalize_result_random_order)
{
    std::list<std::vector<geo::SearchResult>> results;
    geo::SearchResult r1(1.1, 1.1, 1, "test_hash_key_1", "test_sort_key_1", "value_1");
    results.push_back({r1});
    int count = 100;
    std::list<geo::SearchResult> result;
    _geo_client.normalize_result(
        std::move(results), count, geo::geo_client::SortType::random, result);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.front(), r1);

    geo::SearchResult r2(2.2, 2.2, 2, "test_hash_key_2", "test_sort_key_2", "value_2");
    results.push_back({r2});
    _geo_client.normalize_result(std::move(results), 1, geo::geo_client::SortType::random, result);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.front(), r1);

    _geo_client.normalize_result(
        std::move(results), count, geo::geo_client::SortType::random, result);
    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result.front(), r1);
    ASSERT_EQ(result.back(), r2);

    _geo_client.normalize_result(std::move(results), -1, geo::geo_client::SortType::random, result);
    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result.front(), r1);
    ASSERT_EQ(result.back(), r2);
}

TEST_F(geo_client_test, normalize_result_distance_order)
{
    std::list<std::vector<geo::SearchResult>> results;
    geo::SearchResult r2(2.2, 2.2, 2, "test_hash_key_2", "test_sort_key_2", "value_2");
    results.push_back({r2});
    int count = 100;
    std::list<geo::SearchResult> result;
    _geo_client.normalize_result(
        std::move(results), count, geo::geo_client::SortType::asc, result);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.front(), r2);

    geo::SearchResult r1(1.1, 1.1, 1, "test_hash_key_1", "test_sort_key_1", "value_1");
    results.push_back({r1});
    _geo_client.normalize_result(std::move(results), 1, geo::geo_client::SortType::asc, result);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.front(), r1);

    _geo_client.normalize_result(
        std::move(results), count, geo::geo_client::SortType::asc, result);
    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result.front(), r1);
    ASSERT_EQ(result.back(), r2);

    _geo_client.normalize_result(
        std::move(results), -1, geo::geo_client::SortType::asc, result);
    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result.front(), r1);
    ASSERT_EQ(result.back(), r2);
}

TEST_F(geo_client_test, large_cap)
{
    double lat_degrees = 40.039752;
    double lng_degrees = 116.332557;
    double radius_m = 10000;
    int test_data_count = 10000;
    std::string test_hash_key = "test_hash_key_large_cap";
    std::string test_sort_key = "test_sort_key_large_cap";
    std::string test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" +
                             std::to_string(lat_degrees) + "|" + std::to_string(lng_degrees) +
                             "|24.043028|4.15921|0|-1";

    S2Cap cap;
    _geo_client.search_cap(S2LatLng::FromDegrees(lat_degrees, lng_degrees), radius_m, cap);
    for (int i = 0; i < test_data_count; ++i) {
        S2LatLng latlng(S2Testing::SamplePoint(cap));
        ASSERT_TRUE(cap.Contains(latlng.ToPoint()));
        std::string id = std::to_string(i);
        std::string value = id + "|2018-06-05 12:00:00|2018-06-05 13:00:00|abcdefg|" +
                            std::to_string(latlng.lat().degrees()) + "|" +
                            std::to_string(latlng.lng().degrees()) + "|123.456|456.789|0|-1";

        int ret = _geo_client.set(id, "", value, 1000);
        ASSERT_EQ(ret, pegasus::PERR_OK);
    }

    {
        // search the inserted data
        std::list<geo::SearchResult> result;
        int ret = _geo_client.search_radial(
            "0", "", radius_m * 2, -1, geo::geo_client::SortType::asc, 5000, result);
        ASSERT_EQ(ret, pegasus::PERR_OK);
        ASSERT_GE(result.size(), test_data_count);
        geo::SearchResult last;
        for (const auto &r : result) {
            ASSERT_LE(last.distance, r.distance);
            uint64_t val;
            ASSERT_TRUE(dsn::buf2uint64(r.hash_key.c_str(), val));
            ASSERT_LE(0, val);
            ASSERT_LE(val, test_data_count);
            ASSERT_NE(last.hash_key, r.hash_key);
            ASSERT_EQ(r.sort_key, "");
            last = r;
        }
    }

    {
        // search the inserted data
        std::list<geo::SearchResult> result;
        int ret = _geo_client.search_radial(lat_degrees,
                                            lng_degrees,
                                            radius_m,
                                            -1,
                                            geo::geo_client::SortType::asc,
                                            5000,
                                            result);
        ASSERT_EQ(ret, pegasus::PERR_OK);
        ASSERT_GE(result.size(), test_data_count);
        geo::SearchResult last;
        for (const auto &r : result) {
            ASSERT_LE(last.distance, r.distance);
            uint64_t val;
            ASSERT_TRUE(dsn::buf2uint64(r.hash_key.c_str(), val));
            ASSERT_LE(0, val);
            ASSERT_LE(val, test_data_count);
            ASSERT_NE(last.hash_key, r.hash_key);
            ASSERT_EQ(r.sort_key, "");
            last = r;
        }
    }
}
} // namespace geo
} // namespace pegasus
