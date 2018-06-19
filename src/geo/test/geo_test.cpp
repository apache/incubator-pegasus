// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "geo/lib/geo_client.h"
#include <gtest/gtest.h>
#include <dsn/utility/strings.h>
#include <s2/s2cap.h>
#include <s2/s2testing.h>
#include <dsn/utility/string_conv.h>
#include <s2/s2earth.h>

namespace pegasus {
namespace geo {

class geo_client_test : public ::testing::Test
{
public:
    geo_client_test()
    {
        _geo_client.reset(new pegasus::geo::geo_client(
            "config.ini", "onebox", "temp", "temp_geo", new latlng_extractor_for_lbs()));
    }

    pegasus::geo::geo_client *geo_client() { return _geo_client.get(); }

    pegasus_client *common_data_client() { return _geo_client->_common_data_client; }

    void normalize_result(const std::list<std::vector<SearchResult>> &results,
                          int count,
                          geo::geo_client::SortType sort_type,
                          std::list<SearchResult> &result)
    {
        _geo_client->normalize_result(results, count, sort_type, result);
    }

    void gen_search_cap(const S2LatLng &latlng, double radius_m, S2Cap &cap)
    {
        _geo_client->gen_search_cap(latlng, radius_m, cap);
    }

    std::string gen_value(double lat_degrees, double lng_degrees)
    {
        return "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|" + std::to_string(lng_degrees) +
               "|" + std::to_string(lat_degrees) + "|24.043028|4.15921|0|-1";
    }

public:
    std::shared_ptr<pegasus::geo::geo_client> _geo_client;
};

inline bool operator==(const SearchResult &l, const SearchResult &r)
{
    return l.lat_degrees == r.lat_degrees && l.lng_degrees == r.lng_degrees &&
           l.distance == r.distance && l.hash_key == r.hash_key && l.sort_key == r.sort_key &&
           l.value == r.value && l.cellid == r.cellid;
}

TEST_F(geo_client_test, set_and_del)
{
    double lat_degrees = 12.345;
    double lng_degrees = 67.890;
    std::string test_hash_key = "test_hash_key";
    std::string test_sort_key = "test_sort_key";
    std::string test_value = gen_value(lat_degrees, lng_degrees);

    // geo set
    int ret = _geo_client->set(test_hash_key, test_sort_key, test_value);
    ASSERT_EQ(ret, pegasus::PERR_OK);

    // get from common db
    std::string value;
    ret = common_data_client()->get(test_hash_key, test_sort_key, value);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_EQ(value, test_value);

    // search the inserted data
    {
        std::list<geo::SearchResult> result;
        ret = _geo_client->search_radial(
            test_hash_key, test_sort_key, 1, 1, geo::geo_client::SortType::random, 500, result);
        ASSERT_EQ(ret, pegasus::PERR_OK);
        ASSERT_EQ(result.size(), 1);
        ASSERT_NEAR(result.front().distance, 0.0, 1e-6);
        ASSERT_EQ(result.front().hash_key, test_hash_key);
        ASSERT_EQ(result.front().sort_key, test_sort_key);
        ASSERT_EQ(result.front().value, test_value);
    }

    {
        std::list<geo::SearchResult> result;
        ret = _geo_client->search_radial(
            lat_degrees, lng_degrees, 1, 1, geo::geo_client::SortType::random, 500, result);
        ASSERT_EQ(ret, pegasus::PERR_OK);
        ASSERT_EQ(result.size(), 1);
        ASSERT_NEAR(result.front().distance, 0.0, 1e-6);
        ASSERT_EQ(result.front().hash_key, test_hash_key);
        ASSERT_EQ(result.front().sort_key, test_sort_key);
        ASSERT_EQ(result.front().value, test_value);
    }

    // del
    ret = _geo_client->del(test_hash_key, test_sort_key);
    ASSERT_EQ(ret, pegasus::PERR_OK);

    // get from common db
    ret = common_data_client()->get(test_hash_key, test_sort_key, value);
    ASSERT_EQ(ret, pegasus::PERR_NOT_FOUND);

    // search the inserted data
    {
        std::list<geo::SearchResult> result;
        ret = _geo_client->search_radial(
            test_hash_key, test_sort_key, 1, 1, geo::geo_client::SortType::random, 500, result);
        ASSERT_EQ(ret, pegasus::PERR_NOT_FOUND);
        ASSERT_TRUE(result.empty());
    }

    {
        std::list<geo::SearchResult> result;
        ret = _geo_client->search_radial(
            lat_degrees, lng_degrees, 1, 1, geo::geo_client::SortType::random, 500, result);
        ASSERT_EQ(ret, pegasus::PERR_OK);
        ASSERT_TRUE(result.empty());
    }
}

TEST_F(geo_client_test, set_geo_data)
{
    double lat_degrees = 56.789;
    double lng_degrees = 12.345;
    std::string test_hash_key = "test_hash_key_set_geo_data";
    std::string test_sort_key = "test_sort_key_set_geo_data";
    std::string test_value = gen_value(lat_degrees, lng_degrees);

    // geo set_geo_data
    int ret = _geo_client->set_geo_data(test_hash_key, test_sort_key, test_value);
    ASSERT_EQ(ret, pegasus::PERR_OK);

    // get from common db
    std::string value;
    ret = common_data_client()->get(test_hash_key, test_sort_key, value);
    ASSERT_EQ(ret, pegasus::PERR_NOT_FOUND);

    // search the inserted data
    std::list<geo::SearchResult> result;
    ret = _geo_client->search_radial(
        test_hash_key, test_sort_key, 1, 1, geo::geo_client::SortType::random, 500, result);
    ASSERT_EQ(ret, pegasus::PERR_NOT_FOUND);

    ret = _geo_client->search_radial(
        lat_degrees, lng_degrees, 1, 1, geo::geo_client::SortType::random, 500, result);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_EQ(result.size(), 1);
    ASSERT_NEAR(result.front().distance, 0.0, 1e-6);
    ASSERT_EQ(result.front().hash_key, test_hash_key);
    ASSERT_EQ(result.front().sort_key, test_sort_key);
    ASSERT_EQ(result.front().value, test_value);
}

TEST_F(geo_client_test, same_point_diff_hash_key)
{
    double lat_degrees = 22.345;
    double lng_degrees = 67.890;
    std::string test_hash_key = "test_hash_key";
    std::string test_sort_key = "test_sort_key";
    std::string test_value1 = gen_value(lat_degrees, lng_degrees);
    std::string test_value2 = gen_value(lat_degrees, lng_degrees);

    // geo set
    int ret = _geo_client->set(test_hash_key + "1", test_sort_key, test_value1);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ret = _geo_client->set(test_hash_key + "2", test_sort_key, test_value2);
    ASSERT_EQ(ret, pegasus::PERR_OK);

    // get from common db
    std::string value;
    ret = common_data_client()->get(test_hash_key + "1", test_sort_key, value);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_EQ(value, test_value1);
    ret = common_data_client()->get(test_hash_key + "2", test_sort_key, value);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_EQ(value, test_value2);

    // search the inserted data
    {
        std::list<geo::SearchResult> result;
        ret = _geo_client->search_radial(test_hash_key + "1",
                                         test_sort_key,
                                         1,
                                         2,
                                         geo::geo_client::SortType::random,
                                         500,
                                         result);
        ASSERT_EQ(ret, pegasus::PERR_OK);
        ASSERT_EQ(result.size(), 2);
        for (auto &r : result) {
            ASSERT_NEAR(r.distance, 0.0, 1e-6);
            ASSERT_TRUE(r.hash_key == test_hash_key + "1" || r.hash_key == test_hash_key + "2")
                << r.hash_key;
            ASSERT_EQ(r.sort_key, test_sort_key);
            ASSERT_TRUE(r.value == test_value1 || r.value == test_value2) << r.value;
        }
    }

    {
        std::list<geo::SearchResult> result;
        ret = _geo_client->search_radial(
            lat_degrees, lng_degrees, 1, 2, geo::geo_client::SortType::random, 500, result);
        ASSERT_EQ(ret, pegasus::PERR_OK);
        ASSERT_EQ(result.size(), 2);
        for (auto &r : result) {
            ASSERT_NEAR(r.distance, 0.0, 1e-6);
            ASSERT_TRUE(r.hash_key == test_hash_key + "1" || r.hash_key == test_hash_key + "2")
                << r.hash_key;
            ASSERT_EQ(r.sort_key, test_sort_key);
            ASSERT_TRUE(r.value == test_value1 || r.value == test_value2) << r.value;
        }
    }

    // del
    ret = _geo_client->del(test_hash_key + "1", test_sort_key);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ret = _geo_client->del(test_hash_key + "2", test_sort_key);
    ASSERT_EQ(ret, pegasus::PERR_OK);
}

TEST_F(geo_client_test, same_point_diff_sort_key)
{
    double lat_degrees = 32.345;
    double lng_degrees = 67.890;
    std::string test_hash_key = "test_hash_key";
    std::string test_sort_key = "test_sort_key";
    std::string test_value1 = gen_value(lat_degrees, lng_degrees);
    std::string test_value2 = gen_value(lat_degrees, lng_degrees);

    // geo set
    int ret = _geo_client->set(test_hash_key, test_sort_key + "1", test_value1);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ret = _geo_client->set(test_hash_key, test_sort_key + "2", test_value2);
    ASSERT_EQ(ret, pegasus::PERR_OK);

    // get from common db
    std::string value;
    ret = common_data_client()->get(test_hash_key, test_sort_key + "1", value);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_EQ(value, test_value1);
    ret = common_data_client()->get(test_hash_key, test_sort_key + "2", value);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_EQ(value, test_value2);

    // search the inserted data
    {
        std::list<geo::SearchResult> result;
        ret = _geo_client->search_radial(test_hash_key,
                                         test_sort_key + "1",
                                         1,
                                         2,
                                         geo::geo_client::SortType::random,
                                         500,
                                         result);
        ASSERT_EQ(ret, pegasus::PERR_OK);
        ASSERT_EQ(result.size(), 2);
        for (auto &r : result) {
            ASSERT_NEAR(r.distance, 0.0, 1e-6);
            ASSERT_EQ(r.hash_key, test_hash_key);
            ASSERT_TRUE(r.sort_key == test_sort_key + "1" || r.sort_key == test_sort_key + "2")
                << r.sort_key;
            ASSERT_TRUE(r.value == test_value1 || r.value == test_value2) << r.value;
        }
    }

    {
        std::list<geo::SearchResult> result;
        ret = _geo_client->search_radial(
            lat_degrees, lng_degrees, 1, 2, geo::geo_client::SortType::random, 500, result);
        ASSERT_EQ(ret, pegasus::PERR_OK);
        ASSERT_EQ(result.size(), 2);
        for (auto &r : result) {
            ASSERT_NEAR(r.distance, 0.0, 1e-6);
            ASSERT_EQ(r.hash_key, test_hash_key);
            ASSERT_TRUE(r.sort_key == test_sort_key + "1" || r.sort_key == test_sort_key + "2")
                << r.sort_key;
            ASSERT_TRUE(r.value == test_value1 || r.value == test_value2) << r.value;
        }
    }

    // del
    ret = _geo_client->del(test_hash_key, test_sort_key + "1");
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ret = _geo_client->del(test_hash_key, test_sort_key + "2");
    ASSERT_EQ(ret, pegasus::PERR_OK);
}

TEST_F(geo_client_test, normalize_result_random_order)
{
    std::list<std::vector<geo::SearchResult>> results;
    geo::SearchResult r1(1.1, 1.1, 1, "test_hash_key_1", "test_sort_key_1", "value_1");
    results.push_back({r1});
    int count = 100;
    std::list<geo::SearchResult> result;
    normalize_result(results, count, geo::geo_client::SortType::random, result);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.front(), r1);

    geo::SearchResult r2(2.2, 2.2, 2, "test_hash_key_2", "test_sort_key_2", "value_2");
    results.push_back({r2});
    normalize_result(results, 1, geo::geo_client::SortType::random, result);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.front(), r1);

    normalize_result(results, count, geo::geo_client::SortType::random, result);
    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result.front(), r1);
    ASSERT_EQ(result.back(), r2);

    normalize_result(results, -1, geo::geo_client::SortType::random, result);
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
    normalize_result(results, count, geo::geo_client::SortType::asc, result);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.front(), r2);

    geo::SearchResult r1(1.1, 1.1, 1, "test_hash_key_1", "test_sort_key_1", "value_1");
    results.push_back({r1});
    normalize_result(results, 1, geo::geo_client::SortType::asc, result);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.front(), r1);

    normalize_result(results, count, geo::geo_client::SortType::asc, result);
    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result.front(), r1);
    ASSERT_EQ(result.back(), r2);

    normalize_result(results, -1, geo::geo_client::SortType::asc, result);
    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result.front(), r1);
    ASSERT_EQ(result.back(), r2);
}

TEST_F(geo_client_test, distance)
{
    {
        double lat_degrees = 80;
        double lng_degrees = 27;
        std::string test_hash_key = "test_hash_key1";
        std::string test_sort_key = "test_sort_key1";
        std::string test_value = gen_value(lat_degrees, lng_degrees);
        int ret = _geo_client->set(test_hash_key, test_sort_key, test_value);
        ASSERT_EQ(ret, pegasus::PERR_OK);
    }

    {
        double lat_degrees = 55;
        double lng_degrees = -153;
        std::string test_hash_key = "test_hash_key2";
        std::string test_sort_key = "test_sort_key2";
        std::string test_value = gen_value(lat_degrees, lng_degrees);
        int ret = _geo_client->set(test_hash_key, test_sort_key, test_value);
        ASSERT_EQ(ret, pegasus::PERR_OK);
    }

    double distance = 0.0;
    int ret = _geo_client->distance(
        "test_hash_key1", "test_sort_key1", "test_hash_key2", "test_sort_key2", 2000, distance);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_NEAR(distance, 1000 * S2Earth::RadiusKm() * M_PI / 4, 1e-6);

    ret = _geo_client->distance(
        "test_hash_key1", "test_sort_key1", "test_hash_key1", "test_sort_key1", 2000, distance);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_NEAR(distance, 0.0, 1e-6);
}

TEST_F(geo_client_test, large_cap)
{
    double lat_degrees = 40.039752;
    double lng_degrees = 116.332557;
    double radius_m = 10000;
    int test_data_count = 10000;

    S2Cap cap;
    gen_search_cap(S2LatLng::FromDegrees(lat_degrees, lng_degrees), radius_m, cap);
    for (int i = 0; i < test_data_count; ++i) {
        S2LatLng latlng(S2Testing::SamplePoint(cap));
        ASSERT_TRUE(cap.Contains(latlng.ToPoint()));
        std::string id = std::to_string(i);
        std::string value = id + "|2018-06-05 12:00:00|2018-06-05 13:00:00|abcdefg|" +
                            std::to_string(latlng.lng().degrees()) + "|" +
                            std::to_string(latlng.lat().degrees()) + "|123.456|456.789|0|-1";

        int ret = _geo_client->set(id, "", value, 1000);
        ASSERT_EQ(ret, pegasus::PERR_OK);
    }

    {
        // search the inserted data
        std::list<geo::SearchResult> result;
        int ret = _geo_client->search_radial(
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

            double distance = 0.0;
            ret = _geo_client->distance("0", "", r.hash_key, r.sort_key, 2000, distance);
            ASSERT_EQ(ret, pegasus::PERR_OK);
            if (abs(distance - r.distance) > 1e-6) {
                int a = 0;
            }
            ASSERT_NEAR(distance, r.distance, 1e-6);

            last = r;
        }
    }

    {
        // search the inserted data
        std::list<geo::SearchResult> result;
        int ret = _geo_client->search_radial(
            lat_degrees, lng_degrees, radius_m, -1, geo::geo_client::SortType::asc, 5000, result);
        ASSERT_EQ(ret, pegasus::PERR_OK);
        ASSERT_GE(result.size(), test_data_count);

        std::string test_hash_key = "test_hash_key_large_cap";
        std::string test_sort_key = "test_sort_key_large_cap";
        std::string test_value = gen_value(lat_degrees, lng_degrees);
        ret = _geo_client->set(test_hash_key, test_sort_key, test_value);
        ASSERT_EQ(ret, pegasus::PERR_OK);

        geo::SearchResult last;
        for (const auto &r : result) {
            ASSERT_LE(last.distance, r.distance);
            uint64_t val;
            ASSERT_TRUE(dsn::buf2uint64(r.hash_key.c_str(), val));
            ASSERT_LE(0, val);
            ASSERT_LE(val, test_data_count);
            ASSERT_NE(last.hash_key, r.hash_key);
            ASSERT_EQ(r.sort_key, "");

            double distance = 0.0;
            ret = _geo_client->distance(
                test_hash_key, test_sort_key, r.hash_key, r.sort_key, 2000, distance);
            ASSERT_EQ(ret, pegasus::PERR_OK);
            ASSERT_NEAR(distance, r.distance, 1e-6);

            last = r;
        }

        // del
        ret = _geo_client->del(test_hash_key, test_sort_key);
        ASSERT_EQ(ret, pegasus::PERR_OK);
    }
}
} // namespace geo
} // namespace pegasus
