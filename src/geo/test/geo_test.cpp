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

#include "geo/lib/geo_client.h"
#include <gtest/gtest.h>
#include <s2/s2cap.h>
#include <s2/s2testing.h>
#include <s2/s2earth.h>
#include <s2/s2cell.h>
#include "utils/strings.h"
#include "utils/string_conv.h"
#include <base/pegasus_key_schema.h>
#include "utils/fmt_logging.h"
#include "common/replication_other_types.h"
#include "client/replication_ddl_client.h"
#include "base/pegasus_const.h"

namespace pegasus {
namespace geo {

class geo_client_test : public ::testing::Test
{
public:
    geo_client_test()
    {
        std::vector<dsn::rpc_address> meta_list;
        bool ok = dsn::replication::replica_helper::load_meta_servers(
            meta_list, PEGASUS_CLUSTER_SECTION_NAME.c_str(), "onebox");
        CHECK(ok, "load_meta_servers failed");
        auto ddl_client = new dsn::replication::replication_ddl_client(meta_list);
        dsn::error_code error = ddl_client->create_app("temp_geo", "pegasus", 4, 3, {}, false);
        CHECK_EQ(dsn::ERR_OK, error);
        _geo_client.reset(new pegasus::geo::geo_client("config.ini", "onebox", "temp", "temp_geo"));
    }

    pegasus_client *common_data_client() { return _geo_client->_common_data_client; }
    pegasus::geo::geo_client *geo_client() { return _geo_client.get(); }

    int min_level() { return _geo_client->_min_level; }

    bool generate_geo_keys(const std::string &hash_key,
                           const std::string &sort_key,
                           const std::string &value,
                           std::string &geo_hash_key,
                           std::string &geo_sort_key)
    {
        return _geo_client->generate_geo_keys(
            hash_key, sort_key, value, geo_hash_key, geo_sort_key);
    }

    bool restore_origin_keys(const std::string &geo_sort_key,
                             std::string &origin_hash_key,
                             std::string &origin_sort_key)
    {
        return _geo_client->restore_origin_keys(geo_sort_key, origin_hash_key, origin_sort_key);
    }

    void normalize_result(std::list<std::list<SearchResult>> &&results,
                          int count,
                          geo::geo_client::SortType sort_type,
                          std::list<SearchResult> &result)
    {
        _geo_client->normalize_result(std::move(results), count, sort_type, result);
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
    double expect_lat_degrees = 12.345;
    double expect_lng_degrees = 67.890;
    std::string test_hash_key = "test_hash_key";
    std::string test_sort_key = "test_sort_key";
    std::string test_value = gen_value(expect_lat_degrees, expect_lng_degrees);

    // geo set
    int ret = _geo_client->set(test_hash_key, test_sort_key, test_value);
    ASSERT_EQ(ret, pegasus::PERR_OK);

    // get from common db
    std::string value;
    ret = common_data_client()->get(test_hash_key, test_sort_key, value);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_EQ(value, test_value);

    double got_lat_degrees;
    double got_lng_degrees;
    ret = geo_client()->get(test_hash_key, test_sort_key, got_lat_degrees, got_lng_degrees);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_DOUBLE_EQ(expect_lat_degrees, got_lat_degrees);
    ASSERT_DOUBLE_EQ(expect_lng_degrees, got_lng_degrees);

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
        ret = _geo_client->search_radial(expect_lat_degrees,
                                         expect_lng_degrees,
                                         1,
                                         1,
                                         geo::geo_client::SortType::random,
                                         500,
                                         result);
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

    ret = geo_client()->get(test_hash_key, test_sort_key, got_lat_degrees, got_lng_degrees);
    ASSERT_NE(ret, pegasus::PERR_OK);

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
        ret = _geo_client->search_radial(expect_lat_degrees,
                                         expect_lng_degrees,
                                         1,
                                         1,
                                         geo::geo_client::SortType::random,
                                         500,
                                         result);
        ASSERT_EQ(ret, pegasus::PERR_OK);
        ASSERT_TRUE(result.empty());
    }
}

TEST_F(geo_client_test, set_and_del_on_undecoded_data)
{
    double lat_degrees = 23.456;
    double lng_degrees = 78.901;
    std::string test_hash_key = "test_hash_key";
    std::string test_sort_key = "test_sort_key";
    std::string test_undecoded_value = "undecoded_value";
    std::string test_value = gen_value(lat_degrees, lng_degrees);

    // set undecoded value into common data db
    int ret = common_data_client()->set(test_hash_key, test_sort_key, test_undecoded_value);
    ASSERT_EQ(ret, pegasus::PERR_OK);

    // geo set
    ret = _geo_client->set(test_hash_key, test_sort_key, test_value);
    ASSERT_EQ(ret, pegasus::PERR_OK);

    // geo del
    ret = _geo_client->del(test_hash_key, test_sort_key);
    ASSERT_EQ(ret, pegasus::PERR_OK);

    // set undecoded value into common data db
    ret = common_data_client()->set(test_hash_key, test_sort_key, test_undecoded_value);
    ASSERT_EQ(ret, pegasus::PERR_OK);

    // geo del
    ret = _geo_client->del(test_hash_key, test_sort_key);
    ASSERT_EQ(ret, pegasus::PERR_OK);
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
            ASSERT_DOUBLE_EQ(r.distance, 0.0);
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
            ASSERT_DOUBLE_EQ(r.distance, 0.0);
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
            ASSERT_DOUBLE_EQ(r.distance, 0.0);
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
            ASSERT_DOUBLE_EQ(r.distance, 0.0);
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

TEST_F(geo_client_test, generate_and_restore_geo_keys)
{
    std::string geo_hash_key;
    std::string geo_sort_key;
    ASSERT_FALSE(generate_geo_keys("", "", "", geo_hash_key, geo_sort_key));

    double lat_degrees = 32.345;
    double lng_degrees = 67.890;
    std::string test_hash_key = "test_hash_key";
    std::string test_sort_key = "test_sort_key";
    std::string test_value = gen_value(lat_degrees, lng_degrees);

    std::string leaf_cell_id =
        S2Cell(S2LatLng::FromDegrees(lat_degrees, lng_degrees)).id().ToString();
    ASSERT_EQ(leaf_cell_id.length(), 32); // 1 width face, 1 width '/' and 30 width level

    ASSERT_TRUE(
        generate_geo_keys(test_hash_key, test_sort_key, test_value, geo_hash_key, geo_sort_key));
    ASSERT_EQ(min_level() + 2, geo_hash_key.length());
    ASSERT_EQ(leaf_cell_id.substr(0, geo_hash_key.length()), geo_hash_key);
    ASSERT_EQ(leaf_cell_id.substr(geo_hash_key.length()),
              geo_sort_key.substr(0, leaf_cell_id.length() - geo_hash_key.length()));

    dsn::blob hash_key_bb, sort_key_bb;
    int skip_length = (int)(32 - geo_hash_key.length()) + 1; // postfix of cell id and ':'
    pegasus_restore_key(dsn::blob(geo_sort_key.data(),
                                  skip_length,
                                  (unsigned int)(geo_sort_key.length() - skip_length)),
                        hash_key_bb,
                        sort_key_bb);
    ASSERT_EQ(hash_key_bb.to_string(), test_hash_key);
    ASSERT_EQ(sort_key_bb.to_string(), test_sort_key);

    std::string restore_hash_key;
    std::string restore_sort_key;
    ASSERT_TRUE(restore_origin_keys(geo_sort_key, restore_hash_key, restore_sort_key));
    ASSERT_EQ(test_hash_key, restore_hash_key);
    ASSERT_EQ(test_sort_key, restore_sort_key);
}

TEST_F(geo_client_test, normalize_result_random_order)
{
    geo::SearchResult r1(1.1, 1.1, 1, "test_hash_key_1", "test_sort_key_1", "value_1");
    geo::SearchResult r2(2.2, 2.2, 2, "test_hash_key_2", "test_sort_key_2", "value_2");
    int count = 100;
    std::list<geo::SearchResult> result;

    {
        std::list<std::list<geo::SearchResult>> results;
        results.push_back({geo::SearchResult(r1)});
        normalize_result(std::move(results), count, geo::geo_client::SortType::random, result);
        ASSERT_EQ(result.size(), 1);
        ASSERT_EQ(result.front(), r1);
    }

    {
        std::list<std::list<geo::SearchResult>> results;
        results.push_back({geo::SearchResult(r1)});
        results.push_back({geo::SearchResult(r2)});
        normalize_result(std::move(results), 1, geo::geo_client::SortType::random, result);
        ASSERT_EQ(result.size(), 1);
        ASSERT_EQ(result.front(), r1);
    }

    {
        std::list<std::list<geo::SearchResult>> results;
        results.push_back({geo::SearchResult(r1)});
        results.push_back({geo::SearchResult(r2)});
        normalize_result(std::move(results), count, geo::geo_client::SortType::random, result);
        ASSERT_EQ(result.size(), 2);
        ASSERT_EQ(result.front(), r1);
        ASSERT_EQ(result.back(), r2);
    }

    {
        std::list<std::list<geo::SearchResult>> results;
        results.push_back({geo::SearchResult(r1)});
        results.push_back({geo::SearchResult(r2)});
        normalize_result(std::move(results), -1, geo::geo_client::SortType::random, result);
        ASSERT_EQ(result.size(), 2);
        ASSERT_EQ(result.front(), r1);
        ASSERT_EQ(result.back(), r2);
    }
}

TEST_F(geo_client_test, normalize_result_distance_order)
{
    geo::SearchResult r1(1.1, 1.1, 1, "test_hash_key_1", "test_sort_key_1", "value_1");
    geo::SearchResult r2(2.2, 2.2, 2, "test_hash_key_2", "test_sort_key_2", "value_2");
    int count = 100;
    std::list<geo::SearchResult> result;

    {
        std::list<std::list<geo::SearchResult>> results;
        results.push_back({geo::SearchResult(r2)});
        normalize_result(std::move(results), count, geo::geo_client::SortType::asc, result);
        ASSERT_EQ(result.size(), 1);
        ASSERT_EQ(result.front(), r2);
    }

    {
        std::list<std::list<geo::SearchResult>> results;
        results.push_back({geo::SearchResult(r1)});
        normalize_result(std::move(results), 1, geo::geo_client::SortType::asc, result);
        ASSERT_EQ(result.size(), 1);
        ASSERT_EQ(result.front(), r1);
    }

    {
        std::list<std::list<geo::SearchResult>> results;
        results.push_back({geo::SearchResult(r1)});
        results.push_back({geo::SearchResult(r2)});
        normalize_result(std::move(results), count, geo::geo_client::SortType::asc, result);
        ASSERT_EQ(result.size(), 2);
        ASSERT_EQ(result.front(), r1);
        ASSERT_EQ(result.back(), r2);
    }

    {
        std::list<std::list<geo::SearchResult>> results;
        results.push_back({geo::SearchResult(r1)});
        results.push_back({geo::SearchResult(r2)});
        normalize_result(std::move(results), -1, geo::geo_client::SortType::asc, result);
        ASSERT_EQ(result.size(), 2);
        ASSERT_EQ(result.front(), r1);
        ASSERT_EQ(result.back(), r2);
    }
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
    ASSERT_DOUBLE_EQ(distance, 1000 * S2Earth::RadiusKm() * M_PI / 4);

    ret = _geo_client->distance(
        "test_hash_key1", "test_sort_key1", "test_hash_key1", "test_sort_key1", 2000, distance);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_DOUBLE_EQ(distance, 0.0);
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

        int ret = _geo_client->set(id, "", value, 5000);
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
            ASSERT_TRUE(dsn::buf2uint64(r.hash_key.c_str(), val)) << r.hash_key;
            ASSERT_LE(0, val);
            ASSERT_LE(val, test_data_count);
            ASSERT_NE(last.hash_key, r.hash_key);
            ASSERT_EQ(r.sort_key, "");

            double distance = 0.0;
            ret = _geo_client->distance("0", "", r.hash_key, r.sort_key, 2000, distance);
            ASSERT_EQ(ret, pegasus::PERR_OK);
            ASSERT_DOUBLE_EQ(distance, r.distance);

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
            ASSERT_DOUBLE_EQ(distance, r.distance);

            last = r;
        }

        // del
        ret = _geo_client->del(test_hash_key, test_sort_key);
        ASSERT_EQ(ret, pegasus::PERR_OK);
    }
}
} // namespace geo
} // namespace pegasus
