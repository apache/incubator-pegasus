// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "geo/src/geo_client.h"
#include <gtest/gtest.h>
#include <dsn/utility/strings.h>

namespace pegasus {

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
    geo_client_test() {}

    pegasus_client *common_data_client()
    {
        return _geo_client._common_data_client;
    }

    pegasus_client *geo_data_client()
    {
        return _geo_client._geo_data_client;
    }

    void SetUp() override
    {
        _geo_client = pegasus::geo_client("config.ini", "onebox", "temp", "temp_geo", extractor);
    }

    void TearDown() override {}

public:
    pegasus::geo_client _geo_client;
    const std::string test_hash_key = "test_hash_key";
    const std::string test_sort_key = "test_sort_key";
    const std::string test_value = "00:00:00:00:01:5e|2018-04-26|2018-04-28|ezp8xchrr|-0.356396|39."
                                   "469644|24.043028|4.15921|0|-1";
};

TEST_F(geo_client_test, set)
{
    int ret = _geo_client.set(test_hash_key, test_sort_key, test_value);
    ASSERT_EQ(ret, pegasus::PERR_OK);

    std::string value;
    ret = common_data_client()->get(test_hash_key, test_sort_key, value);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_EQ(value, test_value);

    std::list<SearchResult> result;
    ret = _geo_client.search_radial(test_hash_key, test_sort_key, 1, 1, geo_client::SortType::random, 500, result);
    ASSERT_EQ(ret, pegasus::PERR_OK);
    ASSERT_EQ(result.size(), 1);
    ASSERT_LE(result.front().distance, 0.000001);
    ASSERT_EQ(result.front().hash_key, test_hash_key);
    ASSERT_EQ(result.front().sort_key, test_sort_key);
    ASSERT_EQ(result.front().value, test_value);
}
}
