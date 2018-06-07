// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "geo/src/geo_client.h"

#include <iostream>
#include <s2/s2testing.h>
#include <s2/s2cell.h>
#include <dsn/utility/strings.h>

static const int data_count = 10000;
static const int test_count = 1;
static const double radius = 5000.0;

// ./pegasus_geo_test onebox temp temp_geo
int main(int argc, char **argv)
{
    if (argc != 4) {
        std::cerr << "USAGE: " << argv[0] << "<cluster-name> <app-name>" << std::endl;
        return -1;
    }

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

    pegasus::geo_client my_geo("config.ini", argv[1], argv[2], argv[3], extractor);

    // cover beijing 5th ring road
    S2LatLngRect rect(S2LatLng::FromDegrees(39.810151, 116.194511),
                      S2LatLng::FromDegrees(40.028697, 116.535087));
    for (int i = 0; i < data_count; ++i) {
        S2LatLng latlng(S2Testing::SamplePoint(rect));
        std::string id = std::to_string(i);
        std::string value = id + "|2018-06-05 12:00:00|2018-06-05 13:00:00|abcdefg|" +
                            std::to_string(latlng.lat().degrees()) + "|" +
                            std::to_string(latlng.lng().degrees()) + "|123.456|456.789|0|-1";

        int ret = my_geo.set(id, "", value, 1000);
        if (ret != pegasus::PERR_OK) {
            std::cerr << "set data failed. error=" << ret << std::endl;
        }
    }

    for (int i = 0; i < test_count; ++i) {
        S2LatLng latlng(S2Testing::SamplePoint(rect));

        std::list<pegasus::SearchResult> result;
        my_geo.search_radial(latlng.lat().degrees(),
                             latlng.lng().degrees(),
                             radius,
                             -1,
                             pegasus::geo_client::SortType::random,
                             500,
                             result);

        std::cout << "count: " << result.size() << std::endl;
        for (auto &data : result) {
            std::cout << data.to_string() << std::endl;
        }
    }

    for (int i = 0; i < test_count; ++i) {
        std::string id = std::to_string(i);
        std::list<pegasus::SearchResult> result;
        my_geo.search_radial(
            id, "", radius, 20, pegasus::geo_client::SortType::nearest, 500, result);

        std::cout << "count: " << result.size() << std::endl;
        for (auto &data : result) {
            std::cout << data.to_string() << std::endl;
        }
    }

    return 0;
}
