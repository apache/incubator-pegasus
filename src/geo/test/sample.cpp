// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "../src/geo.h"

#include <iostream>
#include <s2/s2testing.h>

static const int data_count = 10000;
static const int test_count = 1;

// ./pegasus_geo_test onebox temp
int main(int argc, char **argv)
{
    if (argc != 3) {
        std::cerr << "USAGE: " << argv[0] << "<cluster-name> <app-name>" << std::endl;
        return -1;
    }

    pegasus::geo my_geo;
    if (pegasus::PERR_OK != my_geo.init("config.ini", argv[1], argv[2])) {
        std::cerr << "init geo failed" << std::endl;
        return -1;
    }

    // cover beijing 5th ring road
    S2LatLngRect rect(S2LatLng::FromDegrees(39.810151, 116.194511),
                      S2LatLng::FromDegrees(40.028697, 116.535087));
    my_geo.TEST_fill_data_in_rect(rect, data_count);

    for (int i = 0; i < test_count; ++i) {
        S2Point pt = S2Testing::SamplePoint(rect);

        std::list<std::pair<std::string, double>> result;
        my_geo.search_radial(S2LatLng::Latitude(pt).degrees(),
                             S2LatLng::Longitude(pt).degrees(),
                             5000,
                             -1,
                             0,
                             result);

        std::cout << "count: " << result.size() << std::endl;
        for (auto &data : result) {
            std::cout << data.first << " => " << data.second << std::endl;
        }
    }

    return 0;
}
