// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "geo/lib/geo_client.h"

#include <iostream>

#include <s2/s2testing.h>
#include <s2/s2cell.h>
#include <monitoring/histogram.h>
#include <rocksdb/env.h>

#include <dsn/utility/strings.h>
#include <dsn/utility/string_conv.h>

static const int data_count = 10000;

int main(int argc, char **argv)
{
    if (argc != 7) {
        std::cerr << "USAGE: " << argv[0]
                  << " <cluster_name> <app_name> <geo_app_name> <radius> <test_count> <max_level>"
                  << std::endl;
        return -1;
    }

    std::string cluster_name = argv[1];
    std::string app_name = argv[2];
    std::string geo_app_name = argv[3];
    double radius = 0.0;
    if (!dsn::buf2double(argv[4], radius)) {
        std::cerr << "radius is invalid: " << argv[4] << std::endl;
        return -1;
    }
    int test_count = 2000;
    if (!dsn::buf2int32(argv[5], test_count)) {
        std::cerr << "test_count is invalid: " << argv[5] << std::endl;
        return -1;
    }
    int max_level = 16;
    if (!dsn::buf2int32(argv[6], max_level)) {
        std::cerr << "max_level is invalid: " << argv[6] << std::endl;
        return -1;
    }
    pegasus::geo::geo_client my_geo("config.ini",
                                    cluster_name.c_str(),
                                    app_name.c_str(),
                                    geo_app_name.c_str(),
                                    new pegasus::geo::latlng_extractor_for_lbs());
    my_geo.set_max_level(max_level);

    // cover beijing 5th ring road
    S2LatLngRect rect(S2LatLng::FromDegrees(39.810151, 116.194511),
                      S2LatLng::FromDegrees(40.028697, 116.535087));

    // generate data for test
    //    for (int i = 0; i < data_count; ++i) {
    //        S2LatLng latlng(S2Testing::SamplePoint(rect));
    //        std::string id = std::to_string(i);
    //        std::string value = id + "|2018-06-05 12:00:00|2018-06-05 13:00:00|abcdefg|" +
    //                            std::to_string(latlng.lng().degrees()) + "|" +
    //                            std::to_string(latlng.lat().degrees()) + "|123.456|456.789|0|-1";
    //
    //        int ret = my_geo.set(id, "", value, 1000);
    //        if (ret != pegasus::PERR_OK) {
    //            std::cerr << "set data failed. error=" << ret << std::endl;
    //        }
    //    }

    rocksdb::HistogramImpl latency_histogram;
    rocksdb::HistogramImpl result_count_histogram;
    rocksdb::Env *env = rocksdb::Env::Default();
    uint64_t start = env->NowNanos();
    std::atomic<uint64_t> count(test_count);
    dsn::utils::notify_event get_completed;
    // test search_radial by lat & lng
    for (int i = 0; i < test_count; ++i) {
        S2LatLng latlng(S2Testing::SamplePoint(rect));

        uint64_t start_nanos = env->NowNanos();
        my_geo.async_search_radial(
            latlng.lat().degrees(),
            latlng.lng().degrees(),
            radius,
            -1,
            pegasus::geo::geo_client::SortType::random,
            500,
            [&, start_nanos](int error_code, std::list<pegasus::geo::SearchResult> &&results) {
                latency_histogram.Add(env->NowNanos() - start_nanos);
                result_count_histogram.Add(results.size());
                uint64_t left = count.fetch_sub(1);
                if (left == 1) {
                    get_completed.notify();
                }
            });
    }
    std::cout << "get_completed.wait" << std::endl;
    get_completed.wait();
    uint64_t end = env->NowNanos();

    std::cout << "start time: " << start << ", end time: " << end
              << ", QPS: " << test_count / ((end - start) / 1e9) << std::endl;
    std::cout << "latency_histogram: " << std::endl;
    std::cout << latency_histogram.ToString() << std::endl;
    std::cout << "result_count_histogram: " << std::endl;
    std::cout << result_count_histogram.ToString() << std::endl;

    return 0;
}
