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

#include <pegasus/error.h>
#include <rocksdb/env.h>
#include <rocksdb/statistics.h>
#include <s2/s1angle.h>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>
#include <s2/s2testing.h>
#include <stdint.h>
#include <atomic>
#include <iostream>
#include <list>
#include <memory>
#include <string>

#include "geo/lib/geo_client.h"
#include "geo/lib/latlng_codec.h"
#include "utils/env.h"
#include "utils/errors.h"
#include "utils/fmt_logging.h"
#include "utils/string_conv.h"
#include "utils/synchronize.h"

static const int data_count = 10000;

int main(int argc, char **argv)
{
    if (argc < 7) {
        std::cerr << "USAGE: " << argv[0]
                  << " <cluster_name> <app_name> <geo_app_name> <radius> "
                     "<test_count> <max_level> [gen_data]"
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
    bool gen_data = false;
    if (argc >= 8) {
        if (!dsn::buf2bool(argv[7], gen_data)) {
            std::cerr << "gen_data is invalid: " << argv[7] << std::endl;
            return -1;
        }
    }

    // TODO(yingchun): the benchmark can not exit normally, we need to fix it later.
    pegasus::geo::geo_client my_geo(
        "config.ini", cluster_name.c_str(), app_name.c_str(), geo_app_name.c_str());
    auto err = my_geo.set_max_level(max_level);
    if (!err.is_ok()) {
        std::cerr << "set_max_level failed, err: " << err << std::endl;
        return -1;
    }

    // cover beijing 5th ring road
    S2LatLngRect rect(S2LatLng::FromDegrees(39.810151, 116.194511),
                      S2LatLng::FromDegrees(40.028697, 116.535087));

    // generate data for test
    if (gen_data) {
        const pegasus::geo::latlng_codec &codec = my_geo.get_codec();
        for (int i = 0; i < data_count; ++i) {
            std::string value;
            S2LatLng latlng(S2Testing::SamplePoint(rect));
            bool ok = codec.encode_to_value(latlng.lat().degrees(), latlng.lng().degrees(), value);
            CHECK(ok, "");
            int ret = my_geo.set(std::to_string(i), "", value, 1000);
            if (ret != pegasus::PERR_OK) {
                std::cerr << "set data failed. error=" << ret << std::endl;
            }
        }
    }

    enum class histogram_type : uint32_t
    {
        LATENCY,
        RESULT_COUNT
    };
    auto statistics = rocksdb::CreateDBStatistics();
    rocksdb::Env *env = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive);
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
                statistics->measureTime(static_cast<uint32_t>(histogram_type::LATENCY),
                                        env->NowNanos() - start_nanos);
                statistics->measureTime(static_cast<uint32_t>(histogram_type::RESULT_COUNT),
                                        results.size());
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
    std::cout << statistics->getHistogramString(static_cast<uint32_t>(histogram_type::LATENCY))
              << std::endl;
    std::cout << "result_count_histogram: " << std::endl;
    std::cout << statistics->getHistogramString(static_cast<uint32_t>(histogram_type::RESULT_COUNT))
              << std::endl;

    return 0;
}
