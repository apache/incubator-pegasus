// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <fmt/core.h>
#include <gmock/gmock.h> // IWYU pragma: keep
#include <cstdint>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "common/replication.codes.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "utils/latency_tracer.h"

namespace dsn {
namespace utils {
class latency_tracer_test : public testing::Test
{
public:
    int _tracer1_stage_count = 3;
    int _tracer2_stage_count = 2;
    int _sub_tracer_stage_count = 2;

    std::shared_ptr<latency_tracer> _tracer1;
    std::shared_ptr<latency_tracer> _tracer2;
    std::shared_ptr<latency_tracer> _tracer3;
    std::shared_ptr<latency_tracer> _tracer4;
    std::shared_ptr<latency_tracer> _sub_tracer;

public:
    void SetUp() override
    {
        FLAGS_enable_latency_tracer = true;
        init_trace_points();
    }

    void init_trace_points()
    {
        _tracer1 = std::make_shared<latency_tracer>(false, "name1", 0);
        for (int i = 0; i < _tracer1_stage_count; i++) {
            ADD_CUSTOM_POINT(_tracer1, fmt::format("stage{}", i));
        }

        _tracer2 = std::make_shared<latency_tracer>(false, "name2", 0);

        for (int i = 0; i < _tracer2_stage_count; i++) {
            ADD_CUSTOM_POINT(_tracer2, fmt::format("stage{}", i));
        }

        _sub_tracer = std::make_shared<latency_tracer>(true, "sub", 0);
        _sub_tracer->set_parent_point_name("test");

        _tracer1->add_sub_tracer(_sub_tracer);
        _tracer2->add_sub_tracer(_sub_tracer);

        for (int i = 0; i < _sub_tracer_stage_count; i++) {
            ADD_CUSTOM_POINT(_sub_tracer, fmt::format("stage{}", i));
        }

        _tracer3 = std::make_shared<latency_tracer>(false, "name3", 0);
        APPEND_EXTERN_POINT(_tracer3, 123, "test");

        _tracer4 = std::make_shared<latency_tracer>(false, "name4", 0, RPC_TEST);
    }

    std::map<int64_t, std::string> get_points(const std::shared_ptr<latency_tracer> &tracer)
    {
        return tracer->_points;
    }

    std::shared_ptr<latency_tracer> get_sub_tracer(const std::shared_ptr<latency_tracer> &tracer)
    {
        return tracer->sub_tracer("sub");
    }
};

TEST_F(latency_tracer_test, add_point)
{
    auto tracer1_points = get_points(_tracer1);
    // tracer constructor will auto push one point, so the total count is stage_count + 1
    ASSERT_EQ(tracer1_points.size(), _tracer1_stage_count + 1);
    int count1 = 0;
    bool tracer1_first = true;
    for (const auto &point : tracer1_points) {
        if (tracer1_first) {
            tracer1_first = false;
            continue;
        }
        ASSERT_THAT(point.second,
                    testing::ContainsRegex(fmt::format(
                        "latency_tracer_test.cpp:[0-9]+:init_trace_points_stage{}", count1++)));
    }

    auto tracer2_points = get_points(_tracer2);
    ASSERT_EQ(tracer2_points.size(), _tracer2_stage_count + 1);
    int count2 = 0;
    bool tracer2_first = true;
    for (const auto &point : tracer2_points) {
        if (tracer2_first) {
            tracer2_first = false;
            continue;
        }
        ASSERT_THAT(point.second,
                    testing::ContainsRegex(fmt::format(
                        "latency_tracer_test.cpp:[0-9]+:init_trace_points_stage{}", count2++)));
    }

    auto tracer1_sub_tracer = get_sub_tracer(_tracer1);
    auto tracer2_sub_tracer = get_sub_tracer(_tracer2);
    ASSERT_EQ(tracer1_sub_tracer, tracer2_sub_tracer);

    auto points = get_points(tracer1_sub_tracer);
    ASSERT_TRUE(get_sub_tracer(tracer1_sub_tracer) == nullptr);
    ASSERT_EQ(points.size(), _sub_tracer_stage_count + 1);
    int count3 = 0;
    bool sub_tracer_first = true;
    for (const auto &point : points) {
        if (sub_tracer_first) {
            sub_tracer_first = false;
            continue;
        }
        ASSERT_THAT(point.second,
                    testing::ContainsRegex(fmt::format(
                        "latency_tracer_test.cpp:[0-9]+:init_trace_points_stage{}", count3++)));
    }

    // tracer3 append one invalid point, it will reset the last position and update the
    // timestamp=previous+1
    auto tracer3_points = get_points(_tracer3);
    ASSERT_EQ(tracer3_points.size(), 2);
    ASSERT_EQ(tracer3_points.rbegin()->first - tracer3_points.begin()->first, 1);

    // tracer4 init with disable trace task code, the points size will be 0
    auto tracer4_points = get_points(_tracer4);
    ASSERT_EQ(tracer4_points.size(), 0);
}
} // namespace utils
} // namespace dsn
