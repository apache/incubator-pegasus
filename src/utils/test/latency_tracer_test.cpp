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

#include "utils/latency_tracer.h"

#include <gtest/gtest.h>
#include <dsn/dist/fmt_logging.h>

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
    std::shared_ptr<latency_tracer> _sub_tracer;

public:
    void SetUp() override
    {
        FLAGS_enable_latency_tracer = true;
        init_trace_points();
    }

    void init_trace_points()
    {
        _tracer1 = std::make_shared<latency_tracer>("name1");
        for (int i = 0; i < _tracer1_stage_count; i++) {
            _tracer1->add_point(fmt::format("stage{}", i));
        }

        _tracer2 = std::make_shared<latency_tracer>("name2");

        for (int i = 0; i < _tracer2_stage_count; i++) {
            _tracer2->add_point(fmt::format("stage{}", i));
        }

        _sub_tracer = std::make_shared<latency_tracer>("sub");

        _tracer1->set_sub_tracer(_sub_tracer);
        _tracer2->set_sub_tracer(_sub_tracer);

        for (int i = 0; i < _sub_tracer_stage_count; i++) {
            _sub_tracer->add_point(fmt::format("stage{}", i));
        }
    }

    std::map<int64_t, std::string> get_points(std::shared_ptr<latency_tracer> tracer)
    {
        return tracer->_points;
    }

    std::shared_ptr<latency_tracer> get_sub_tracer(std::shared_ptr<latency_tracer> tracer)
    {
        return tracer->_sub_tracer;
    }
};

TEST_F(latency_tracer_test, add_point)
{
    auto tracer1_points = get_points(_tracer1);
    ASSERT_EQ(tracer1_points.size(), _tracer1_stage_count);
    int count1 = 0;
    for (auto point : tracer1_points) {
        ASSERT_EQ(point.second, fmt::format("stage{}", count1++));
    }

    auto tracer2_points = get_points(_tracer2);
    ASSERT_EQ(tracer2_points.size(), _tracer2_stage_count);
    int count2 = 0;
    for (auto point : tracer2_points) {
        ASSERT_EQ(point.second, fmt::format("stage{}", count2++));
    }

    auto tracer1_sub_tracer = get_sub_tracer(_tracer1);
    auto tracer2_sub_tracer = get_sub_tracer(_tracer2);
    ASSERT_EQ(tracer1_sub_tracer, tracer2_sub_tracer);

    auto points = get_points(tracer1_sub_tracer);
    ASSERT_TRUE(get_sub_tracer(tracer1_sub_tracer) == nullptr);
    ASSERT_EQ(points.size(), _sub_tracer_stage_count);
    int count3 = 0;
    for (auto point : points) {
        ASSERT_EQ(point.second, fmt::format("stage{}", count3++));
    }

    _tracer1->dump_trace_points(0);
    _tracer2->dump_trace_points(0);
}
} // namespace utils
} // namespace dsn
