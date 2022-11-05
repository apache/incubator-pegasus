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

#include "math.h"

#include <algorithm>
#include <numeric>

#include "utils/api_utilities.h"
#include "utils/fmt_logging.h"
#include "utils/math.h"

namespace dsn {
namespace utils {

double mean_stddev(const std::vector<uint32_t> &result_set, bool partial_sample)
{
    CHECK_GT_MSG(result_set.size(), 1, "invalid sample data input for stddev");

    double sum = std::accumulate(result_set.begin(), result_set.end(), 0.0);
    double mean = sum / result_set.size();

    double accum = 0.0;
    std::for_each(result_set.begin(), result_set.end(), [&](const double d) {
        accum += (d - mean) * (d - mean);
    });

    double stddev;
    if (partial_sample)
        stddev = sqrt(accum / (result_set.size() - 1));
    else
        stddev = sqrt(accum / (result_set.size()));

    stddev = ((double)((int)((stddev + 0.005) * 100))) / 100;
    return stddev;
}

} // namespace utils
} // namespace dsn
