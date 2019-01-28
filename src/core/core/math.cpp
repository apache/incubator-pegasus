// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <numeric>
#include <algorithm>
#include <math.h>
#include <dsn/c/api_utilities.h>
#include <dsn/utility/math.h>

namespace dsn {
namespace utils {

double mean_stddev(const std::vector<uint32_t> &result_set, bool partial_sample)
{
    dassert(result_set.size() > 1, "invalid sample data input for stddev");

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
