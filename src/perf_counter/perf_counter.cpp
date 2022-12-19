/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "perf_counter/perf_counter.h"

#include "utils/strings.h"

static const char *ctypes[] = {
    "NUMBER", "VOLATILE_NUMBER", "RATE", "PERCENTILE", "INVALID_COUNTER"};
const char *dsn_counter_type_to_string(dsn_perf_counter_type_t t)
{
    if (t >= COUNTER_TYPE_COUNT)
        return ctypes[COUNTER_TYPE_COUNT];
    return ctypes[t];
}

dsn_perf_counter_type_t dsn_counter_type_from_string(const char *str)
{
    for (int i = 0; i < COUNTER_TYPE_COUNT; ++i) {
        if (dsn::utils::equals(str, ctypes[i]))
            return (dsn_perf_counter_type_t)i;
    }
    return COUNTER_TYPE_INVALID;
}

static const char *ptypes[] = {"P50", "P90", "P95", "P99", "P999", "INVALID_PERCENTILE"};
const char *dsn_percentile_type_to_string(dsn_perf_counter_percentile_type_t t)
{
    if (t >= COUNTER_PERCENTILE_COUNT)
        return ptypes[COUNTER_PERCENTILE_COUNT];
    return ptypes[t];
}

dsn_perf_counter_percentile_type_t dsn_percentile_type_from_string(const char *str)
{
    for (int i = 0; i < COUNTER_PERCENTILE_COUNT; ++i) {
        if (dsn::utils::equals(str, ptypes[i]))
            return (dsn_perf_counter_percentile_type_t)i;
    }
    return COUNTER_PERCENTILE_INVALID;
}
