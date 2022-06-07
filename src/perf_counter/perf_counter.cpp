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

#include <cstring>
#include <dsn/perf_counter/perf_counter.h>

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
        if (strcmp(str, ctypes[i]) == 0)
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
        if (strcmp(str, ptypes[i]) == 0)
            return (dsn_perf_counter_percentile_type_t)i;
    }
    return COUNTER_PERCENTILE_INVALID;
}
