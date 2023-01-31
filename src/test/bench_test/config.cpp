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

#include "config.h"

#include "utils/config_api.h"
#include "utils/flags.h"

namespace pegasus {
namespace test {

DSN_DEFINE_int32(pegasus.benchmark,
                 pegasus_timeout_ms,
                 1000,
                 "pegasus read/write timeout in milliseconds");
DSN_DEFINE_int32(pegasus.benchmark, threads, 1, "Number of concurrent threads to run");
DSN_DEFINE_int32(pegasus.benchmark, hashkey_size, 16, "size of each hashkey");
DSN_DEFINE_int32(pegasus.benchmark, sortkey_size, 16, "size of each sortkey");
DSN_DEFINE_int32(pegasus.benchmark, value_size, 100, "Size of each value");

config::config()
{
    pegasus_cluster_name = dsn_config_get_value_string(
        "pegasus.benchmark", "pegasus_cluster_name", "onebox", "pegasus cluster name");
    pegasus_app_name = dsn_config_get_value_string(
        "pegasus.benchmark", "pegasus_app_name", "temp", "pegasus app name");
    benchmarks = dsn_config_get_value_string(
        "pegasus.benchmark",
        "benchmarks",
        "fillrandom_pegasus,readrandom_pegasus,deleterandom_pegasus",
        "Comma-separated list of operations to run in the specified order. Available benchmarks:\n"
        "\tfillrandom_pegasus       -- pegasus write N values in random key order\n"
        "\treadrandom_pegasus       -- pegasus read N times in random order\n"
        "\tdeleterandom_pegasus     -- pegasus delete N keys in random order\n");
    num = dsn_config_get_value_uint64(
        "pegasus.benchmark", "num", 10000, "Number of key/values to place in database");
    seed = dsn_config_get_value_uint64(
        "pegasus.benchmark",
        "seed",
        1000,
        "Seed base for random number generators. When 0 it is deterministic");
    seed = seed ? seed : 1000;
    env = rocksdb::Env::Default();
}
} // namespace test
} // namespace pegasus
