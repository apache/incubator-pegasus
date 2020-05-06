// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/utility/config_api.h>
#include "config.h"

namespace pegasus {
namespace test {

config::config()
{
    pegasus_cluster_name = dsn_config_get_value_string(
        "pegasus.benchmark", "pegasus_cluster_name", "onebox", "pegasus cluster name");
    pegasus_app_name = dsn_config_get_value_string(
        "pegasus.benchmark", "pegasus_app_name", "temp", "pegasus app name");
    pegasus_timeout_ms =
        (int32_t)dsn_config_get_value_uint64("pegasus.benchmark",
                                             "pegasus_timeout_ms",
                                             1000,
                                             "pegasus read/write timeout in milliseconds");
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
    threads = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "threads", 1, "Number of concurrent threads to run");
    hashkey_size = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "hashkey_size", 16, "size of each hashkey");
    sortkey_size = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "sortkey_size", 16, "size of each sortkey");
    value_size = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "value_size", 100, "Size of each value");
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
