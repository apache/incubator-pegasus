// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "config.h"
#include "utils.h"

namespace pegasus {
namespace test {

config *config::get_instance()
{
    static config instance;
    return &instance;
}

config::config()
{
    _key_size = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "key_size", 0, "size of each key");
    _prefix_size = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark",
        "prefix_size",
        0,
        "control the prefix size for HashSkipList and plain table");
    if (config::get_instance()->_prefix_size > config::get_instance()->_key_size) {
        fprintf(stderr, "prefix size is larger than key size");
        exit(1);
    }
    pegasus_cluster_name = dsn_config_get_value_string(
        "pegasus.benchmark", "pegasus_cluster_name", "", "pegasus cluster name");
    pegasus_app_name = dsn_config_get_value_string(
        "pegasus.benchmark", "pegasus_app_name", "", "pegasus app name");
    pegasus_timeout_ms = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "pegasus_timeout_ms", 0, "pegasus read/write timeout in milliseconds");
    benchmarks = dsn_config_get_value_string("pegasus.benchmark", "benchmarks", "", "");
    num = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "num", 0, "Number of key/values to place in database");
    seed = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "seed", 0, "Seed base for random number generators");
    threads = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "threads", 0, "Number of concurrent threads to run");
    duration_seconds = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "duration", 0, "Time in seconds for the random-ops tests to run");
    value_size = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "value_size", 0, "Size of each value");
    _batch_size =
        (int32_t)dsn_config_get_value_uint64("pegasus.benchmark", "batch_size", 0, "Batch size");
    _compression_ratio = dsn_config_get_value_double("pegasus.benchmark",
                                                     "compression_ratio",
                                                     0,
                                                     "Arrange to generate values that shrink, to "
                                                     "this fraction of their original size after "
                                                     "compression");
    _ops_between_duration_checks = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "ops_between_duration_checks", 0, "Check duration limit every x ops");
    _stats_interval = dsn_config_get_value_uint64(
        "pegasus.benchmark",
        "stats_interval",
        0,
        "Stats are reported every N operations when this is greater than zero");
    _stats_interval_seconds = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "stats_interval_seconds", 0, "Report stats every N seconds.");
    _report_interval_seconds = dsn_config_get_value_uint64("pegasus.benchmark",
                                                           "report_interval_seconds",
                                                           0,
                                                           "If greater than zero, it will write "
                                                           "simple stats in CVS format to "
                                                           "--report_file every N seconds");
    _report_file = dsn_config_get_value_string("pegasus.benchmark",
                                               "report_file",
                                               "",
                                               "Filename where some simple stats are reported to "
                                               "(if --report_interval_seconds is bigger than 0)");
    _thread_status_per_interval =
        (int32_t)dsn_config_get_value_uint64("pegasus.benchmark",
                                             "thread_status_per_interval",
                                             0,
                                             "Takes and report a snapshot of the "
                                             "current status of each thread when "
                                             "this is greater than 0");
    _keys_per_prefix =
        (int32_t)dsn_config_get_value_uint64("pegasus.benchmark", "keys_per_prefix", 0, "");
    _env = rocksdb::Env::Default();
}
} // namespace test
} // namespace pegasus