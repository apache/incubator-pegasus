//
// Created by mi on 2019/8/7.
//

#pragma once

#include <string>
#include <dsn/utility/config_api.h>
#include <rocksdb/env.h>

namespace pegasus {
namespace test {

/** Thread safety singleton */
struct config
{
    static config *get_instance();

    /** config parameters */
    std::string _pegasus_cluster_name;
    std::string _pegasus_app_name;
    uint32_t _pegasus_timeout_ms;
    std::string _benchmarks;
    uint32_t _num_pairs;
    uint32_t _seed;
    uint32_t _threads;
    uint32_t _duration_seconds;
    uint32_t _value_size;
    uint32_t _batch_size;
    uint32_t _key_size;
    double _compression_ratio;
    bool _histogram;
    bool _enable_numa;
    uint32_t _ops_between_duration_checks;
    uint64_t _stats_interval;
    uint32_t _stats_interval_seconds;
    uint64_t _report_interval_seconds;
    std::string _report_file;
    uint32_t _thread_status_per_interval;
    uint64_t _benchmark_write_rate_limit;
    uint64_t _benchmark_read_rate_limit;
    uint32_t _prefix_size;
    uint32_t _keys_per_prefix;
    rocksdb::Env *_env;
    bool _little_endian;

private:
    config();
};
}
}
