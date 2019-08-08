// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <memory>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include <rocksdb/env.h>
#include <port/port_posix.h>

#include "utils.h"

namespace pegasus {
namespace test {
class reporter_agent {
public:
    reporter_agent(rocksdb::Env *env, const std::string &fname, uint64_t report_interval_secs);
    ~reporter_agent();
    void report_finished_ops(int64_t num_ops);

private:
    std::string header() const;
    void sleep_and_report();

    rocksdb::Env *env_;
    std::unique_ptr<rocksdb::WritableFile> report_file_;
    std::atomic<int64_t> total_ops_done_;
    int64_t last_report_;
    const uint64_t report_interval_secs_;
    std::thread reporting_thread_;
    std::mutex mutex_;
    // will notify on stop
    std::condition_variable stop_cv_;
    bool stop_;
};
}
}
