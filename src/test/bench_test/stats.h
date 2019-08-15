// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <rocksdb/statistics.h>

#include "utils.h"

namespace pegasus {
namespace test {
class stats
{
public:
    stats();
    void set_hist_stats(std::shared_ptr<rocksdb::Statistics> hist_stats);
    void start(int id);
    void merge(const stats &other);
    void stop();
    void add_message(const std::string &msg);
    void print_thread_status();
    void finished_ops(int64_t num_ops, enum operation_type op_type);
    void add_bytes(int64_t n);
    void report(const std::string &name);

private:
    uint32_t report_default_step(uint64_t current_report);

    // thread id which controls this stats
    int _tid;
    // the start time of benchmark
    uint64_t _start;
    // the end time of benchmark
    uint64_t _finish;
    // senconds that benchmark costs
    double _seconds;
    // how many operations are done
    uint64_t _done;
    // the point(operation count) at which the next report
    uint64_t _next_report;
    // how many bytes the benchmark read/write
    uint64_t _bytes;
    // the last operation's finish time
    uint64_t _last_op_finish;
    // performance analyzer
    std::shared_ptr<rocksdb::Statistics> _hist_stats;
    // the information of benchmark operation
    std::string message_;
};
} // namespace test
} // namespace pegasus
