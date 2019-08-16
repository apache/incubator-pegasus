// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <rocksdb/statistics.h>

#include "utils.h"

namespace pegasus {
namespace test {
class statistics
{
public:
    statistics(std::shared_ptr<rocksdb::Statistics> hist_stats = nullptr);
    void start(int id);
    void finished_ops(int64_t num_ops, enum operation_type op_type);
    void stop();
    void merge(const statistics &other);
    void report(operation_type op_type);
    void add_bytes(int64_t n);
    void add_message(const std::string &msg);
    void set_hist_stats(std::shared_ptr<rocksdb::Statistics> hist_stats);

private:
    uint32_t report_step(uint64_t current_report) const;
    void print_thread_status() const;

    // thread id which controls this statistics
    int _tid;
    // the start time of benchmark
    uint64_t _start;
    // the stop time of benchmark
    uint64_t _finish;
    // how many operations are done
    uint64_t _done;
    // the point(operation count) at which the next report
    uint64_t _next_report;
    // how many bytes the benchmark read/write
    uint64_t _bytes;
    // the last operation's finish time
    uint64_t _last_op_finish;
    // the information of benchmark operation
    std::string _message;
    // histogram performance analyzer
    std::shared_ptr<rocksdb::Statistics> _hist_stats;
};
} // namespace test
} // namespace pegasus
