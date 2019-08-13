// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <rocksdb/statistics.h>

#include "utils.h"

namespace pegasus {
namespace test {
class combined_stats;
class stats
{
public:
    stats();
    void set_hist_stats(std::shared_ptr<rocksdb::Statistics> hist_stats_);
    void start(int id);
    void merge(const stats &other);
    void stop();
    void add_message(const std::string &msg);
    void print_thread_status();
    void finished_ops(int64_t num_ops, enum operation_type op_type);
    void add_bytes(int64_t n);
    void report(const std::string &name);

private:
    int id_;
    uint64_t start_;
    uint64_t finish_;
    double seconds_;
    uint64_t done_;
    uint64_t last_report_done_;
    uint64_t next_report_;
    uint64_t bytes_;
    uint64_t last_op_finish_;
    uint64_t last_report_finish_;
    std::shared_ptr<rocksdb::Statistics> hist_stats;
    std::string message_;
    bool exclude_from_merge_;
    friend class combined_stats;
};

class combined_stats
{
public:
    void add_stats(const stats &stat);
    void report(const std::string &bench_name);

private:
    double calc_avg(std::vector<double> &data);
    double calc_median(std::vector<double> &data);

    std::vector<double> throughput_ops_;
    std::vector<double> throughput_mbs_;
};
} // namespace test
} // namespace pegasus
