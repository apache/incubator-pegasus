// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <unordered_map>

#include "statistics.h"
#include "config.h"

namespace pegasus {
namespace test {

class benchmark;
struct thread_arg;
typedef void (benchmark::*bench_method)(thread_arg *);

struct thread_arg
{
    int64_t seed;
    statistics stats;
    bench_method method;
    benchmark *bm;

    thread_arg(uint64_t seed_,
               std::shared_ptr<rocksdb::Statistics> hist_stats_,
               bench_method bench_method_,
               benchmark *benchmark_)
        : seed(seed_), stats(hist_stats_), method(bench_method_), bm(benchmark_)
    {
    }
};

class benchmark
{
public:
    benchmark();
    ~benchmark() = default;
    void run();

private:
    /** thread main function */
    static void thread_body(void *v);

    /** benchmark operations **/
    void run_benchmark(int thread_count, operation_type op_type);
    void write_random(thread_arg *thread);
    void read_random(thread_arg *thread);
    void delete_random(thread_arg *thread);

    /**  generate hash/sort key and value */
    void generate_kv_pair(std::string &hashkey, std::string &sortkey, std::string &value);

    /** some auxiliary functions */
    operation_type get_operation_type(const std::string &name);
    void print_header();
    void print_warnings();

private:
    // the pegasus client to do read/write/delete operations
    pegasus_client *_client;
    // the map of operation type and the process method
    std::unordered_map<operation_type, bench_method, std::hash<unsigned char>> _operation_method;
};
} // namespace test
} // namespace pegasus
