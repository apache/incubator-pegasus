// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <memory>
#include <cinttypes>
#include <unordered_map>

#include "stats.h"
#include "config.h"

namespace pegasus {
namespace test {
// State shared by all concurrent executions of the same benchmark.
struct shared_state
{
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int total;

    // Each thread goes through the following states:
    //    (1) initializing
    //    (2) waiting for others to be initialized
    //    (3) running
    //    (4) done

    long num_initialized;
    long num_done;
    bool start;

    shared_state(int total) : total(total), num_initialized(0), num_done(0), start(false)
    {
        pthread_mutex_init(&mu, NULL);
        pthread_cond_init(&cv, NULL);
    }
};

// Per-thread state for concurrent executions of the same benchmark.
struct thread_state
{
    int tid; // 0..n-1 when running in n threads
    pegasus::test::stats stats;

    /* implicit */
    thread_state(int index) : tid(index) {}
};

class benchmark;
typedef void (benchmark::*bench_method)(thread_state *);

struct thread_arg
{
    benchmark *bm;
    shared_state *shared;
    thread_state *thread;
    bench_method method;
};

class benchmark
{
public:
    benchmark();
    void run();

private:
    /** thread main function */
    static void thread_body(void *v);

    /** benchmark operations **/
    stats run_benchmark(int n,
                        const std::string &name,
                        bench_method method,
                        std::shared_ptr<rocksdb::Statistics> hist_stats = nullptr);
    void write_random(thread_state *thread);
    void read_random(thread_state *thread);
    void delete_random(thread_state *thread);
    void do_write(thread_state *thread,
                  const std::vector<std::string> &hashkeys,
                  const std::vector<std::string> &sortkeys,
                  const std::vector<std::string> &values);

    /**  generate hash/sort key and value */
    void generate_random_keys(std::vector<std::string> &hashkeys,
                              std::vector<std::string> &sortkeys);
    void generate_random_values(std::vector<std::string> &hashkeys);
    std::string allocate_hashkey();
    std::string allocate_sortkey();
    std::string allocate_key(int key_size);
    void generate_hashkey_from_int(uint64_t v, std::string *key);
    void generate_sortkey_from_int(uint64_t v, std::string *key);
    void generate_key_from_int(uint64_t v, std::string *key, int key_size);
    int64_t get_random_num();

    /** some auxiliary functions */
    operation_type get_operation_type(const std::string &name);
    void print_header();
    void print_warnings(const char *compression);

private:
    // the pegasus client to do read/write/delete operations
    pegasus_client *_client;
    // the map of operation type and the process method
    std::unordered_map<operation_type, bench_method, std::hash<unsigned char>> _operation_method;
};
} // namespace test
} // namespace pegasus
