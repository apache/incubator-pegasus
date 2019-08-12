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
struct thread_arg
{
    benchmark *bm;
    shared_state *shared;
    thread_state *thread;
    void (benchmark::*method)(thread_state *);
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
                        void (benchmark::*method)(thread_state *),
                        std::shared_ptr<rocksdb::Statistics> hist_stats = nullptr);
    void write_random(thread_state *thread);
    void do_write(thread_state *thread, write_mode write_mode);
    int64_t get_random_key();
    void read_random(thread_state *thread);
    void delete_random(thread_state *thread);
    void do_delete(thread_state *thread, bool seq);

    /** some auxiliary functions */
    std::string allocate_key(std::unique_ptr<const char[]> *key_guard);
    void generate_key_from_int(uint64_t v, int64_t num_keys, std::string *key);
    void print_header();
    void print_warnings(const char *compression);
    void generate_random_keys(uint32_t num,
                              std::vector<std::string> &hashkeys,
                              std::vector<std::string> &sortkeys);

private:
    int64_t num_;
    int value_size_;
    int key_size_;
    int prefix_size_;
    int64_t keys_per_prefix_;
    pegasus_client *client;
};
} // namespace test
} // namespace pegasus
