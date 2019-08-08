//
// Created by mi on 2019/8/7.
//

#pragma once

#include <memory>
#include <cinttypes>
#include <unordered_map>

#include <port/port_posix.h>
#include <util/mutexlock.h>
#include <rocksdb/rate_limiter.h>

#include "stats.h"
#include "config.h"

namespace pegasus {
namespace test {

// State shared by all concurrent executions of the same benchmark.
struct shared_state
{
    rocksdb::port::Mutex mu;
    rocksdb::port::CondVar cv;
    int total;
    std::shared_ptr<rocksdb::RateLimiter> write_rate_limiter;
    std::shared_ptr<rocksdb::RateLimiter> read_rate_limiter;

    // Each thread goes through the following states:
    //    (1) initializing
    //    (2) waiting for others to be initialized
    //    (3) running
    //    (4) done

    long num_initialized;
    long num_done;
    bool start;

    shared_state() : cv(&mu) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct thread_state
{
    int _tid;       // 0..n-1 when running in n threads
    pegasus::test::stats _stats;
    shared_state *_shared;

    /* implicit */
    thread_state(int index) : _tid(index) {}
};

class benchmark;
struct thread_arg
{
    benchmark *bm;
    shared_state *shared;
    thread_state *thread;
    void (benchmark::*method)(thread_state *);
};

class benchmark {
public:
    benchmark();
    void run();

private:
    /** thread main function */
    static void thread_body(void *v);

    /** benchmark operations **/
    stats run_benchmark(
            int n,
            const std::string &name,
            void (benchmark::*method)(thread_state *),
            std::shared_ptr<rocksdb::Statistics> hist_stats = nullptr);
    void write_random_rrdb(thread_state *thread);
    void do_write_rrdb(thread_state *thread, write_mode write_mode);
    int64_t get_random_key();
    void read_random_rrdb(thread_state *thread);
    void delete_random_rrdb(thread_state *thread);
    void do_delete_rrdb(thread_state *thread, bool seq);

    /** some auxiliary functions */
    std::string allocate_key(std::unique_ptr<const char[]> *key_guard);
    void generate_key_from_int(uint64_t v, int64_t num_keys, std::string *key);
    void print_header();
    void print_warnings(const char *compression);
    std::string trim_space(const std::string &s);
    void print_environment();

private:
    int64_t num_;
    int value_size_;
    int key_size_;
    int prefix_size_;
    int64_t keys_per_prefix_;
    int64_t entries_per_batch_; // TODO for multi set
    double read_random_exp_range_;
};
}
}
