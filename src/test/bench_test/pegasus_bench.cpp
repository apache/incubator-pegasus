// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#ifdef NUMA
#include <numa.h>
#include <numaif.h>
#endif

#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <sys/types.h>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <algorithm>
#include <zconf.h>
#include <iostream>
#include <dsn/utility/config_api.h>
#include <random>
#include <dsn/utility/rand.h>

#include <rocksdb/env.h>
#include <util/string_util.h>
#include <monitoring/histogram.h>
#include <rocksdb/rate_limiter.h>

#include "pegasus/client.h"
#include "mutex_lock.h"
#include "sync.h"
#include "utils.h"
#include "random_generator.h"
#include "stats.h"

namespace google {
}
namespace gflags {
}

using namespace google;
using namespace gflags;
using namespace pegasus;
using namespace rocksdb;
using namespace std;

namespace pegasus {
namespace test {

static const bool k_little_endian = PLATFORM_IS_LITTLE_ENDIAN;
static const string pegasus_config = "config.ini";
static rocksdb::Env *flags_env = rocksdb::Env::Default();
static string pegasus_cluster_name;
static string pegasus_app_name;
static uint32_t pegasus_timeout_ms;
static string benchmarks;
static uint32_t num_pairs;
static uint32_t seed;
static uint32_t threads;
static uint32_t duration_seconds;
static uint32_t value_size;
static uint32_t batch_size;
static uint32_t key_size;
static double compression_ratio;
static bool histogram;
static bool enable_numa;
static uint32_t ops_between_duration_checks;
static uint32_t stats_interval_seconds;
static uint64_t report_interval_seconds;
static string report_file;
static uint32_t thread_status_per_interval;
static uint64_t benchmark_write_rate_limit;
static uint64_t benchmark_read_rate_limit;
static uint32_t prefix_size;
static uint32_t keys_per_prefix;

// State shared by all concurrent executions of the same benchmark.
struct shared_state
{
    pegasus::test::mutex mu;
    pegasus::test::cond_var cv;
    int total;
    std::shared_ptr<RateLimiter> write_rate_limiter;
    std::shared_ptr<RateLimiter> read_rate_limiter;

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
    int tid;       // 0..n-1 when running in n threads
    stats stats_;
    shared_state *shared;

    /* implicit */
    thread_state(int index) : tid(index) {}
};

class duration
{
public:
    duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0)
    {
        max_seconds_ = max_seconds;
        max_ops_ = max_ops;
        ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
        ops_ = 0;
        start_at_ = flags_env->NowMicros();
    }

    int64_t get_stage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

    bool done(int64_t increment)
    {
        if (increment <= 0)
            increment = 1; // avoid done(0) and infinite loops
        ops_ += increment;

        if (max_seconds_) {
            // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
            auto granularity = ops_between_duration_checks;
            if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
                uint64_t now = flags_env->NowMicros();
                return ((now - start_at_) / 1000000) >= max_seconds_;
            } else {
                return false;
            }
        } else {
            return ops_ > max_ops_;
        }
    }

private:
    uint64_t max_seconds_;
    int64_t max_ops_;
    int64_t ops_per_stage_;
    int64_t ops_;
    uint64_t start_at_;
};

class benchmark
{
private:
    int64_t num_;
    int value_size_;
    int key_size_;
    int prefix_size_;
    int64_t keys_per_prefix_;
    int64_t entries_per_batch_; // TODO for multi set
    double read_random_exp_range_;

    void print_header()
    {
        print_environment();
        fprintf(stdout, "Keys:       %d bytes each\n", key_size);
        fprintf(stdout,
                "Values:     %d bytes each (%d bytes after compression)\n",
                value_size,
                static_cast<int>(value_size * compression_ratio + 0.5));
        fprintf(stdout, "Entries:    %" PRIu64 "\n", num_);
        fprintf(stdout, "Prefix:    %d bytes\n", prefix_size);
        fprintf(stdout, "Keys per prefix:    %" PRIu64 "\n", keys_per_prefix_);
        fprintf(stdout,
                "RawSize:    %.1f MB (estimated)\n",
                ((static_cast<int64_t>(key_size + value_size) * num_) / 1048576.0));
        fprintf(
            stdout,
            "FileSize:   %.1f MB (estimated)\n",
            (((key_size + value_size * compression_ratio) * num_) / 1048576.0));
        fprintf(stdout, "Write rate: %" PRIu64 " bytes/second\n", benchmark_write_rate_limit);
        fprintf(stdout, "Read rate: %" PRIu64 " ops/second\n", benchmark_read_rate_limit);
        if (enable_numa) {
            fprintf(stderr, "Running in NUMA enabled mode.\n");
#ifndef NUMA
            fprintf(stderr, "NUMA is not defined in the system.\n");
            exit(1);
#else
            if (numa_available() == -1) {
                fprintf(stderr, "NUMA is not supported by the system.\n");
                exit(1);
            }
#endif
        }

        print_warnings("");
        fprintf(stdout, "------------------------------------------------\n");
    }

    void print_warnings(const char *compression)
    {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
        fprintf(stdout, "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
        fprintf(stdout, "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
    }

// Current the following isn't equivalent to OS_LINUX.
#if defined(__linux)
    static Slice trim_space(Slice s)
    {
        unsigned int start = 0;
        while (start < s.size() && isspace(s[start])) {
            start++;
        }
        unsigned int limit = static_cast<unsigned int>(s.size());
        while (limit > start && isspace(s[limit - 1])) {
            limit--;
        }
        return Slice(s.data() + start, limit - start);
    }
#endif

    void print_environment()
    {
#if defined(__linux)
        time_t now = time(nullptr);
        char buf[52];
        // Lint complains about ctime() usage, so replace it with ctime_r(). The
        // requirement is to provide a buffer which is at least 26 bytes.
        fprintf(stderr, "Date:       %s", ctime_r(&now, buf)); // ctime_r() adds newline

        FILE *cpuinfo = fopen("/proc/cpuinfo", "r");
        if (cpuinfo != nullptr) {
            char line[1000];
            int num_cpus = 0;
            std::string cpu_type;
            std::string cache_size;
            while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
                const char *sep = strchr(line, ':');
                if (sep == nullptr) {
                    continue;
                }
                Slice key = trim_space(Slice(line, sep - 1 - line));
                Slice val = trim_space(Slice(sep + 1));
                if (key == "model name") {
                    ++num_cpus;
                    cpu_type = val.ToString();
                } else if (key == "cache size") {
                    cache_size = val.ToString();
                }
            }
            fclose(cpuinfo);
            fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
            fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
        }
#endif
    }

public:
    benchmark()
        : num_(num_pairs),
          value_size_(value_size),
          key_size_(key_size),
          prefix_size_(prefix_size),
          keys_per_prefix_(keys_per_prefix),
          entries_per_batch_(1),
          read_random_exp_range_(0.0)
    {

        if (prefix_size > key_size) {
            fprintf(stderr, "prefix size is larger than key size");
            exit(1);
        }
    }

    Slice allocate_key(std::unique_ptr<const char[]> *key_guard)
    {
        char *data = new char[key_size_];
        const char *const_data = data;
        key_guard->reset(const_data);
        return Slice(key_guard->get(), key_size_);
    }

    // Generate key according to the given specification and random number.
    // The resulting key will have the following format (if keys_per_prefix_
    // is positive), extra trailing bytes are either cut off or padded with '0'.
    // The prefix value is derived from key value.
    //   ----------------------------
    //   | prefix 00000 | key 00000 |
    //   ----------------------------
    // If keys_per_prefix_ is 0, the key is simply a binary representation of
    // random number followed by trailing '0's
    //   ----------------------------
    //   |        key 00000         |
    //   ----------------------------
    void generate_key_from_int(uint64_t v, int64_t num_keys, Slice *key)
    {
        char *start = const_cast<char *>(key->data());
        char *pos = start;
        if (keys_per_prefix_ > 0) {
            int64_t num_prefix = num_keys / keys_per_prefix_;
            int64_t prefix = v % num_prefix;
            int bytes_to_fill = std::min(prefix_size_, 8);
            if (k_little_endian) {
                for (int i = 0; i < bytes_to_fill; ++i) {
                    pos[i] = (prefix >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
                }
            } else {
                memcpy(pos, static_cast<void *>(&prefix), bytes_to_fill);
            }
            if (prefix_size_ > 8) {
                // fill the rest with 0s
                memset(pos + 8, '0', prefix_size_ - 8);
            }
            pos += prefix_size_;
        }

        int bytes_to_fill = std::min(key_size_ - static_cast<int>(pos - start), 8);
        if (k_little_endian) {
            for (int i = 0; i < bytes_to_fill; ++i) {
                pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
            }
        } else {
            memcpy(pos, static_cast<void *>(&v), bytes_to_fill);
        }
        pos += bytes_to_fill;
        if (key_size_ > pos - start) {
            memset(pos, '0', key_size_ - (pos - start));
        }
    }

    void run()
    {
        print_header();
        std::stringstream benchmark_stream(benchmarks);
        std::string name;
        while (std::getline(benchmark_stream, name, ',')) {
            // Sanitize parameters
            num_ = num_pairs;
            value_size_ = value_size;
            key_size_ = key_size;
            entries_per_batch_ = batch_size;

            void (benchmark::*method)(thread_state *) = nullptr;
            void (benchmark::*post_process_method)() = nullptr;

            int num_threads = threads;

            int num_repeat = 1;
            int num_warmup = 0;
            if (!name.empty() && *name.rbegin() == ']') {
                auto it = name.find('[');
                if (it == std::string::npos) {
                    fprintf(stderr, "unknown benchmark arguments '%s'\n", name.c_str());
                    exit(1);
                }
                std::string args = name.substr(it + 1);
                args.resize(args.size() - 1);
                name.resize(it);

                std::string bench_arg;
                std::stringstream args_stream(args);
                while (std::getline(args_stream, bench_arg, '-')) {
                    if (bench_arg.empty()) {
                        continue;
                    }
                    if (bench_arg[0] == 'X') {
                        // Repeat the benchmark n times
                        std::string num_str = bench_arg.substr(1);
                        num_repeat = std::stoi(num_str);
                    } else if (bench_arg[0] == 'W') {
                        // Warm up the benchmark for n times
                        std::string num_str = bench_arg.substr(1);
                        num_warmup = std::stoi(num_str);
                    }
                }
            }

            // Both fillseqdeterministic and filluniquerandomdeterministic
            // fill the levels except the max level with UNIQUE_RANDOM
            // and fill the max level with fillseq and filluniquerandom, respectively
            if (name == "fillrandom_pegasus") {
                method = &benchmark::write_random_rrdb;
            } else if (name == "readrandom_pegasus") {
                method = &benchmark::read_random_rrdb;
            } else if (name == "deleterandom_pegasus") {
                method = &benchmark::delete_random_rrdb;
            } else if (!name.empty()) { // No error message for empty name
                fprintf(stderr, "unknown benchmark '%s'\n", name.c_str());
                exit(1);
            }

            if (method != nullptr) {
                if (num_warmup > 0) {
                    printf("Warming up benchmark by running %d times\n", num_warmup);
                }

                for (int i = 0; i < num_warmup; i++) {
                    run_benchmark(num_threads, name, method);
                }

                if (num_repeat > 1) {
                    printf("Running benchmark for %d times\n", num_repeat);
                }

                combined_stats combined_stats_;
                for (int i = 0; i < num_repeat; i++) {
                    stats stats = run_benchmark(num_threads, name, method);
                    combined_stats_.add_stats(stats);
                }
                if (num_repeat > 1) {
                    combined_stats_.report(name);
                }
            }
            if (post_process_method != nullptr) {
                (this->*post_process_method)();
            }
        }
    }

private:
    struct thread_arg
    {
        benchmark *bm;
        shared_state *shared;
        thread_state *thread;
        void (benchmark::*method)(thread_state *);
    };

    static void thread_body(void *v)
    {
        thread_arg *arg = reinterpret_cast<thread_arg *>(v);
        shared_state *shared = arg->shared;
        thread_state *thread = arg->thread;
        {
            mutex_lock l(&shared->mu);
            shared->num_initialized++;
            if (shared->num_initialized >= shared->total) {
                shared->cv.signal_all();
            }
            while (!shared->start) {
                shared->cv.wait();
            }
        }

        thread->stats_.start(thread->tid);
        (arg->bm->*(arg->method))(thread);
        thread->stats_.stop();

        {
            mutex_lock l(&shared->mu);
            shared->num_done++;
            if (shared->num_done >= shared->total) {
                shared->cv.signal_all();
            }
        }
    }

    stats run_benchmark(int n, Slice name, void (benchmark::*method)(thread_state *))
    {
        shared_state shared;
        shared.total = n;
        shared.num_initialized = 0;
        shared.num_done = 0;
        shared.start = false;
        if (benchmark_write_rate_limit > 0) {
            shared.write_rate_limiter.reset(
                NewGenericRateLimiter(benchmark_write_rate_limit));
        }
        if (benchmark_read_rate_limit > 0) {
            shared.read_rate_limiter.reset(NewGenericRateLimiter(benchmark_read_rate_limit,
                                                                 100000 /* refill_period_us */,
                                                                 10 /* fairness */,
                                                                 RateLimiter::Mode::kReadsOnly));
        }

        std::unique_ptr<reporter_agent> reporter_agent_;
        if (report_interval_seconds > 0) {
            reporter_agent_.reset(
                new reporter_agent(flags_env, report_file, report_interval_seconds));
        }

        thread_arg *arg = new thread_arg[n];

        for (int i = 0; i < n; i++) {
#ifdef NUMA
            if (enable_numa) {
                // Performs a local allocation of memory to threads in numa node.
                int n_nodes = numa_num_task_nodes(); // Number of nodes in NUMA.
                numa_exit_on_error = 1;
                int numa_node = i % n_nodes;
                bitmask *nodes = numa_allocate_nodemask();
                numa_bitmask_clearall(nodes);
                numa_bitmask_setbit(nodes, numa_node);
                // numa_bind() call binds the process to the node and these
                // properties are passed on to the thread that is created in
                // StartThread method called later in the loop.
                numa_bind(nodes);
                numa_set_strict(1);
                numa_free_nodemask(nodes);
            }
#endif
            arg[i].bm = this;
            arg[i].method = method;
            arg[i].shared = &shared;
            arg[i].thread = new thread_state(i);
            arg[i].thread->stats_.set_reporter_agent(reporter_agent_.get());
            arg[i].thread->shared = &shared;
            flags_env->StartThread(thread_body, &arg[i]);
        }

        shared.mu.lock();
        while (shared.num_initialized < n) {
            shared.cv.wait();
        }

        shared.start = true;
        shared.cv.signal_all();
        while (shared.num_done < n) {
            shared.cv.wait();
        }
        shared.mu.unlock();

        // Stats for some threads can be excluded.
        stats merge_stats;
        for (int i = 0; i < n; i++) {
            merge_stats.merge(arg[i].thread->stats_);
        }
        merge_stats.report(name.ToString());

        for (int i = 0; i < n; i++) {
            delete arg[i].thread;
        }
        delete[] arg;

        return merge_stats;
    }

    enum write_mode
    {
        RANDOM,
        SEQUENTIAL,
        UNIQUE_RANDOM
    };

    void write_random_rrdb(thread_state *thread) { do_write_rrdb(thread, RANDOM); }

    class key_generator
    {
    public:
        key_generator(write_mode mode, uint64_t num, uint64_t num_per_set = 64 * 1024)
            : mode_(mode), num_(num), next_(0)
        {
            if (mode_ == UNIQUE_RANDOM) {
                // NOTE: if memory consumption of this approach becomes a concern,
                // we can either break it into pieces and only random shuffle a section
                // each time. Alternatively, use a bit map implementation
                // (https://reviews.facebook.net/differential/diff/54627/)
                values_.resize(num_);
                for (uint64_t i = 0; i < num_; ++i) {
                    values_[i] = i;
                }
                std::shuffle(values_.begin(),
                             values_.end(),
                             std::default_random_engine(static_cast<unsigned int>(seed)));
            }
        }

        uint64_t next()
        {
            switch (mode_) {
            case SEQUENTIAL:
                return next_++;
            case RANDOM:
                return dsn::rand::next_u32() % num_;
            case UNIQUE_RANDOM:
                assert(next_ + 1 <= num_); // TODO < -> <=
                return values_[next_++];
            }
            assert(false);
            return std::numeric_limits<uint64_t>::max();
        }

    private:
        write_mode mode_;
        const uint64_t num_;
        uint64_t next_;
        std::vector<uint64_t> values_;
    };

    void do_write_rrdb(thread_state *thread, write_mode write_mode)
    {
        const int test_duration = write_mode == RANDOM ? duration_seconds : 0;
        const int64_t num_ops = num_;

        std::unique_ptr<key_generator> key_gen;
        int64_t max_ops = num_ops;
        int64_t ops_per_stage = max_ops;

        duration duration_(test_duration, max_ops, ops_per_stage);
        key_gen.reset(new key_generator(write_mode, num_, ops_per_stage));

        if (num_ != num_pairs) {
            char msg[100];
            snprintf(msg, sizeof(msg), "(%" PRIu64 " ops)", num_);
            thread->stats_.add_message(msg);
        }

        random_generator gen(compression_ratio, value_size);
        int64_t bytes = 0;
        pegasus_client *client = pegasus_client_factory::get_client(
            pegasus_cluster_name.c_str(), pegasus_app_name.c_str());
        if (client == nullptr) {
            fprintf(stderr, "create client error\n");
            exit(1);
        }

        std::unique_ptr<const char[]> key_guard;
        Slice key = allocate_key(&key_guard);
        while (!duration_.done(1)) {
            if (thread->shared->write_rate_limiter.get() != nullptr) {
                thread->shared->write_rate_limiter->Request(value_size_ + key_size_, Env::IO_HIGH);
            }
            int64_t rand_num = key_gen->next();
            generate_key_from_int(rand_num, num_pairs, &key);
            int try_count = 0;
            while (true) {
                try_count++;
                int ret = client->set(key.ToString(),
                                      "",
                                      gen.generate(value_size_),
                                      pegasus_timeout_ms);
                if (ret == ::pegasus::PERR_OK) {
                    bytes += value_size_ + key_size_;
                    break;
                } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                    fprintf(stderr, "Set returned an error: %s\n", client->get_error_string(ret));
                    exit(1);
                } else {
                    fprintf(stderr, "Set timeout, retry(%d)\n", try_count);
                }
            }
            thread->stats_.finished_ops(nullptr, nullptr, 1, kWrite);
        }
        thread->stats_.add_bytes(bytes);
    }

    int64_t get_random_key()
    {
        uint64_t rand_int = dsn::rand::next_u64();
        int64_t key_rand;
        if (read_random_exp_range_ == 0) {
            key_rand = rand_int % num_pairs;
        } else {
            const uint64_t kBigInt = static_cast<uint64_t>(1U) << 62;
            long double order = -static_cast<long double>(rand_int % kBigInt) /
                                static_cast<long double>(kBigInt) * read_random_exp_range_;
            long double exp_ran = std::exp(order);
            uint64_t rand_num = static_cast<int64_t>(exp_ran * static_cast<long double>(num_pairs));
            // Map to a different number to avoid locality.
            const uint64_t kBigPrime = 0x5bd1e995;
            // Overflow is like %(2^64). Will have little impact of results.
            key_rand = static_cast<int64_t>((rand_num * kBigPrime) % num_pairs);
        }
        return key_rand;
    }

    void read_random_rrdb(thread_state *thread)
    {
        int64_t read = 0;
        int64_t found = 0;
        int64_t bytes = 0;
        std::unique_ptr<const char[]> key_guard;
        Slice key = allocate_key(&key_guard);
        pegasus_client *client = pegasus_client_factory::get_client(
            pegasus_cluster_name.c_str(), pegasus_app_name.c_str());
        if (client == nullptr) {
            fprintf(stderr, "Create client error\n");
            exit(1);
        }

        duration duration_(duration_seconds, num_pairs);
        while (!duration_.done(1)) {
            // We use same key_rand as seed for key and column family so that we can
            // deterministically find the cfh corresponding to a particular key, as it
            // is done in DoWrite method.
            int64_t key_rand = get_random_key();
            generate_key_from_int(key_rand, num_pairs, &key);
            read++;
            int try_count = 0;
            while (true) {
                try_count++;
                std::string value;
                int ret = client->get(key.ToString(), "", value, pegasus_timeout_ms);
                if (ret == ::pegasus::PERR_OK) {
                    found++;
                    bytes += key.size() + value.size();
                    break;
                } else if (ret == ::pegasus::PERR_NOT_FOUND) {
                    break;
                } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                    fprintf(stderr, "Get returned an error: %s\n", client->get_error_string(ret));
                    exit(1);
                } else {
                    fprintf(stderr, "Get timeout, retry(%d)\n", try_count);
                }
            }
            thread->stats_.finished_ops(nullptr, nullptr, 1, kRead);
        }

        char msg[100];
        snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)", found, read);

        thread->stats_.add_bytes(bytes);
        thread->stats_.add_message(msg);
    }

    void do_delete_rrdb(thread_state *thread, bool seq)
    {
        duration duration(seq ? 0 : duration_seconds, num_);
        int64_t i = 0;
        std::unique_ptr<const char[]> key_guard;
        Slice key = allocate_key(&key_guard);

        pegasus_client *client = pegasus_client_factory::get_client(
            pegasus_cluster_name.c_str(), pegasus_app_name.c_str());
        if (client == nullptr) {
            fprintf(stderr, "create client error\n");
            exit(1);
        }

        while (!duration.done(1)) {
            const int64_t k = seq ? i : (dsn::rand::next_u64() % num_pairs);
            generate_key_from_int(k, num_pairs, &key);
            int try_count = 0;
            while (true) {
                try_count++;
                int ret = client->del(key.ToString(), "", pegasus_timeout_ms);
                if (ret == ::pegasus::PERR_OK) {
                    break;
                } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                    fprintf(stderr, "Del returned an error: %s\n", client->get_error_string(ret));
                    exit(1);
                } else {
                    fprintf(stderr, "Get timeout, retry(%d)\n", try_count);
                }
            }
            thread->stats_.finished_ops(nullptr, nullptr, 1, kDelete);
            i++;
        }
    }

    void delete_random_rrdb(thread_state *thread) { do_delete_rrdb(thread, false); }
};
} // namespace test
} // namespace pegasus

int db_bench_tool(int argc, char **argv)
{
    static bool initialized = false;
    if (!initialized) {
        std::cout << "\nUSAGE:\n" << argv[0] << " [OPTIONS]...";
        initialized = true;
    }

    bool init = ::pegasus::pegasus_client_factory::initialize(pegasus::test::pegasus_config.c_str());
    if (!init) {
        fprintf(stderr, "Init pegasus error\n");
        return -1;
    }
    sleep(1);
    fprintf(stdout, "Init pegasus succeed\n");

    pegasus::test::benchmark bm;
    bm.run();
    sleep(1);    // Sleep a while to exit gracefully.
    return 0;
}

int main(int argc, char **argv) { return db_bench_tool(argc, argv); }
