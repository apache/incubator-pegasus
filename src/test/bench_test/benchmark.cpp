//
// Created by mi on 2019/8/7.
//

#include <sstream>
#include <dsn/utility/rand.h>
#include <tgmath.h>
#include <pegasus/client.h>
#include "benchmark.h"
#include "duration.h"
#include "random_generator.h"
#include "key_generator.h"

namespace pegasus {
namespace test {

static std::unordered_map<operation_type, std::string, std::hash<unsigned char>> operation_type_string = {
        {kRead, "read"}, {kWrite, "write"}, {kDelete, "delete"}, {kScan, "scan"}, {kOthers, "op"}};

benchmark::benchmark()
: num_(config::get_instance()->_num_pairs),
  value_size_(config::get_instance()->_value_size),
  key_size_(config::get_instance()->_key_size),
  prefix_size_(config::get_instance()->_prefix_size),
  keys_per_prefix_(config::get_instance()->_keys_per_prefix),
  entries_per_batch_(1),
  read_random_exp_range_(0.0)
{
    if (config::get_instance()->_prefix_size > config::get_instance()->_key_size) {
        fprintf(stderr, "prefix size is larger than key size");
        exit(1);
    }
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
void benchmark::generate_key_from_int(uint64_t v, int64_t num_keys, std::string *key)
{
    char *start = const_cast<char *>(key->data());
    char *pos = start;
    if (keys_per_prefix_ > 0) {
        int64_t num_prefix = num_keys / keys_per_prefix_;
        int64_t prefix = v % num_prefix;
        int bytes_to_fill = std::min(prefix_size_, 8);
        if (config::get_instance()->_little_endian) {
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
    if (config::get_instance()->_little_endian) {
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

void benchmark::run()
{
    print_header();
    std::stringstream benchmark_stream(config::get_instance()->_benchmarks);
    std::string name;
    while (std::getline(benchmark_stream, name, ',')) {
        // Sanitize parameters
        num_ = config::get_instance()->_num_pairs;
        value_size_ = config::get_instance()->_value_size;
        key_size_ = config::get_instance()->_key_size;
        entries_per_batch_ = config::get_instance()->_batch_size;

        void (benchmark::*method)(thread_state *) = nullptr;
        void (benchmark::*post_process_method)() = nullptr;

        int num_threads = config::get_instance()->_threads;

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
            std::shared_ptr<rocksdb::Statistics> hist_stats =
                    config::get_instance()->_histogram ? rocksdb::CreateDBStatistics() : nullptr;

            for (int i = 0; i < num_repeat; i++) {
                stats stats_ = run_benchmark(num_threads, name, method, hist_stats);
                combined_stats_.add_stats(stats_);
            }
            if (num_repeat > 1) {
                combined_stats_.report(name);
            }
            if (config::get_instance()->_histogram) {
                for (auto type : operation_type_string) {
                    fprintf(stdout,
                            "Microseconds per %s:\n%s\n",
                            operation_type_string[type.first].c_str(),
                            hist_stats->getHistogramString(type.first).c_str());
                }
            }
        }
        if (post_process_method != nullptr) {
            (this->*post_process_method)();
        }
    }
}

void benchmark::thread_body(void *v)
{
    thread_arg *arg = reinterpret_cast<thread_arg *>(v);
    shared_state *shared = arg->shared;
    thread_state *thread = arg->thread;
    {
        rocksdb::MutexLock l(&shared->mu);
        shared->num_initialized++;
        if (shared->num_initialized >= shared->total) {
            shared->cv.SignalAll();
        }
        while (!shared->start) {
            shared->cv.Wait();
        }
    }

    thread->_stats.start(thread->_tid);
    (arg->bm->*(arg->method))(thread);
    thread->_stats.stop();

    {
        rocksdb::MutexLock l(&shared->mu);
        shared->num_done++;
        if (shared->num_done >= shared->total) {
            shared->cv.SignalAll();
        }
    }
}

stats benchmark::run_benchmark(
        int n,
        const std::string &name,
        void (benchmark::*method)(thread_state *),
        std::shared_ptr<rocksdb::Statistics> hist_stats)
{
    shared_state shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;
    if (config::get_instance()->_benchmark_write_rate_limit > 0) {
        shared.write_rate_limiter.reset(
                rocksdb::NewGenericRateLimiter(config::get_instance()->_benchmark_write_rate_limit));
    }
    if (config::get_instance()->_benchmark_read_rate_limit > 0) {
        shared.read_rate_limiter.reset(NewGenericRateLimiter(config::get_instance()->_benchmark_read_rate_limit,
                                                             100000 /* refill_period_us */,
                                                             10 /* fairness */,
                                                             rocksdb::RateLimiter::Mode::kReadsOnly));
    }

    std::unique_ptr<reporter_agent> reporter_agent_;
    if (config::get_instance()->_report_interval_seconds > 0) {
        reporter_agent_.reset(new reporter_agent(
                config::get_instance()->_env,
                config::get_instance()->_report_file,
                config::get_instance()->_report_interval_seconds));
    }

    thread_arg *arg = new thread_arg[n];

    for (int i = 0; i < n; i++) {
#ifdef NUMA
        if (FLAGS_enable_numa) {
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
        arg[i].thread->_stats.set_reporter_agent(reporter_agent_.get());
        arg[i].thread->_stats.set_hist_stats(hist_stats);
        arg[i].thread->_shared = &shared;
        config::get_instance()->_env->StartThread(thread_body, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
        shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
        shared.cv.Wait();
    }
    shared.mu.Unlock();

    // Stats for some threads can be excluded.
    stats merge_stats;
    for (int i = 0; i < n; i++) {
        merge_stats.merge(arg[i].thread->_stats);
    }
    merge_stats.report(name);

    for (int i = 0; i < n; i++) {
        delete arg[i].thread;
    }
    delete[] arg;

    return merge_stats;
}

void benchmark::write_random_rrdb(thread_state *thread)
{
    do_write_rrdb(thread, RANDOM);
}

void benchmark::do_write_rrdb(thread_state *thread, write_mode write_mode)
{
    const int test_duration = write_mode == RANDOM ? config::get_instance()->_duration_seconds : 0;
    const int64_t num_ops = num_;

    std::unique_ptr<key_generator> key_gen;
    int64_t max_ops = num_ops;
    int64_t ops_per_stage = max_ops;

    duration duration(test_duration, max_ops, ops_per_stage);
    key_gen.reset(new key_generator(write_mode, num_, ops_per_stage));

    if (num_ != config::get_instance()->_num_pairs) {
        char msg[100];
        snprintf(msg, sizeof(msg), "(%" PRIu64 " ops)", num_);
        thread->_stats.add_message(msg);
    }

    random_generator gen = random_generator(
            config::get_instance()->_compression_ratio,
            config::get_instance()->_value_size);
    int64_t bytes = 0;
    pegasus_client *client = pegasus_client_factory::get_client(
            config::get_instance()->_pegasus_cluster_name.c_str(),
            config::get_instance()->_pegasus_app_name.c_str());
    if (client == nullptr) {
        fprintf(stderr, "create client error\n");
        exit(1);
    }

    std::unique_ptr<const char[]> key_guard;
    std::string key = allocate_key(&key_guard);
    while (!duration.done(1)) {
        if (thread->_shared->write_rate_limiter.get() != nullptr) {
            thread->_shared->write_rate_limiter->Request(value_size_ + key_size_, rocksdb::Env::IO_HIGH);
        }
        int64_t rand_num = key_gen->next();
        generate_key_from_int(rand_num, config::get_instance()->_num_pairs, &key);
        int try_count = 0;
        while (true) {
            try_count++;
            int ret = client->set(key,
                                  "",
                                  gen.generate(value_size_),
                                  config::get_instance()->_pegasus_timeout_ms);
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
        thread->_stats.finished_ops(nullptr, nullptr, 1, kWrite);
    }
    thread->_stats.add_bytes(bytes);
}

int64_t benchmark::get_random_key()
{
    uint64_t rand_int = dsn::rand::next_u64();
    int64_t key_rand;
    if (read_random_exp_range_ == 0) {
        key_rand = rand_int % config::get_instance()->_num_pairs;
    } else {
        const uint64_t kBigInt = static_cast<uint64_t>(1U) << 62;
        long double order = -static_cast<long double>(rand_int % kBigInt) /
                            static_cast<long double>(kBigInt) * read_random_exp_range_;
        long double exp_ran = std::exp(order);
        uint64_t rand_num = static_cast<int64_t>(
                exp_ran * static_cast<long double>(config::get_instance()->_num_pairs));
        // Map to a different number to avoid locality.
        const uint64_t kBigPrime = 0x5bd1e995;
        // Overflow is like %(2^64). Will have little impact of results.
        key_rand = static_cast<int64_t>((rand_num * kBigPrime) % config::get_instance()->_num_pairs);
    }
    return key_rand;
}

void benchmark::read_random_rrdb(thread_state *thread)
{
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;
    std::unique_ptr<const char[]> key_guard;
    std::string key = allocate_key(&key_guard);
    pegasus_client *client = pegasus_client_factory::get_client(
            config::get_instance()->_pegasus_cluster_name.c_str(),
            config::get_instance()->_pegasus_app_name.c_str());
    if (client == nullptr) {
        fprintf(stderr, "Create client error\n");
        exit(1);
    }

    duration duration(config::get_instance()->_duration_seconds, config::get_instance()->_num_pairs);
    while (!duration.done(1)) {
        // We use same key_rand as seed for key and column family so that we can
        // deterministically find the cfh corresponding to a particular key, as it
        // is done in DoWrite method.
        int64_t key_rand = get_random_key();
        generate_key_from_int(key_rand, config::get_instance()->_num_pairs, &key);
        read++;
        int try_count = 0;
        while (true) {
            try_count++;
            std::string value;
            int ret = client->get(key, "", value, config::get_instance()->_pegasus_timeout_ms);
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
        thread->_stats.finished_ops(nullptr, nullptr, 1, kRead);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)", found, read);

    thread->_stats.add_bytes(bytes);
    thread->_stats.add_message(msg);
}

std::string benchmark::allocate_key(std::unique_ptr<const char[]> *key_guard)
{
    char *data = new char[key_size_];
    const char *const_data = data;
    key_guard->reset(const_data);
    return std::string(key_guard->get(), key_size_);
}

void benchmark::do_delete_rrdb(thread_state *thread, bool seq)
{
    duration duration(seq ? 0 : config::get_instance()->_duration_seconds, num_);
    int64_t i = 0;
    std::unique_ptr<const char[]> key_guard;
    std::string key = allocate_key(&key_guard);

    pegasus_client *client = pegasus_client_factory::get_client(
            config::get_instance()->_pegasus_cluster_name.c_str(),
            config::get_instance()->_pegasus_app_name.c_str());
    if (client == nullptr) {
        fprintf(stderr, "create client error\n");
        exit(1);
    }

    while (!duration.done(1)) {
        const int64_t k = seq ? i : (dsn::rand::next_u64() % config::get_instance()->_num_pairs);
        generate_key_from_int(k, config::get_instance()->_num_pairs, &key);
        int try_count = 0;
        while (true) {
            try_count++;
            int ret = client->del(key, "", config::get_instance()->_pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                break;
            } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                fprintf(stderr, "Del returned an error: %s\n", client->get_error_string(ret));
                exit(1);
            } else {
                fprintf(stderr, "Get timeout, retry(%d)\n", try_count);
            }
        }
        thread->_stats.finished_ops(nullptr, nullptr, 1, kDelete);
        i++;
    }
}

void benchmark::delete_random_rrdb(thread_state *thread)
{
    do_delete_rrdb(thread, false);
}

void benchmark::print_header()
{
    print_environment();
    fprintf(stdout, "Keys:       %d bytes each\n", config::get_instance()->_key_size);
    fprintf(stdout,
            "Values:     %d bytes each (%d bytes after compression)\n",
            config::get_instance()->_value_size,
            static_cast<int>(config::get_instance()->_value_size * config::get_instance()->_compression_ratio + 0.5));
    fprintf(stdout, "Entries:    %" PRIu64 "\n", num_);
    fprintf(stdout, "Prefix:    %d bytes\n", config::get_instance()->_prefix_size);
    fprintf(stdout, "Keys per prefix:    %" PRIu64 "\n", keys_per_prefix_);
    fprintf(stdout,
            "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(config::get_instance()->_key_size + config::get_instance()->_value_size) * num_) / 1048576.0));
    fprintf(stdout,
            "FileSize:   %.1f MB (estimated)\n",
            (((config::get_instance()->_key_size
               + config::get_instance()->_value_size * config::get_instance()->_compression_ratio) * num_) / 1048576.0));
    fprintf(stdout, "Write rate: %" PRIu64 " bytes/second\n", config::get_instance()->_benchmark_write_rate_limit);
    fprintf(stdout, "Read rate: %" PRIu64 " ops/second\n", config::get_instance()->_benchmark_read_rate_limit);
    if (config::get_instance()->_enable_numa) {
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

void benchmark::print_warnings(const char *compression)
{
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(stdout, "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
    fprintf(stdout, "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
}

std::string benchmark::trim_space(const std::string &s)
{
    unsigned int start = 0;
    while (start < s.size() && isspace(s[start])) {
        start++;
    }
    unsigned int limit = static_cast<unsigned int>(s.size());
    while (limit > start && isspace(s[limit - 1])) {
        limit--;
    }
    return std::string(s.data() + start, limit - start);
}

void benchmark::print_environment()
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
            std::string key = trim_space(std::string(line, sep - 1 - line));
            std::string val = trim_space(std::string(sep + 1));
            if (key == "model name") {
                ++num_cpus;
                cpu_type = val;
            } else if (key == "cache size") {
                cache_size = val;
            }
        }
        fclose(cpuinfo);
        fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
        fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
}
}
}
