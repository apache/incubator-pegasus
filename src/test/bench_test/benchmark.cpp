// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <sstream>
#include <dsn/utility/rand.h>
#include <tgmath.h>
#include <pegasus/client.h>
#include "benchmark.h"
#include "random_generator.h"

namespace pegasus {
namespace test {

static std::unordered_map<operation_type, std::string, std::hash<unsigned char>>
    operation_type_string = {
        {kRead, "read"}, {kWrite, "write"}, {kDelete, "delete"}, {kScan, "scan"}, {kOthers, "op"}};

benchmark::benchmark()
    : num_(config::get_instance()->num),
      value_size_(config::get_instance()->value_size),
      key_size_(config::get_instance()->key_size),
      prefix_size_(config::get_instance()->prefix_size),
      keys_per_prefix_(config::get_instance()->keys_per_prefix)
{
    client =
        pegasus_client_factory::get_client(config::get_instance()->pegasus_cluster_name.c_str(),
                                           config::get_instance()->pegasus_app_name.c_str());
    if (client == nullptr) {
        fprintf(stderr, "create client error\n");
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
        memcpy(pos, static_cast<void *>(&prefix), bytes_to_fill);
        if (prefix_size_ > 8) {
            // fill the rest with 0s
            memset(pos + 8, '0', prefix_size_ - 8);
        }
        pos += prefix_size_;
    }

    int bytes_to_fill = std::min(key_size_ - static_cast<int>(pos - start), 8);
    memcpy(pos, static_cast<void *>(&v), bytes_to_fill);
    pos += bytes_to_fill;
    if (key_size_ > pos - start) {
        memset(pos, '0', key_size_ - (pos - start));
    }
}

void benchmark::run()
{
    print_header();
    std::stringstream benchmark_stream(config::get_instance()->benchmarks);
    std::string name;
    while (std::getline(benchmark_stream, name, ',')) {
        void (benchmark::*method)(thread_state *) = nullptr;
        // Both fillseqdeterministic and filluniquerandomdeterministic
        // fill the levels except the max level with UNIQUE_RANDOM
        // and fill the max level with fillseq and filluniquerandom, respectively
        if (name == "fillrandom_pegasus") {
            method = &benchmark::write_random;
        } else if (name == "readrandom_pegasus") {
            method = &benchmark::read_random;
        } else if (name == "deleterandom_pegasus") {
            method = &benchmark::delete_random;
        } else if (!name.empty()) { // No error message for empty name
            fprintf(stderr, "unknown benchmark '%s'\n", name.c_str());
            exit(1);
        }

        if (method != nullptr) {
            combined_stats combined_stats_;
            std::shared_ptr<rocksdb::Statistics> hist_stats = rocksdb::CreateDBStatistics();

            stats stats_ = run_benchmark(config::get_instance()->threads, name, method, hist_stats);
            combined_stats_.add_stats(stats_);
            combined_stats_.report(name);
            for (auto type : operation_type_string) {
                fprintf(stdout,
                        "Microseconds per %s:\n%s\n",
                        operation_type_string[type.first].c_str(),
                        hist_stats->getHistogramString(type.first).c_str());
            }
        }
    }
}

void benchmark::thread_body(void *v)
{
    thread_arg *arg = reinterpret_cast<thread_arg *>(v);
    shared_state *shared = arg->shared;
    thread_state *thread = arg->thread;

    // add num_initialized safety, and signal the main process when num_initialized > total.
    // then wait the shared->start is set to true
    pthread_mutex_lock(&shared->mu);
    shared->num_initialized++;
    if (shared->num_initialized >= shared->total) {
        pthread_cond_broadcast(&shared->cv);
    }
    while (!shared->start) {
        pthread_cond_wait(&shared->cv, &shared->mu);
    }
    pthread_mutex_unlock(&shared->mu);

    // progress the method
    thread->stats.start(thread->tid);
    (arg->bm->*(arg->method))(thread);
    thread->stats.stop();

    // add num_done satety, and notify the main process
    pthread_mutex_lock(&shared->mu);
    shared->num_done++;
    if (shared->num_done >= shared->total) {
        pthread_cond_broadcast(&shared->cv);
    }
    pthread_mutex_unlock(&shared->mu);
}

stats benchmark::run_benchmark(int n,
                               const std::string &name,
                               void (benchmark::*method)(thread_state *),
                               std::shared_ptr<rocksdb::Statistics> hist_stats)
{
    std::unique_ptr<reporter_agent> reporter_agent_;
    if (config::get_instance()->report_interval_seconds > 0) {
        reporter_agent_.reset(new reporter_agent(config::get_instance()->env,
                                                 config::get_instance()->report_file,
                                                 config::get_instance()->report_interval_seconds));
    }

    // init thead args
    shared_state shared(n);
    thread_arg *arg = new thread_arg[n];
    for (int i = 0; i < n; i++) {
        arg[i].bm = this;
        arg[i].method = method;
        arg[i].shared = &shared;
        arg[i].thread = new thread_state(i);
        arg[i].thread->stats.set_reporter_agent(reporter_agent_.get());
        arg[i].thread->stats.set_hist_stats(hist_stats);
        config::get_instance()->env->StartThread(thread_body, &arg[i]);
    }

    // wait all of the theads' initialized
    pthread_mutex_lock(&shared.mu);
    while (shared.num_initialized < n) {
        pthread_cond_wait(&shared.cv, &shared.mu);
    }

    // wait all of the theads' done
    shared.start = true;
    pthread_cond_broadcast(&shared.cv);
    while (shared.num_done < n) {
        pthread_cond_wait(&shared.cv, &shared.mu);
    }
    pthread_mutex_unlock(&shared.mu);

    // Stats for some threads can be excluded.
    stats merge_stats;
    for (int i = 0; i < n; i++) {
        merge_stats.merge(arg[i].thread->stats);
    }
    merge_stats.report(name);

    // delete thread args
    for (int i = 0; i < n; i++) {
        delete arg[i].thread;
    }
    delete[] arg;

    return merge_stats;
}

void benchmark::write_random(thread_state *thread) { do_write(thread, RANDOM); }

void benchmark::do_write(thread_state *thread, write_mode write_mode)
{
    // generate random hash keys and sort keys
    std::vector<std::string> hashkeys, sortkeys;
    generate_random_keys(config::get_instance()->num, hashkeys, sortkeys);

    // do write for all the hash keys and sort keys
    int64_t bytes = 0;
    random_generator gen = random_generator(config::get_instance()->value_size);
    for (int i = 0; i < config::get_instance()->num; i++) {
        int try_count = 0;
        while (true) {
            try_count++;
            int ret = client->set(hashkeys[i],
                                  sortkeys[i],
                                  gen.generate(value_size_),
                                  config::get_instance()->pegasus_timeout_ms);
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
        thread->stats.finished_ops(nullptr, nullptr, 1, kWrite);
    }

    thread->stats.add_bytes(bytes);
}

int64_t benchmark::get_random_key() { return dsn::rand::next_u64() % config::get_instance()->num; }

void benchmark::read_random(thread_state *thread)
{
    // generate random hash keys and sort keys
    std::vector<std::string> hashkeys, sortkeys;
    generate_random_keys(config::get_instance()->num, hashkeys, sortkeys);

    // do read for all the hash keys and sort keys
    int64_t found = 0;
    int64_t bytes = 0;
    for (int i = 0; i < config::get_instance()->num; i++) {
        int try_count = 0;
        while (true) {
            try_count++;
            std::string value;
            int ret = client->get(
                hashkeys[i], sortkeys[i], value, config::get_instance()->pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                found++;
                bytes += hashkeys[i].size() + sortkeys[i].size() + value.size();
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
        thread->stats.finished_ops(nullptr, nullptr, 1, kRead);
    }

    char msg[100];
    snprintf(
        msg, sizeof(msg), "(%" PRIu64 " of %" PRIu32 " found)", found, config::get_instance()->num);
    thread->stats.add_bytes(bytes);
    thread->stats.add_message(msg);
}

std::string benchmark::allocate_key(std::unique_ptr<const char[]> *key_guard)
{
    char *data = new char[key_size_];
    const char *const_data = data;
    key_guard->reset(const_data);
    return std::string(key_guard->get(), key_size_);
}

void benchmark::do_delete(thread_state *thread, bool seq)
{
    // generate random hash keys and sort keys
    std::vector<std::string> hashkeys, sortkeys;
    generate_random_keys(config::get_instance()->num, hashkeys, sortkeys);

    // do delete for all the hash keys and sort keys
    for (int i = 0; i < config::get_instance()->num; i++) {
        int try_count = 0;
        while (true) {
            try_count++;
            int ret =
                client->del(hashkeys[i], sortkeys[i], config::get_instance()->pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                break;
            } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                fprintf(stderr, "Del returned an error: %s\n", client->get_error_string(ret));
                exit(1);
            } else {
                fprintf(stderr, "Get timeout, retry(%d)\n", try_count);
            }
        }
        thread->stats.finished_ops(nullptr, nullptr, 1, kDelete);
    }
}

void benchmark::delete_random(thread_state *thread) { do_delete(thread, false); }

void benchmark::print_header()
{
    fprintf(stdout, "Keys:       %d bytes each\n", config::get_instance()->key_size);
    fprintf(stdout, "Values:     %d bytes each\n", config::get_instance()->value_size);
    fprintf(stdout, "Entries:    %" PRIu64 "\n", num_);
    fprintf(stdout, "Prefix:    %d bytes\n", config::get_instance()->prefix_size);
    fprintf(stdout, "Keys per prefix:    %" PRIu64 "\n", keys_per_prefix_);
    fprintf(stdout,
            "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(config::get_instance()->key_size +
                                   config::get_instance()->value_size) *
              num_) /
             1048576.0));
    fprintf(stdout,
            "FileSize:   %.1f MB (estimated)\n",
            (((config::get_instance()->key_size + config::get_instance()->value_size) * num_) /
             1048576.0));

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

void benchmark::generate_random_keys(uint32_t num,
                                     std::vector<std::string> &hashkeys,
                                     std::vector<std::string> &sortkeys)
{
    // generate random hash keys and sort keys
    int ops = 0;
    std::unique_ptr<const char[]> hashkey_guard;
    std::unique_ptr<const char[]> sortkey_guard;
    std::string hashkey = allocate_key(&hashkey_guard);
    std::string sortkey = allocate_key(&sortkey_guard);
    while (ops++ < config::get_instance()->num) {
        generate_key_from_int(get_random_key(), num, &hashkey);
        generate_key_from_int(get_random_key(), num, &sortkey);
        hashkeys.push_back(hashkey);
        sortkeys.push_back(sortkey);
    }
}
} // namespace test
} // namespace pegasus
