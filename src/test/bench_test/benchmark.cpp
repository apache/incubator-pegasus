// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <sstream>
#include <pegasus/client.h>
#include <cinttypes>

#include "benchmark.h"
#include "random_generator.h"
#include "utils.h"

namespace pegasus {
namespace test {
benchmark::benchmark() : _random_generator(random_generator::get_instance())
{
    _client =
        pegasus_client_factory::get_client(config::get_instance().pegasus_cluster_name.c_str(),
                                           config::get_instance().pegasus_app_name.c_str());
    assert(nullptr != _client);

    // init operation method map
    _operation_method = {{kUnknown, nullptr},
                         {kRead, &benchmark::read_random},
                         {kWrite, &benchmark::write_random},
                         {kDelete, &benchmark::delete_random}};
}

benchmark::~benchmark()
{
    _client = nullptr;
    _operation_method.clear();
}

void benchmark::run()
{
    // print summarize information
    print_header();

    std::stringstream benchmark_stream(config::get_instance().benchmarks);
    std::string name;
    while (std::getline(benchmark_stream, name, ',')) {
        // get operation type
        operation_type op_type = get_operation_type(name);

        // run the specified benchmark
        run_benchmark(config::get_instance().threads, op_type);
    }
}

void benchmark::run_benchmark(int n, operation_type op_type)
{
    // get method by operation type
    bench_method method = _operation_method[op_type];
    assert(method != nullptr);

    // create histogram statistic
    std::shared_ptr<rocksdb::Statistics> hist_stats = rocksdb::CreateDBStatistics();

    // init thead args
    shared_state shared(n);
    thread_arg *arg = new thread_arg[n];
    for (int i = 0; i < n; i++) {
        arg[i].bm = this;
        arg[i].method = method;
        arg[i].shared = &shared;
        arg[i].thread = new thread_state(i);
        arg[i].thread->stats.set_hist_stats(hist_stats);
        config::get_instance().env->StartThread(thread_body, &arg[i]);
    }

    // wait all of the theads' initialized
    pthread_mutex_lock(&shared.mu);
    while (shared.num_initialized < n) {
        pthread_cond_wait(&shared.cv, &shared.mu);
    }

    // wait all of the theads done
    shared.start = true;
    pthread_cond_broadcast(&shared.cv);
    while (shared.num_done < n) {
        pthread_cond_wait(&shared.cv, &shared.mu);
    }
    pthread_mutex_unlock(&shared.mu);

    // merge stats
    stats merge_stats(hist_stats);
    for (int i = 0; i < n; i++) {
        merge_stats.merge(arg[i].thread->stats);
    }
    merge_stats.report(op_type);

    // delete thread args
    for (int i = 0; i < n; i++) {
        delete arg[i].thread;
    }
    delete[] arg;
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

void benchmark::write_random(thread_state *thread)
{
    uint64_t bytes = 0;

    // do write operation num times
    for (int i = 0; i < config::get_instance().num; i++) {
        // generate random key and value
        std::string hashkey = generate_hashkey();
        std::string sortkey = generate_sortkey();
        std::string value = generate_value();

        // write to pegasus
        int try_count = 0;
        while (true) {
            try_count++;
            int ret =
                _client->set(hashkey, sortkey, value, config::get_instance().pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                bytes += config::get_instance().value_size + config::get_instance().hashkey_size +
                         config::get_instance().sortkey_size;
                break;
            } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                fprintf(stderr, "Set returned an error: %s\n", _client->get_error_string(ret));
                exit(1);
            } else {
                fprintf(stderr, "Set timeout, retry(%d)\n", try_count);
            }
        }

        // mark finished this operations
        thread->stats.finished_ops(1, kWrite);
    }

    // statistical total bytes
    thread->stats.add_bytes(bytes);
}

void benchmark::read_random(thread_state *thread)
{
    // to improve hit rate, write first. By using same random seed
    uint32_t seed = _random_generator.next();
    _random_generator.reseed(seed);
    write_random(thread);

    // reseed, to ensure same seed with random write above
    _random_generator.reseed(seed);

    // reset start time
    thread->stats.start(thread->tid);

    uint64_t bytes = 0;
    uint64_t found = 0;
    for (int i = 0; i < config::get_instance().num; i++) {
        // generate random hash key and sort key
        std::string hashkey = generate_hashkey();
        std::string sortkey = generate_sortkey();

        // to keep pace with write random,
        // in order to generate same random sequence with random write
        generate_value();

        // read from pegasus
        int try_count = 0;
        while (true) {
            try_count++;
            std::string value;
            int ret =
                _client->get(hashkey, sortkey, value, config::get_instance().pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                found++;
                bytes += hashkey.size() + sortkey.size() + value.size();
                break;
            } else if (ret == ::pegasus::PERR_NOT_FOUND) {
                break;
            } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                fprintf(stderr, "Get returned an error: %s\n", _client->get_error_string(ret));
                exit(1);
            } else {
                fprintf(stderr, "Get timeout, retry(%d)\n", try_count);
            }
        }

        // mark finished this operations
        thread->stats.finished_ops(1, kRead);
    }

    // statistical bytes and message
    char msg[100];
    snprintf(
        msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)", found, config::get_instance().num);
    thread->stats.add_bytes(bytes);
    thread->stats.add_message(msg);
}

void benchmark::delete_random(thread_state *thread)
{
    // do delete operation num times
    for (int i = 0; i < config::get_instance().num; i++) {
        // generate hash key and sort key
        std::string hashkey = generate_hashkey();
        std::string sortkey = generate_sortkey();

        // write to pegasus
        int try_count = 0;
        while (true) {
            try_count++;
            int ret = _client->del(hashkey, sortkey, config::get_instance().pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                break;
            } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                fprintf(stderr, "Del returned an error: %s\n", _client->get_error_string(ret));
                exit(1);
            } else {
                fprintf(stderr, "Get timeout, retry(%d)\n", try_count);
            }
        }

        // statistics this operation
        thread->stats.finished_ops(1, kDelete);
    }
}

std::string benchmark::generate_hashkey()
{
    return generate_string(config::get_instance().hashkey_size);
}

std::string benchmark::generate_sortkey()
{
    return generate_string(config::get_instance().sortkey_size);
}

std::string benchmark::generate_value()
{
    return generate_string(config::get_instance().value_size);
}

std::string benchmark::generate_string(int len)
{
    std::string key;

    // fill with random int
    int random_int = _random_generator.uniform(config::get_instance().num);
    key.append(reinterpret_cast<char *>(&random_int), std::min(len, 8));

    // append with '0'
    key.resize(len, '0');

    return key;
}

operation_type benchmark::get_operation_type(const std::string &name)
{
    operation_type op_type = kUnknown;
    if (name == "fillrandom_pegasus") {
        op_type = kWrite;
    } else if (name == "readrandom_pegasus") {
        op_type = kRead;
    } else if (name == "deleterandom_pegasus") {
        op_type = kDelete;
    } else if (!name.empty()) { // No error message for empty name
        fprintf(stderr, "unknown benchmark '%s'\n", name.c_str());
        exit(1);
    }

    return op_type;
}

void benchmark::print_header()
{
    const config &config_ = config::get_instance();
    fprintf(stdout, "Hashkeys:       %d bytes each\n", config_.hashkey_size);
    fprintf(stdout, "Sortkeys:       %d bytes each\n", config_.sortkey_size);
    fprintf(stdout, "Values:     %d bytes each\n", config_.value_size);
    fprintf(stdout, "Entries:    %" PRIu64 "\n", config_.num);
    fprintf(
        stdout,
        "RawSize:    %.1f MB (estimated)\n",
        ((static_cast<int64_t>(config_.hashkey_size + config_.sortkey_size + config_.value_size) *
          config_.num) /
         1048576.0));
    fprintf(stdout,
            "FileSize:   %.1f MB (estimated)\n",
            (((config_.hashkey_size + config_.sortkey_size + config_.value_size) * config_.num) /
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
} // namespace test
} // namespace pegasus
