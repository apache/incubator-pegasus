// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <sstream>
#include <dsn/utility/rand.h>
#include <tgmath.h>
#include <pegasus/client.h>
#include "benchmark.h"
#include "random_generator.h"
#include "utils.h"

namespace pegasus {
namespace test {
static std::unordered_map<operation_type, std::string, std::hash<unsigned char>>
    operation_type_string = {
        {kUnknown, "unKnown"}, {kRead, "read"}, {kWrite, "write"}, {kDelete, "delete"}};

benchmark::benchmark()
{
    _client =
        pegasus_client_factory::get_client(config::get_instance()->pegasus_cluster_name.c_str(),
                                           config::get_instance()->pegasus_app_name.c_str());
    if (_client == nullptr) {
        fprintf(stderr, "create client error\n");
        exit(1);
    }

    _operation_method = {{kUnknown, nullptr},
                         {kRead, &benchmark::read_random},
                         {kWrite, &benchmark::write_random},
                         {kDelete, &benchmark::delete_random}};
}

// Generate key according to the given specification and random number.
// The resulting key will have the following format (if _keys_per_prefix
// is positive), extra trailing bytes are either cut off or padded with '0'.
// The prefix value is derived from key value.
//   ----------------------------
//   | prefix 00000 | key 00000 |
//   ----------------------------
// If _keys_per_prefix is 0, the key is simply a binary representation of
// random number followed by trailing '0's
//   ----------------------------
//   |        key 00000         |
//   ----------------------------
void benchmark::generate_key_from_int(uint64_t v, std::string *key, int key_size)
{
    char *start = const_cast<char *>(key->data());
    char *pos = start;

    // copy v to the address of pos
    int bytes_to_fill = std::min(key_size, 8);
    memcpy(pos, static_cast<void *>(&v), bytes_to_fill);

    // fill '0' with the remain space
    pos += bytes_to_fill;
    if (key_size > pos - start) {
        memset(pos, '0', key_size - (pos - start));
    }
}

void benchmark::generate_hashkey_from_int(uint64_t v, std::string *key)
{
    generate_key_from_int(v, key, config::get_instance()->hashkey_size);
}

void benchmark::generate_sortkey_from_int(uint64_t v, std::string *key)
{
    generate_key_from_int(v, key, config::get_instance()->sortkey_size);
}

void benchmark::run()
{
    print_header();
    std::stringstream benchmark_stream(config::get_instance()->benchmarks);
    std::string name;
    while (std::getline(benchmark_stream, name, ',')) {
        // get operation type
        operation_type op_type = get_operation_type(name);

        // get method by operation type
        bench_method method = _operation_method[op_type];
        assert(method != nullptr);

        // run the specified benchmark
        std::shared_ptr<rocksdb::Statistics> hist_stats = rocksdb::CreateDBStatistics();
        stats stats_ = run_benchmark(config::get_instance()->threads, name, method, hist_stats);

        // print report
        stats_.report(name);
        fprintf(stdout,
                "Microseconds per %s:\n%s\n",
                operation_type_string[op_type].c_str(),
                hist_stats->getHistogramString(op_type).c_str());
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
                               bench_method method,
                               std::shared_ptr<rocksdb::Statistics> hist_stats)
{
    // init thead args
    shared_state shared(n);
    thread_arg *arg = new thread_arg[n];
    for (int i = 0; i < n; i++) {
        arg[i].bm = this;
        arg[i].method = method;
        arg[i].shared = &shared;
        arg[i].thread = new thread_state(i);
        arg[i].thread->stats.set_hist_stats(hist_stats);
        config::get_instance()->env->StartThread(thread_body, &arg[i]);
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

void benchmark::write_random(thread_state *thread)
{
    // generate random hash keys and sort keys
    std::vector<std::string> hashkeys, sortkeys;
    generate_random_keys(hashkeys, sortkeys);

    // generate random values
    std::vector<std::string> values;
    generate_random_values(values);

    // write to pegasus
    do_write(thread, hashkeys, sortkeys, values);
}

void benchmark::do_write(thread_state *thread,
                         const std::vector<std::string> &hashkeys,
                         const std::vector<std::string> &sortkeys,
                         const std::vector<std::string> &values)
{
    // do write for all the hash keys and sort keys
    int64_t bytes = 0;
    for (int i = 0; i < config::get_instance()->num; i++) {
        int try_count = 0;
        while (true) {
            try_count++;
            int ret = _client->set(
                hashkeys[i], sortkeys[i], values[i], config::get_instance()->pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                bytes += config::get_instance()->value_size + config::get_instance()->hashkey_size +
                         config::get_instance()->sortkey_size;
                break;
            } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                fprintf(stderr, "Set returned an error: %s\n", _client->get_error_string(ret));
                exit(1);
            } else {
                fprintf(stderr, "Set timeout, retry(%d)\n", try_count);
            }
        }
        thread->stats.finished_ops(1, kWrite);
    }

    thread->stats.add_bytes(bytes);
}

void benchmark::read_random(thread_state *thread)
{
    // generate random hash keys and sort keys
    std::vector<std::string> hashkeys, sortkeys;
    generate_random_keys(hashkeys, sortkeys);

    // first write, in order to improve read hit ratio
    std::vector<std::string> values;
    generate_random_values(values);
    do_write(thread, hashkeys, sortkeys, values);

    // do read for all the hash keys and sort keys
    int64_t found = 0;
    int64_t bytes = 0;
    for (int i = 0; i < config::get_instance()->num; i++) {
        int try_count = 0;
        while (true) {
            try_count++;
            std::string value;
            int ret = _client->get(
                hashkeys[i], sortkeys[i], value, config::get_instance()->pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                found++;
                bytes += hashkeys[i].size() + sortkeys[i].size() + value.size();
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
        thread->stats.finished_ops(1, kRead);
    }

    char msg[100];
    snprintf(
        msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)", found, config::get_instance()->num);
    thread->stats.add_bytes(bytes);
    thread->stats.add_message(msg);
}

void benchmark::delete_random(thread_state *thread)
{
    // generate random hash keys and sort keys
    std::vector<std::string> hashkeys, sortkeys;
    generate_random_keys(hashkeys, sortkeys);

    // do delete for all the hash keys and sort keys
    for (int i = 0; i < config::get_instance()->num; i++) {
        int try_count = 0;
        while (true) {
            try_count++;
            int ret =
                _client->del(hashkeys[i], sortkeys[i], config::get_instance()->pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                break;
            } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                fprintf(stderr, "Del returned an error: %s\n", _client->get_error_string(ret));
                exit(1);
            } else {
                fprintf(stderr, "Get timeout, retry(%d)\n", try_count);
            }
        }
        thread->stats.finished_ops(1, kDelete);
    }
}

int64_t benchmark::get_random_num() { return dsn::rand::next_u64() % config::get_instance()->num; }

std::string benchmark::allocate_key(int key_size)
{
    return std::string(new char[key_size], key_size);
}

std::string benchmark::allocate_hashkey()
{
    return allocate_key(config::get_instance()->hashkey_size);
}

std::string benchmark::allocate_sortkey()
{
    return allocate_key(config::get_instance()->hashkey_size);
}

void benchmark::generate_random_values(std::vector<std::string> &values)
{
    random_generator gen = random_generator(config::get_instance()->value_size);
    for (int i = 0; i < config::get_instance()->num; i++) {
        values.push_back(gen.generate(config::get_instance()->value_size));
    }
}

void benchmark::generate_random_keys(std::vector<std::string> &hashkeys,
                                     std::vector<std::string> &sortkeys)
{
    // generate random hash keys and sort keys
    std::string hashkey = allocate_hashkey();
    std::string sortkey = allocate_sortkey();
    for (int i = 0; i < config::get_instance()->num; i++) {
        generate_hashkey_from_int(get_random_num(), &hashkey);
        generate_sortkey_from_int(get_random_num(), &sortkey);
        hashkeys.push_back(hashkey);
        sortkeys.push_back(sortkey);
    }
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
    config *config_ = config::get_instance();
    fprintf(stdout, "Hashkeys:       %d bytes each\n", config_->hashkey_size);
    fprintf(stdout, "Sortkeys:       %d bytes each\n", config_->sortkey_size);
    fprintf(stdout, "Values:     %d bytes each\n", config_->value_size);
    fprintf(stdout, "Entries:    %" PRIu64 "\n", config_->num);
    fprintf(stdout,
            "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(config_->hashkey_size + config_->sortkey_size +
                                   config_->value_size) *
              config_->num) /
             1048576.0));
    fprintf(stdout,
            "FileSize:   %.1f MB (estimated)\n",
            (((config::get_instance()->hashkey_size + config_->sortkey_size + config_->value_size) *
              config_->num) /
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
