/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "benchmark.h"

#include <sstream>

#include "rand.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"

namespace pegasus {
namespace test {
benchmark::benchmark()
{
    _client = pegasus_client_factory::get_client(config::instance().pegasus_cluster_name.c_str(),
                                                 config::instance().pegasus_app_name.c_str());
    CHECK_NOTNULL(_client, "");

    // init operation method map
    _operation_method = {{kUnknown, nullptr},
                         {kRead, &benchmark::read_random},
                         {kWrite, &benchmark::write_random},
                         {kDelete, &benchmark::delete_random}};
}

void benchmark::run()
{
    // print summarize information
    print_header();

    std::stringstream benchmark_stream(config::instance().benchmarks);
    std::string name;
    while (std::getline(benchmark_stream, name, ',')) {
        // run the specified benchmark
        operation_type op_type = get_operation_type(name);
        run_benchmark(config::instance().threads, op_type);
    }
}

void benchmark::run_benchmark(int thread_count, operation_type op_type)
{
    // get method by operation type
    bench_method method = _operation_method[op_type];
    CHECK_NOTNULL(method, "");

    // create histogram statistic
    std::shared_ptr<rocksdb::Statistics> hist_stats = rocksdb::CreateDBStatistics();

    // create thread args for each thread, and run them
    std::vector<std::shared_ptr<thread_arg>> args;
    for (int i = 0; i < thread_count; i++) {
        args.push_back(
            std::make_shared<thread_arg>(i + config::instance().seed, hist_stats, method, this));
        config::instance().env->StartThread(thread_body, args[i].get());
    }

    // wait all threads are done
    config::instance().env->WaitForJoin();

    // merge statistics
    statistics merge_stats(hist_stats);
    for (int i = 0; i < thread_count; i++) {
        merge_stats.merge(args[i]->stats);
    }
    merge_stats.report(op_type);
}

void benchmark::thread_body(void *v)
{
    thread_arg *arg = static_cast<thread_arg *>(v);

    // reseed local random generator
    reseed_thread_local_rng(arg->seed);

    // progress the method
    arg->stats.start();
    (arg->bm->*(arg->method))(arg);
    arg->stats.stop();
}

void benchmark::write_random(thread_arg *thread)
{
    // do write operation num times
    uint64_t bytes = 0;
    int count = 0;
    for (int i = 0; i < config::instance().num; i++) {
        // generate hash key and sort key
        std::string hashkey, sortkey, value;
        generate_kv_pair(hashkey, sortkey, value);

        // write to pegasus
        int try_count = 0;
        while (true) {
            try_count++;
            int ret = _client->set(hashkey, sortkey, value, config::instance().pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                bytes += config::instance().value_size + config::instance().hashkey_size +
                         config::instance().sortkey_size;
                count++;
                break;
            } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                fmt::print(stderr, "Set returned an error: {}\n", _client->get_error_string(ret));
                dsn_exit(1);
            } else {
                fmt::print(stderr, "Set timeout, retry({})\n", try_count);
            }
        }

        // count this operation
        thread->stats.finished_ops(1, kWrite);
    }

    // count total write bytes
    thread->stats.add_bytes(bytes);
}

void benchmark::read_random(thread_arg *thread)
{
    uint64_t bytes = 0;
    uint64_t found = 0;
    for (int i = 0; i < config::instance().num; i++) {
        // generate hash key and sort key
        // generate value for random to keep in peace with write
        std::string hashkey, sortkey, value;
        generate_kv_pair(hashkey, sortkey, value);

        // read from pegasus
        int try_count = 0;
        while (true) {
            try_count++;
            int ret = _client->get(hashkey, sortkey, value, config::instance().pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                found++;
                bytes += hashkey.size() + sortkey.size() + value.size();
                break;
            } else if (ret == ::pegasus::PERR_NOT_FOUND) {
                break;
            } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                fmt::print(stderr, "Get returned an error: {}\n", _client->get_error_string(ret));
                dsn_exit(1);
            } else {
                fmt::print(stderr, "Get timeout, retry({})\n", try_count);
            }
        }

        // count this operation
        thread->stats.finished_ops(1, kRead);
    }

    // count total read bytes and hit rate
    std::string msg = fmt::format("({} of {} found)", found, config::instance().num);
    thread->stats.add_bytes(bytes);
    thread->stats.add_message(msg);
}

void benchmark::delete_random(thread_arg *thread)
{
    // do delete operation num times
    for (int i = 0; i < config::instance().num; i++) {
        // generate hash key and sort key
        // generate value for random to keep in peace with write
        std::string hashkey, sortkey, value;
        generate_kv_pair(hashkey, sortkey, value);

        int try_count = 0;
        while (true) {
            try_count++;
            int ret = _client->del(hashkey, sortkey, config::instance().pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                break;
            } else if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                fmt::print(stderr, "Del returned an error: {}\n", _client->get_error_string(ret));
                dsn_exit(1);
            } else {
                fmt::print(stderr, "Get timeout, retry({})\n", try_count);
            }
        }

        // count this operation
        thread->stats.finished_ops(1, kDelete);
    }
}

void benchmark::generate_kv_pair(std::string &hashkey, std::string &sortkey, std::string &value)
{
    hashkey = generate_string(config::instance().hashkey_size);
    sortkey = generate_string(config::instance().sortkey_size);
    value = generate_string(config::instance().value_size);
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
        fmt::print(stderr, "unknown benchmark '{}'\n", name);
        dsn_exit(1);
    }

    return op_type;
}

void benchmark::print_header()
{
    const config &config_ = config::instance();
    fmt::print(stdout, "Hashkeys:       {} bytes each\n", config_.hashkey_size);
    fmt::print(stdout, "Sortkeys:       {} bytes each\n", config_.sortkey_size);
    fmt::print(stdout, "Values:         {} bytes each\n", config_.value_size);
    fmt::print(stdout, "Entries:        {}\n", config_.num);
    fmt::print(stdout,
               "FileSize:       {} MB (estimated)\n",
               ((config_.hashkey_size + config_.sortkey_size + config_.value_size) * config_.num) >>
                   20);

    print_warnings();
    fmt::print(stdout, "------------------------------------------------\n");
}

void benchmark::print_warnings()
{
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fmt::print(stdout, "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
    fmt::print(stdout, "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
}
} // namespace test
} // namespace pegasus
