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

#include <cstring>
#include <sstream>

#include "rand.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/strings.h"

namespace pegasus {
namespace test {

DSN_DEFINE_uint64(pegasus.benchmark,
                  benchmark_num,
                  10000,
                  "Number of key/values to place in database");
DSN_DEFINE_uint64(pegasus.benchmark,
                  benchmark_seed,
                  1000,
                  "Seed base for random number generators. When 0 it is deterministic");
DSN_DEFINE_string(pegasus.benchmark, pegasus_cluster_name, "onebox", "The Pegasus cluster name");
DSN_DEFINE_validator(pegasus_cluster_name,
                     [](const char *value) -> bool { return !dsn::utils::is_empty(value); });
DSN_DEFINE_string(pegasus.benchmark, pegasus_app_name, "temp", "pegasus app name");
DSN_DEFINE_validator(pegasus_app_name,
                     [](const char *value) -> bool { return !dsn::utils::is_empty(value); });
DSN_DEFINE_string(
    pegasus.benchmark,
    benchmarks,
    "fillrandom_pegasus,readrandom_pegasus,deleterandom_pegasus",
    "Comma-separated list of operations to run in the specified order. Available benchmarks:\n"
    "\tfillrandom_pegasus       -- pegasus write N values in random key order\n"
    "\treadrandom_pegasus       -- pegasus read N times in random order\n"
    "\tdeleterandom_pegasus     -- pegasus delete N keys in random order\n"
    "\tmultisetrandom_pegasus   -- pegasus write N random values with multi_count hash keys list\n"
    "\tmultigetrandom_pegasus   -- pegasus read N random keys with multi_count hash list\n");

DSN_DEFINE_validator(benchmarks,
                     [](const char *value) -> bool { return !dsn::utils::is_empty(value); });

DSN_DEFINE_int32(pegasus.benchmark,
                 pegasus_timeout_ms,
                 1000,
                 "pegasus read/write timeout in milliseconds");
DSN_DEFINE_int32(pegasus.benchmark, threads, 1, "Number of concurrent threads to run");
DSN_DEFINE_int32(pegasus.benchmark, hashkey_size, 16, "Size of each hashkey");
DSN_DEFINE_int32(pegasus.benchmark, sortkey_size, 16, "Size of each sortkey");
DSN_DEFINE_int32(pegasus.benchmark, value_size, 100, "Size of each value");
DSN_DEFINE_int32(pegasus.benchmark, multi_count, 100, "Values count of the same hashkey");

DSN_DEFINE_group_validator(multi_count, [](std::string &message) -> bool {
    std::string operation_type = FLAGS_benchmarks;
    if ((operation_type == "multisetrandom_pegasus" ||
         operation_type == "multigetrandom_pegasus") &&
        FLAGS_benchmark_num % FLAGS_multi_count != 0) {
        message = fmt::format("[pegasus.benchmark].benchmark_num {} should be a multiple of "
                              "[pegasus.benchmark].multi_count({}).",
                              FLAGS_benchmark_num,
                              FLAGS_multi_count);
        return false;
    }
    return true;
});

benchmark::benchmark()
{
    _client =
        pegasus_client_factory::get_client(FLAGS_pegasus_cluster_name, FLAGS_pegasus_app_name);
    CHECK_NOTNULL(_client, "");

    // init operation method map
    _operation_method = {{kUnknown, nullptr},
                         {kRead, &benchmark::read_random},
                         {kWrite, &benchmark::write_random},
                         {kMultiSet, &benchmark::multi_set_random},
                         {kMultiGet, &benchmark::multi_get_random},
                         {kDelete, &benchmark::delete_random}};
}

void benchmark::run()
{
    // print summarize information
    print_header();

    std::stringstream benchmark_stream(FLAGS_benchmarks);
    std::string name;
    while (std::getline(benchmark_stream, name, ',')) {
        // run the specified benchmark
        operation_type op_type = get_operation_type(name);
        run_benchmark(FLAGS_threads, op_type);
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
        args.push_back(std::make_shared<thread_arg>(
            i + (FLAGS_benchmark_seed == 0 ? 1000 : FLAGS_benchmark_seed),
            hist_stats,
            method,
            this));
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
    for (int i = 0; i < FLAGS_benchmark_num; i++) {
        // generate hash key and sort key
        std::string hashkey, sortkey, value;
        generate_kv_pair(hashkey, sortkey, value);

        // write to pegasus
        int try_count = 0;
        while (true) {
            try_count++;
            int ret = _client->set(hashkey, sortkey, value, FLAGS_pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                bytes += FLAGS_value_size + FLAGS_hashkey_size + FLAGS_sortkey_size;
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

void benchmark::multi_set_random(thread_arg *thread)
{
    uint64_t bytes = 0;

    for (int i = 0; i < FLAGS_benchmark_num / FLAGS_multi_count; i++) {
        // Generate hash key.
        std::string hashkey = generate_string(FLAGS_hashkey_size);

        // Generate sort key and value.
        std::map<std::string, std::string> kvs;
        std::string sortkey, value;
        for (int j = 0; j < FLAGS_multi_count; j++) {
            sortkey = generate_string(FLAGS_sortkey_size);
            value = generate_string(FLAGS_value_size);
            kvs.emplace(sortkey, value);
        }

        // Write to Pegasus.
        int try_count = 0;
        while (true) {
            try_count++;
            int ret = _client->multi_set(hashkey, kvs, FLAGS_pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                bytes += (FLAGS_value_size + FLAGS_hashkey_size + FLAGS_sortkey_size) *
                         FLAGS_multi_count;
                break;
            }
            if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                fmt::print(
                    stderr, "multi_set returned an error: {}\n", _client->get_error_string(ret));
                dsn_exit(1);
            }
            fmt::print(stderr, "multi_set timeout, retry({})\n", try_count);
        }

        // Count this operation.
        thread->stats.finished_ops(1, kMultiSet);
    }

    // Count total write bytes.
    thread->stats.add_bytes(bytes);
}

void benchmark::read_random(thread_arg *thread)
{
    uint64_t bytes = 0;
    uint64_t found = 0;
    for (int i = 0; i < FLAGS_benchmark_num; i++) {
        // generate hash key and sort key
        // generate value for random to keep in peace with write
        std::string hashkey, sortkey, value;
        generate_kv_pair(hashkey, sortkey, value);

        // read from pegasus
        int try_count = 0;
        while (true) {
            try_count++;
            int ret = _client->get(hashkey, sortkey, value, FLAGS_pegasus_timeout_ms);
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
    std::string msg = fmt::format("({} of {} found)", found, FLAGS_benchmark_num);
    thread->stats.add_bytes(bytes);
    thread->stats.add_message(msg);
}

void benchmark::multi_get_random(thread_arg *thread)
{
    uint64_t bytes = 0;
    uint64_t found = 0;
    int max_fetch_count = 100;
    int max_fetch_size = 1000000;

    for (int i = 0; i < FLAGS_benchmark_num / FLAGS_multi_count; i++) {
        // Generate hash key.
        std::string hashkey = generate_string(FLAGS_hashkey_size);

        // Generate sort key.
        // Generate value for random to keep in peace with write.
        std::map<std::string, std::string> kvs;
        std::set<std::string> sortkeys;
        for (int j = 0; j < FLAGS_multi_count; j++) {
            sortkeys.insert(generate_string(FLAGS_sortkey_size));
            // Make output string be sorted like multi_set_random.
            generate_string(FLAGS_value_size);
        }

        // Read from Pegasus.
        int try_count = 0;
        while (true) {
            try_count++;
            int ret = _client->multi_get(
                hashkey, sortkeys, kvs, max_fetch_count, max_fetch_size, FLAGS_pegasus_timeout_ms);
            if (ret == ::pegasus::PERR_OK) {
                found += kvs.size();
                bytes += FLAGS_multi_count * hashkey.size();
                for (const auto &kv : kvs) {
                    bytes = kv.first.size() + kv.second.size() + bytes;
                }
                break;
            }
            if (ret == ::pegasus::PERR_NOT_FOUND) {
                break;
            }
            if (ret != ::pegasus::PERR_TIMEOUT || try_count > 3) {
                fmt::print(
                    stderr, "multi_get returned an error: {}\n", _client->get_error_string(ret));
                dsn_exit(1);
            }
            fmt::print(stderr, "multi_get timeout, retry({})\n", try_count);
        }

        // Count this operation.
        thread->stats.finished_ops(1, kMultiGet);
    }

    // Count total read bytes and hit rate.
    std::string msg = fmt::format("({} of {} found)", found, FLAGS_benchmark_num);
    thread->stats.add_bytes(bytes);
    thread->stats.add_message(msg);
}

void benchmark::delete_random(thread_arg *thread)
{
    // do delete operation num times
    for (int i = 0; i < FLAGS_benchmark_num; i++) {
        // generate hash key and sort key
        // generate value for random to keep in peace with write
        std::string hashkey, sortkey, value;
        generate_kv_pair(hashkey, sortkey, value);

        int try_count = 0;
        while (true) {
            try_count++;
            int ret = _client->del(hashkey, sortkey, FLAGS_pegasus_timeout_ms);
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
    hashkey = generate_string(FLAGS_hashkey_size);
    sortkey = generate_string(FLAGS_sortkey_size);
    value = generate_string(FLAGS_value_size);
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
    } else if (name == "multisetrandom_pegasus") {
        op_type = kMultiSet;
    } else if (name == "multigetrandom_pegasus") {
        op_type = kMultiGet;
    } else if (!name.empty()) { // No error message for empty name
        fmt::print(stderr, "unknown benchmark '{}'\n", name);
        dsn_exit(1);
    }

    return op_type;
}

void benchmark::print_header()
{
    const config &config_ = config::instance();
    fmt::print(stdout, "Hashkeys:       {} bytes each\n", FLAGS_hashkey_size);
    fmt::print(stdout, "Sortkeys:       {} bytes each\n", FLAGS_sortkey_size);
    fmt::print(stdout, "Values:         {} bytes each\n", FLAGS_value_size);
    fmt::print(stdout, "Entries:        {}\n", FLAGS_benchmark_num);
    fmt::print(
        stdout,
        "FileSize:       {} MB (estimated)\n",
        ((FLAGS_hashkey_size + FLAGS_sortkey_size + FLAGS_value_size) * FLAGS_benchmark_num) >> 20);

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
