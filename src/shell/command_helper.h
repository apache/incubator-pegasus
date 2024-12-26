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

#pragma once

#include <getopt.h>
#include <fstream>
#include <functional>
#include <iomanip>
#include <memory>
#include <queue>
#include <thread>
#include <utility>

#include <boost/algorithm/string.hpp>
#include <fmt/color.h>
#include <fmt/ostream.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/sst_dump_tool.h>
#include <rocksdb/statistics.h>

#include <geo/lib/geo_client.h>
#include <pegasus/error.h>
#include <pegasus/git_commit.h>
#include <pegasus/version.h>
#include <rrdb/rrdb.code.definition.h>
#include <rrdb/rrdb_types.h>

#include "base/pegasus_key_schema.h"
#include "base/pegasus_utils.h"
#include "base/pegasus_value_schema.h"
#include "client/replication_ddl_client.h"
#include "command_executor.h"
#include "command_utils.h"
#include "common/json_helper.h"
#include "http/http_client.h"
#include "perf_counter/perf_counter_utils.h"
#include "remote_cmd/remote_command.h"
#include "task/async_calls.h"
#include "tools/mutation_log_tool.h"
#include "utils/fmt_utils.h"
#include <string_view>
#include "utils/errors.h"
#include "utils/metrics.h"
#include "utils/ports.h"
#include "utils/string_conv.h"
#include "utils/strings.h"
#include "utils/synchronize.h"
#include "utils/time_utils.h"

#define SHELL_PRINTLN_ERROR(msg, ...)                                                              \
    fmt::print(stderr,                                                                             \
               fmt::emphasis::bold | fmt::fg(fmt::color::red),                                     \
               "ERROR: {}\n",                                                                      \
               fmt::format(msg, ##__VA_ARGS__))

#define SHELL_PRINT_WARNING_BASE(msg, ...)                                                         \
    fmt::print(stdout,                                                                             \
               fmt::emphasis::bold | fmt::fg(fmt::color::yellow),                                  \
               "WARNING: {}",                                                                      \
               fmt::format(msg, ##__VA_ARGS__))

#define SHELL_PRINT_WARNING(msg, ...) SHELL_PRINT_WARNING_BASE(msg, ##__VA_ARGS__)

#define SHELL_PRINTLN_WARNING(msg, ...)                                                            \
    SHELL_PRINT_WARNING_BASE("{}\n", fmt::format(msg, ##__VA_ARGS__))

#define SHELL_PRINT_OK_BASE(msg, ...)                                                              \
    fmt::print(stdout, fmt::emphasis::bold | fmt::fg(fmt::color::green), msg, ##__VA_ARGS__)

#define SHELL_PRINT_OK(msg, ...) SHELL_PRINT_OK_BASE(msg, ##__VA_ARGS__)

#define SHELL_PRINTLN_OK(msg, ...) SHELL_PRINT_OK_BASE("{}\n", fmt::format(msg, ##__VA_ARGS__))

// Print messages to stderr and return false if `exp` is evaluated to false.
#define SHELL_PRINT_AND_RETURN_FALSE_IF_NOT(exp, ...)                                              \
    do {                                                                                           \
        if (dsn_unlikely(!(exp))) {                                                                \
            SHELL_PRINTLN_ERROR(__VA_ARGS__);                                                      \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

#define RETURN_FALSE_IF_SAMPLE_INTERVAL_MS_INVALID()                                               \
    SHELL_PRINT_AND_RETURN_FALSE_IF_NOT(dsn::buf2uint32(optarg, sample_interval_ms),               \
                                        "parse sample_interval_ms({}) failed",                     \
                                        optarg);                                                   \
    SHELL_PRINT_AND_RETURN_FALSE_IF_NOT(sample_interval_ms > 0, "sample_interval_ms should be > 0")

DEFINE_TASK_CODE(LPC_SCAN_DATA, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE(LPC_GET_METRICS, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

enum scan_data_operator
{
    SCAN_COPY,
    SCAN_CLEAR,
    SCAN_COUNT,
    SCAN_GEN_GEO,
    SCAN_AND_MULTI_SET
};
USER_DEFINED_ENUM_FORMATTER(scan_data_operator)

class top_container
{
public:
    struct top_heap_item
    {
        std::string hash_key;
        std::string sort_key;
        long row_size;
        top_heap_item(std::string &&hash_key_, std::string &&sort_key_, long row_size_)
            : hash_key(std::move(hash_key_)), sort_key(std::move(sort_key_)), row_size(row_size_)
        {
        }
    };
    struct top_heap_compare
    {
        bool operator()(top_heap_item i1, top_heap_item i2) { return i1.row_size < i2.row_size; }
    };
    typedef std::priority_queue<top_heap_item, std::vector<top_heap_item>, top_heap_compare>
        top_heap;

    top_container(int count) : _count(count) {}

    void push(std::string &&hash_key, std::string &&sort_key, long row_size)
    {
        dsn::utils::auto_lock<dsn::utils::ex_lock_nr> l(_lock);
        if (_heap.size() < _count) {
            _heap.emplace(std::move(hash_key), std::move(sort_key), row_size);
        } else {
            const top_heap_item &top = _heap.top();
            if (top.row_size < row_size) {
                _heap.pop();
                _heap.emplace(std::move(hash_key), std::move(sort_key), row_size);
            }
        }
    }

    top_heap &all() { return _heap; }

private:
    int _count;
    top_heap _heap;
    dsn::utils::ex_lock_nr _lock;
};

enum class histogram_type
{
    HASH_KEY_SIZE,
    SORT_KEY_SIZE,
    VALUE_SIZE,
    ROW_SIZE
};

struct scan_data_context
{
    scan_data_operator op;
    int split_id;
    int max_batch_count;
    int timeout_ms;
    bool no_overwrite; // if set true, then use check_and_set() instead of set()
                       // when inserting data to destination table for copy_data,
                       // to not overwrite old data if it aleady exist.
    pegasus::pegasus_client::filter_type sort_key_filter_type;
    std::string sort_key_filter_pattern;
    pegasus::pegasus_client::filter_type value_filter_type;
    std::string value_filter_pattern;
    pegasus::pegasus_client::pegasus_scanner_wrapper scanner;
    pegasus::pegasus_client *client;
    pegasus::geo::geo_client *geoclient;
    std::atomic_bool *error_occurred;
    std::atomic_long split_rows;
    std::atomic_long split_request_count;
    std::atomic_bool split_completed;
    bool stat_size;
    std::shared_ptr<rocksdb::Statistics> statistics;
    int top_count;
    top_container top_rows;
    bool count_hash_key;
    std::string last_hash_key;
    std::atomic_long split_hash_key_count;

    long data_count;
    uint32_t multi_ttl_seconds;
    std::unordered_map<std::string, std::map<std::string, std::string>> multi_kvs;
    dsn::utils::semaphore sema;

    scan_data_context(scan_data_operator op_,
                      int split_id_,
                      int max_batch_count_,
                      int timeout_ms_,
                      pegasus::pegasus_client::pegasus_scanner_wrapper scanner_,
                      pegasus::pegasus_client *client_,
                      pegasus::geo::geo_client *geoclient_,
                      std::atomic_bool *error_occurred_,
                      int max_multi_set_concurrency = 20,
                      bool stat_size_ = false,
                      std::shared_ptr<rocksdb::Statistics> statistics_ = nullptr,
                      int top_count_ = 0,
                      bool count_hash_key_ = false)
        : op(op_),
          split_id(split_id_),
          max_batch_count(max_batch_count_),
          timeout_ms(timeout_ms_),
          no_overwrite(false),
          sort_key_filter_type(pegasus::pegasus_client::FT_NO_FILTER),
          value_filter_type(pegasus::pegasus_client::FT_NO_FILTER),
          scanner(scanner_),
          client(client_),
          geoclient(geoclient_),
          error_occurred(error_occurred_),
          split_rows(0),
          split_request_count(0),
          split_completed(false),
          stat_size(stat_size_),
          statistics(statistics_),
          top_count(top_count_),
          top_rows(top_count_),
          count_hash_key(count_hash_key_),
          split_hash_key_count(0),
          data_count(0),
          multi_ttl_seconds(0),
          sema(max_multi_set_concurrency)
    {
        // max_batch_count should > 1 because scan may be terminated
        // when split_request_count = 1
        CHECK_GT(max_batch_count, 1);
    }
    void set_sort_key_filter(pegasus::pegasus_client::filter_type type, const std::string &pattern)
    {
        sort_key_filter_type = type;
        sort_key_filter_pattern = pattern;
    }
    void set_value_filter(pegasus::pegasus_client::filter_type type, const std::string &pattern)
    {
        value_filter_type = type;
        value_filter_pattern = pattern;
    }
    void set_no_overwrite() { no_overwrite = true; }
};
inline void update_atomic_max(std::atomic_long &max, long value)
{
    while (true) {
        long old = max.load();
        if (value <= old || max.compare_exchange_weak(old, value)) {
            break;
        }
    }
}
inline pegasus::pegasus_client::filter_type parse_filter_type(const std::string &name,
                                                              bool include_exact)
{
    if (include_exact && name == "exact")
        return pegasus::pegasus_client::FT_MATCH_EXACT;
    else
        return (pegasus::pegasus_client::filter_type)type_from_string(
            dsn::apps::_filter_type_VALUES_TO_NAMES,
            std::string("ft_match_") + name,
            ::dsn::apps::filter_type::FT_NO_FILTER);
}
// return true if the data is valid for the filter
inline bool validate_filter(pegasus::pegasus_client::filter_type filter_type,
                            const std::string &filter_pattern,
                            const std::string &value)
{
    switch (filter_type) {
    case pegasus::pegasus_client::FT_NO_FILTER:
        return true;
    case pegasus::pegasus_client::FT_MATCH_EXACT:
        return filter_pattern == value;
    case pegasus::pegasus_client::FT_MATCH_ANYWHERE:
    case pegasus::pegasus_client::FT_MATCH_PREFIX:
    case pegasus::pegasus_client::FT_MATCH_POSTFIX: {
        if (filter_pattern.length() == 0)
            return true;
        if (value.length() < filter_pattern.length())
            return false;
        if (filter_type == pegasus::pegasus_client::FT_MATCH_ANYWHERE) {
            return std::string_view(value).find(filter_pattern) != std::string_view::npos;
        } else if (filter_type == pegasus::pegasus_client::FT_MATCH_PREFIX) {
            return dsn::utils::mequals(
                value.data(), filter_pattern.data(), filter_pattern.length());
        } else { // filter_type == pegasus::pegasus_client::FT_MATCH_POSTFIX
            return dsn::utils::mequals(value.data() + value.length() - filter_pattern.length(),
                                       filter_pattern.data(),
                                       filter_pattern.length());
        }
    }
    default:
        LOG_FATAL("unsupported filter type: {}", filter_type);
    }
    return false;
}
// return true if the data is valid for the filter
inline bool
validate_filter(scan_data_context *context, const std::string &sort_key, const std::string &value)
{
    // for sort key, we only need to check MATCH_EXACT, because it is not supported
    // on the server side, but MATCH_PREFIX is already satisified.
    if (context->sort_key_filter_type == pegasus::pegasus_client::FT_MATCH_EXACT &&
        sort_key.length() > context->sort_key_filter_pattern.length())
        return false;
    return validate_filter(context->value_filter_type, context->value_filter_pattern, value);
}

inline int compute_ttl_seconds(uint32_t expire_ts_seconds, bool &ts_expired)
{
    auto epoch_now = pegasus::utils::epoch_now();
    ts_expired = pegasus::check_if_ts_expired(epoch_now, expire_ts_seconds);
    if (expire_ts_seconds > 0 && !ts_expired) {
        return static_cast<int>(expire_ts_seconds - epoch_now);
    }
    return 0;
}

inline void batch_execute_multi_set(scan_data_context *context)
{
    for (const auto &kv : context->multi_kvs) {
        // wait for satisfied with max_multi_set_concurrency
        context->sema.wait();
        int multi_size = kv.second.size();
        context->client->async_multi_set(
            kv.first,
            kv.second,
            [context, multi_size](int err, pegasus::pegasus_client::internal_info &&info) {
                if (err != pegasus::PERR_OK) {
                    if (!context->split_completed.exchange(true)) {
                        fprintf(stderr,
                                "ERROR: split[%d] async_multi_set set failed: %s\n",
                                context->split_id,
                                context->client->get_error_string(err));
                        context->error_occurred->store(true);
                    }
                } else {
                    context->split_rows += multi_size;
                }
                context->sema.signal();
            },
            context->timeout_ms,
            context->multi_ttl_seconds);
    }
    context->multi_kvs.clear();
    context->data_count = 0;
}
// copy data by async_multi_set
inline void scan_multi_data_next(scan_data_context *context)
{
    if (!context->split_completed.load() && !context->error_occurred->load()) {
        context->scanner->async_next([context](int ret,
                                               std::string &&hash_key,
                                               std::string &&sort_key,
                                               std::string &&value,
                                               pegasus::pegasus_client::internal_info &&info,
                                               uint32_t expire_ts_seconds,
                                               uint32_t kv_count) {
            if (ret == pegasus::PERR_OK) {
                if (validate_filter(context, sort_key, value)) {
                    bool ts_expired = false;
                    int ttl_seconds = 0;
                    ttl_seconds = compute_ttl_seconds(expire_ts_seconds, ts_expired);
                    if (!ts_expired) {
                        // empty hashkey should get hashkey by sortkey
                        if (hash_key == "") {
                            // wait for satisfied with max_multi_set_concurrency
                            context->sema.wait();

                            auto callback = [context](
                                                int err,
                                                pegasus::pegasus_client::internal_info &&info) {
                                if (err != pegasus::PERR_OK) {
                                    if (!context->split_completed.exchange(true)) {
                                        fprintf(stderr,
                                                "ERROR: split[%d] async check and set failed: %s\n",
                                                context->split_id,
                                                context->client->get_error_string(err));
                                        context->error_occurred->store(true);
                                    }
                                } else {
                                    context->split_rows++;
                                }
                                context->sema.signal();
                            };

                            context->client->async_set(hash_key,
                                                       sort_key,
                                                       value,
                                                       std::move(callback),
                                                       context->timeout_ms,
                                                       ttl_seconds);
                        } else {
                            context->data_count++;
                            if (context->multi_kvs.find(hash_key) == context->multi_kvs.end()) {
                                context->multi_kvs.emplace(hash_key,
                                                           std::map<std::string, std::string>());
                            }
                            if (context->multi_ttl_seconds < ttl_seconds || ttl_seconds == 0) {
                                context->multi_ttl_seconds = ttl_seconds;
                            }
                            context->multi_kvs[hash_key].emplace(std::move(sort_key),
                                                                 std::move(value));

                            if (context->data_count >= context->max_batch_count) {
                                batch_execute_multi_set(context);
                            }
                        }
                    }
                }
                scan_multi_data_next(context);
            } else if (ret == pegasus::PERR_SCAN_COMPLETE) {
                batch_execute_multi_set(context);
                context->split_completed.store(true);
            } else {
                if (!context->split_completed.exchange(true)) {
                    fprintf(stderr,
                            "ERROR: split[%d] scan next failed: %s\n",
                            context->split_id,
                            context->client->get_error_string(ret));
                    context->error_occurred->store(true);
                }
            }
        });
    }
}

inline void scan_data_next(scan_data_context *context)
{
    while (!context->split_completed.load() && !context->error_occurred->load() &&
           context->split_request_count.load() < context->max_batch_count) {
        context->split_request_count++;
        context->scanner->async_next([context](int ret,
                                               std::string &&hash_key,
                                               std::string &&sort_key,
                                               std::string &&value,
                                               pegasus::pegasus_client::internal_info &&info,
                                               uint32_t expire_ts_seconds,
                                               int32_t kv_count) {
            if (ret == pegasus::PERR_OK) {
                if (kv_count != -1 || validate_filter(context, sort_key, value)) {
                    bool ts_expired = false;
                    int ttl_seconds = 0;
                    switch (context->op) {
                    case SCAN_COPY:
                        context->split_request_count++;
                        ttl_seconds = compute_ttl_seconds(expire_ts_seconds, ts_expired);
                        if (ts_expired) {
                            scan_data_next(context);
                        } else if (context->no_overwrite) {
                            auto callback =
                                [context](int err,
                                          pegasus::pegasus_client::check_and_set_results &&results,
                                          pegasus::pegasus_client::internal_info &&info) {
                                    if (err != pegasus::PERR_OK) {
                                        if (!context->split_completed.exchange(true)) {
                                            fprintf(
                                                stderr,
                                                "ERROR: split[%d] async check and set failed: %s\n",
                                                context->split_id,
                                                context->client->get_error_string(err));
                                            context->error_occurred->store(true);
                                        }
                                    } else {
                                        if (results.set_succeed) {
                                            context->split_rows++;
                                        }
                                        scan_data_next(context);
                                    }
                                    // should put "split_request_count--" at end of the scope,
                                    // to prevent that split_request_count becomes 0 in the middle.
                                    context->split_request_count--;
                                };
                            pegasus::pegasus_client::check_and_set_options options;
                            options.set_value_ttl_seconds = ttl_seconds;
                            context->client->async_check_and_set(
                                hash_key,
                                sort_key,
                                pegasus::pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                "",
                                sort_key,
                                value,
                                options,
                                std::move(callback),
                                context->timeout_ms);
                        } else {
                            auto callback =
                                [context](int err, pegasus::pegasus_client::internal_info &&info) {
                                    if (err != pegasus::PERR_OK) {
                                        if (!context->split_completed.exchange(true)) {
                                            fprintf(stderr,
                                                    "ERROR: split[%d] async set failed: %s\n",
                                                    context->split_id,
                                                    context->client->get_error_string(err));
                                            context->error_occurred->store(true);
                                        }
                                    } else {
                                        context->split_rows++;
                                        scan_data_next(context);
                                    }
                                    // should put "split_request_count--" at end of the scope,
                                    // to prevent that split_request_count becomes 0 in the middle.
                                    context->split_request_count--;
                                };
                            context->client->async_set(hash_key,
                                                       sort_key,
                                                       value,
                                                       std::move(callback),
                                                       context->timeout_ms,
                                                       ttl_seconds);
                        }
                        break;
                    case SCAN_CLEAR:
                        context->split_request_count++;
                        context->client->async_del(
                            hash_key,
                            sort_key,
                            [context](int err, pegasus::pegasus_client::internal_info &&info) {
                                if (err != pegasus::PERR_OK) {
                                    if (!context->split_completed.exchange(true)) {
                                        fprintf(stderr,
                                                "ERROR: split[%d] async del failed: %s\n",
                                                context->split_id,
                                                context->client->get_error_string(err));
                                        context->error_occurred->store(true);
                                    }
                                } else {
                                    context->split_rows++;
                                    scan_data_next(context);
                                }
                                // should put "split_request_count--" at end of the scope,
                                // to prevent that split_request_count becomes 0 in the middle.
                                context->split_request_count--;
                            },
                            context->timeout_ms);
                        break;
                    case SCAN_COUNT:
                        if (kv_count != -1) {
                            context->split_rows += kv_count;
                            scan_data_next(context);
                            break;
                        }
                        context->split_rows++;
                        if (context->stat_size && context->statistics) {
                            long hash_key_size = hash_key.size();
                            context->statistics->measureTime(
                                static_cast<uint32_t>(histogram_type::HASH_KEY_SIZE),
                                hash_key_size);

                            long sort_key_size = sort_key.size();
                            context->statistics->measureTime(
                                static_cast<uint32_t>(histogram_type::SORT_KEY_SIZE),
                                sort_key_size);

                            long value_size = value.size();
                            context->statistics->measureTime(
                                static_cast<uint32_t>(histogram_type::VALUE_SIZE), value_size);

                            long row_size = hash_key_size + sort_key_size + value_size;
                            context->statistics->measureTime(
                                static_cast<uint32_t>(histogram_type::ROW_SIZE), row_size);

                            if (context->top_count > 0) {
                                context->top_rows.push(
                                    std::move(hash_key), std::move(sort_key), row_size);
                            }
                        }
                        if (context->count_hash_key) {
                            if (hash_key != context->last_hash_key) {
                                context->split_hash_key_count++;
                                context->last_hash_key = std::move(hash_key);
                            }
                        }
                        scan_data_next(context);
                        break;
                    case SCAN_GEN_GEO:
                        context->split_request_count++;
                        ttl_seconds = compute_ttl_seconds(expire_ts_seconds, ts_expired);
                        if (ts_expired) {
                            scan_data_next(context);
                        } else {
                            context->geoclient->async_set(
                                hash_key,
                                sort_key,
                                value,
                                [context](int err, pegasus::pegasus_client::internal_info &&info) {
                                    if (err != pegasus::PERR_OK) {
                                        if (!context->split_completed.exchange(true)) {
                                            fprintf(stderr,
                                                    "ERROR: split[%d] async set failed: %s\n",
                                                    context->split_id,
                                                    context->client->get_error_string(err));
                                            context->error_occurred->store(true);
                                        }
                                    } else {
                                        context->split_rows++;
                                        scan_data_next(context);
                                    }
                                    // should put "split_request_count--" at end of the scope,
                                    // to prevent that split_request_count becomes 0 in the middle.
                                    context->split_request_count--;
                                },
                                context->timeout_ms,
                                ttl_seconds);
                        }
                        break;
                    default:
                        LOG_FATAL("op = {}", context->op);
                        break;
                    }
                } else {
                    scan_data_next(context);
                }
            } else if (ret == pegasus::PERR_SCAN_COMPLETE) {
                context->split_completed.store(true);
            } else {
                if (!context->split_completed.exchange(true)) {
                    fprintf(stderr,
                            "ERROR: split[%d] scan next failed: %s\n",
                            context->split_id,
                            context->client->get_error_string(ret));
                    context->error_occurred->store(true);
                }
            }
            // should put "split_request_count--" at end of the scope,
            // to prevent that split_request_count becomes 0 in the middle.
            context->split_request_count--;
        });

        if (context->count_hash_key) {
            // disable parallel scan if count_hash_key == true
            break;
        }
    }
}

struct node_desc
{
    std::string desc;
    dsn::host_port hp;
    node_desc(const std::string &s, const dsn::host_port &n) : desc(s), hp(n) {}
};

// type: all | replica-server | meta-server
inline bool fill_nodes(shell_context *sc, const std::string &type, std::vector<node_desc> &nodes)
{
    if (type == "all" || type == "meta-server") {
        for (const auto &hp : sc->meta_list) {
            nodes.emplace_back("meta-server", hp);
        }
    }

    if (type == "all" || type == "replica-server") {
        std::map<dsn::host_port, dsn::replication::node_status::type> rs_nodes;
        ::dsn::error_code err =
            sc->ddl_client->list_nodes(dsn::replication::node_status::NS_ALIVE, rs_nodes);
        if (err != ::dsn::ERR_OK) {
            fprintf(stderr, "ERROR: list node failed: %s\n", err.to_string());
            return false;
        }
        for (auto &kv : rs_nodes) {
            nodes.emplace_back("replica-server", kv.first);
        }
    }

    return true;
}

// Fetch the metrics according to `query_string` for each target node.
inline std::vector<dsn::http_result> get_metrics(const std::vector<node_desc> &nodes,
                                                 const std::string &query_string)
{
    std::vector<dsn::http_result> results(nodes.size());

    dsn::task_tracker tracker;
    for (size_t i = 0; i < nodes.size(); ++i) {
        (void)dsn::tasking::enqueue(
            LPC_GET_METRICS, &tracker, [&nodes, &query_string, &results, i]() {
                dsn::http_url url;

#define SET_RESULT_AND_RETURN_IF_URL_NOT_OK(name, expr)                                            \
    do {                                                                                           \
        auto err = url.set_##name(expr);                                                           \
        if (!err) {                                                                                \
            results[i] = dsn::http_result(std::move(err));                                         \
            return;                                                                                \
        }                                                                                          \
    } while (0)

                SET_RESULT_AND_RETURN_IF_URL_NOT_OK(host, nodes[i].hp.host().c_str());
                SET_RESULT_AND_RETURN_IF_URL_NOT_OK(port, nodes[i].hp.port());
                SET_RESULT_AND_RETURN_IF_URL_NOT_OK(
                    path, dsn::metrics_http_service::kMetricsQueryPath.c_str());
                SET_RESULT_AND_RETURN_IF_URL_NOT_OK(query, query_string.c_str());
                results[i] = dsn::http_get(url);

#undef SET_RESULT_AND_RETURN_IF_URL_NOT_OK
            });
    }

    tracker.wait_outstanding_tasks();
    return results;
}

// Adapt the result returned by `get_metrics` into the structure that could be processed by
// `remote_command`.
template <typename... Args>
inline dsn::error_s process_get_metrics_result(const dsn::http_result &result,
                                               const node_desc &node,
                                               const char *what,
                                               Args &&...args)
{
    if (dsn_unlikely(!result.error())) {
        return FMT_ERR(result.error().code(),
                       "ERROR: query {} metrics from node {} failed, msg={}",
                       fmt::format(what, std::forward<Args>(args)...),
                       node.hp,
                       result.error());
    }

    if (dsn_unlikely(result.status() != dsn::http_status_code::kOk)) {
        return FMT_ERR(dsn::ERR_HTTP_ERROR,
                       "ERROR: query {} metrics from node {} failed, http_status={}, msg={}",
                       fmt::format(what, std::forward<Args>(args)...),
                       node.hp,
                       dsn::get_http_status_message(result.status()),
                       result.body());
    }

    return dsn::error_s::ok();
}

#define RETURN_SHELL_IF_GET_METRICS_FAILED(result, node, what, ...)                                \
    do {                                                                                           \
        const auto &res = process_get_metrics_result(result, node, what, ##__VA_ARGS__);           \
        if (dsn_unlikely(!res)) {                                                                  \
            fmt::println(res.description());                                                       \
            return true;                                                                           \
        }                                                                                          \
    } while (0)

// Adapt the result of some parsing operations on the metrics returned by `get_metrics` into the
// structure that could be processed by `remote_command`.
template <typename... Args>
inline dsn::error_s process_parse_metrics_result(const dsn::error_s &result,
                                                 const node_desc &node,
                                                 const char *what,
                                                 Args &&...args)
{
    if (dsn_unlikely(!result)) {
        return FMT_ERR(result.code(),
                       "ERROR: {} metrics response from node {} failed, msg={}",
                       fmt::format(what, std::forward<Args>(args)...),
                       node.hp,
                       result);
    }

    return dsn::error_s::ok();
}

#define RETURN_SHELL_IF_PARSE_METRICS_FAILED(expr, node, what, ...)                                \
    do {                                                                                           \
        const auto &res = process_parse_metrics_result(expr, node, what, ##__VA_ARGS__);           \
        if (dsn_unlikely(!res)) {                                                                  \
            fmt::println(res.description());                                                       \
            return true;                                                                           \
        }                                                                                          \
    } while (0)

using stat_var_map = std::unordered_map<std::string, double *>;

// Abstract class used to aggregate the stats based on the custom filters while iterating over
// the fetched metrics.
//
// Given the type and attributes of an entity, derived classes need to implement a custom filter
// to return the selected `stat_var_map`, if any. Calculations including addition and subtraction
// are also provided for aggregating the stats. The metric name would be a dimension for the
// aggregation.
class aggregate_stats
{
public:
    aggregate_stats() = default;

    virtual ~aggregate_stats() = default;

#define CALC_STAT_VARS(entities, op)                                                               \
    for (const auto &entity : entities) {                                                          \
        stat_var_map *stat_vars = nullptr;                                                         \
        RETURN_NOT_OK(get_stat_vars(entity.type, entity.attributes, &stat_vars));                  \
                                                                                                   \
        if (stat_vars == nullptr || stat_vars->empty()) {                                          \
            continue;                                                                              \
        }                                                                                          \
                                                                                                   \
        for (const auto &m : entity.metrics) {                                                     \
            auto iter = stat_vars->find(m.name);                                                   \
            if (iter != stat_vars->end()) {                                                        \
                *iter->second op m.value;                                                          \
            }                                                                                      \
        }                                                                                          \
    }                                                                                              \
    return dsn::error_s::ok()

    // Following interfaces provide calculations over the fetched metrics. They would perform
    // each calculation on each metric whose name was found in `stat_var_map` returned by
    // `get_stat_vars`.

    // Assign the matched metric value directly to the selected member of `stat_var_map` without
    // extra calculation.
    dsn::error_s assign(const std::vector<dsn::metric_entity_brief_value_snapshot> &entities)
    {
        CALC_STAT_VARS(entities, =);
    }

    // Add and assign the matched metric value to the selected member of `stat_var_map`.
    dsn::error_s add_assign(const std::vector<dsn::metric_entity_brief_value_snapshot> &entities)
    {
        CALC_STAT_VARS(entities, +=);
    }

    // Subtract and assign the matched metric value to the selected member of `stat_var_map`.
    dsn::error_s sub_assign(const std::vector<dsn::metric_entity_brief_value_snapshot> &entities)
    {
        CALC_STAT_VARS(entities, -=);
    }

    void calc_rates(uint64_t timestamp_ns_start, uint64_t timestamp_ns_end)
    {
        calc_rates(dsn::calc_metric_sample_duration_s(timestamp_ns_start, timestamp_ns_end));
    }

#undef CALC_STAT_VARS

protected:
    // Given the type and attributes of an entity, decide which `stat_var_map` is selected, if any.
    // Otherwise, `*stat_vars` would be set to nullptr.
    virtual dsn::error_s get_stat_vars(const std::string &entity_type,
                                       const dsn::metric_entity::attr_map &entity_attrs,
                                       stat_var_map **stat_vars) = 0;

    // Implement self-defined calculation for rates, such as QPS.
    virtual void calc_rates(double duration_s) = 0;
};

// Support multiple kinds of aggregations over the fetched metrics, such as sums, increases and
// rates. Users could choose to create aggregations as needed.
class aggregate_stats_calcs
{
public:
    aggregate_stats_calcs() noexcept = default;

    ~aggregate_stats_calcs() = default;

    aggregate_stats_calcs(aggregate_stats_calcs &&) noexcept = default;
    aggregate_stats_calcs &operator=(aggregate_stats_calcs &&) noexcept = default;

#define DEF_CALC_CREATOR(name)                                                                     \
    template <typename T, typename... Args>                                                        \
    void create_##name(Args &&...args)                                                             \
    {                                                                                              \
        _##name = std::make_unique<T>(std::forward<Args>(args)...);                                \
    }

    // Create the aggregations as needed.
    DEF_CALC_CREATOR(assignments)
    DEF_CALC_CREATOR(sums)
    DEF_CALC_CREATOR(increases)
    DEF_CALC_CREATOR(rates)

#undef DEF_CALC_CREATOR

#define CALC_ASSIGNMENT_STATS(entities)                                                            \
    do {                                                                                           \
        if (_assignments) {                                                                        \
            RETURN_NOT_OK(_assignments->assign(entities));                                         \
        }                                                                                          \
    } while (0)

#define CALC_ACCUM_STATS(entities)                                                                 \
    do {                                                                                           \
        if (_sums) {                                                                               \
            RETURN_NOT_OK(_sums->add_assign(entities));                                            \
        }                                                                                          \
    } while (0)

    // Perform the chosen aggregations (both assignment and accum) on the fetched metrics.
    dsn::error_s aggregate_metrics(const std::string &json_string)
    {
        DESERIALIZE_METRIC_QUERY_BRIEF_SNAPSHOT(value, json_string, query_snapshot);

        return aggregate_metrics(query_snapshot);
    }

    dsn::error_s aggregate_metrics(const dsn::metric_query_brief_value_snapshot &query_snapshot)
    {
        CALC_ASSIGNMENT_STATS(query_snapshot.entities);
        CALC_ACCUM_STATS(query_snapshot.entities);

        return dsn::error_s::ok();
    }

    // Perform the chosen aggregations (assignement, accum, delta and rate) on the fetched metrics.
    dsn::error_s aggregate_metrics(const std::string &json_string_start,
                                   const std::string &json_string_end)
    {
        DESERIALIZE_METRIC_QUERY_BRIEF_2_SAMPLES(
            json_string_start, json_string_end, query_snapshot_start, query_snapshot_end);

        return aggregate_metrics(query_snapshot_start, query_snapshot_end);
    }

    dsn::error_s
    aggregate_metrics(const dsn::metric_query_brief_value_snapshot &query_snapshot_start,
                      const dsn::metric_query_brief_value_snapshot &query_snapshot_end)
    {
        // Apply ending sample to the assignment and accum aggregations.
        CALC_ASSIGNMENT_STATS(query_snapshot_end.entities);
        CALC_ACCUM_STATS(query_snapshot_end.entities);

        const std::array deltas_list = {&_increases, &_rates};
        for (const auto stats : deltas_list) {
            if (!(*stats)) {
                continue;
            }

            RETURN_NOT_OK((*stats)->add_assign(query_snapshot_end.entities));
            RETURN_NOT_OK((*stats)->sub_assign(query_snapshot_start.entities));
        }

        if (_rates) {
            _rates->calc_rates(query_snapshot_start.timestamp_ns, query_snapshot_end.timestamp_ns);
        }

        return dsn::error_s::ok();
    }

#undef CALC_ACCUM_STATS

#undef CALC_ASSIGNMENT_STATS

private:
    DISALLOW_COPY_AND_ASSIGN(aggregate_stats_calcs);

    std::unique_ptr<aggregate_stats> _assignments;
    std::unique_ptr<aggregate_stats> _sums;
    std::unique_ptr<aggregate_stats> _increases;
    std::unique_ptr<aggregate_stats> _rates;
};

// Convenient macros for `get_stat_vars` to set `*stat_vars` to nullptr and return under some
// circumstances.
#define RETURN_NULL_STAT_VARS_IF(expr)                                                             \
    do {                                                                                           \
        if (expr) {                                                                                \
            *stat_vars = nullptr;                                                                  \
            return dsn::error_s::ok();                                                             \
        }                                                                                          \
    } while (0)

#define RETURN_NULL_STAT_VARS_IF_NOT_OK(expr)                                                      \
    do {                                                                                           \
        const auto &err = (expr);                                                                  \
        if (dsn_unlikely(!err)) {                                                                  \
            *stat_vars = nullptr;                                                                  \
            return err;                                                                            \
        }                                                                                          \
    } while (0)

// A helper macro to parse command argument, the result is filled in a string vector variable named
// 'container'.
#define PARSE_STRS(container)                                                                      \
    do {                                                                                           \
        const auto param = cmd(param_index++).str();                                               \
        ::dsn::utils::split_args(param.c_str(), container, ',');                                   \
        if (container.empty()) {                                                                   \
            SHELL_PRINTLN_ERROR(                                                                   \
                "invalid command, '{}' should be in the form of 'val1,val2,val3' and "             \
                "should not be empty",                                                             \
                param);                                                                            \
            return false;                                                                          \
        }                                                                                          \
        std::set<std::string> str_set(container.begin(), container.end());                         \
        if (str_set.size() != container.size()) {                                                  \
            SHELL_PRINTLN_ERROR("invalid command, '{}' has duplicate values", param);              \
            return false;                                                                          \
        }                                                                                          \
    } while (false)

#define PARSE_OPT_STRS(container, def_val, ...)                                                    \
    do {                                                                                           \
        const auto param = cmd(__VA_ARGS__, (def_val)).str();                                      \
        ::dsn::utils::split_args(param.c_str(), container, ',');                                   \
    } while (false)

// A helper macro to parse command argument, the result is filled in an uint32_t variable named
// 'value'.
#define PARSE_UINT(value)                                                                          \
    do {                                                                                           \
        const auto param = cmd(param_index++).str();                                               \
        if (!::dsn::buf2uint32(param, value)) {                                                    \
            SHELL_PRINTLN_ERROR("invalid command, '{}' should be an unsigned integer", param);     \
            return false;                                                                          \
        }                                                                                          \
    } while (false)

// A helper macro to parse an optional command argument, the result is filled in an uint32_t
// variable 'value'.
//
// Variable arguments are `name` or `init_list` of argh::parser::operator(). See argh::parser
// for details.
#define PARSE_OPT_UINT(value, def_val, ...)                                                        \
    do {                                                                                           \
        const auto param = cmd(__VA_ARGS__, (def_val)).str();                                      \
        if (!::dsn::buf2uint32(param, value)) {                                                    \
            SHELL_PRINTLN_ERROR("invalid command, '{}' should be an unsigned integer", param);     \
            return false;                                                                          \
        }                                                                                          \
    } while (false)

// A helper macro to parse command argument, the result is filled in an uint32_t vector variable
// 'container'.
#define PARSE_UINTS(container)                                                                     \
    do {                                                                                           \
        std::vector<std::string> strs;                                                             \
        PARSE_STRS(strs);                                                                          \
        container.clear();                                                                         \
        for (const auto &str : strs) {                                                             \
            uint32_t v;                                                                            \
            if (!::dsn::buf2uint32(str, v)) {                                                      \
                SHELL_PRINTLN_ERROR("invalid command, '{}' should be an unsigned integer", str);   \
                return false;                                                                      \
            }                                                                                      \
            container.insert(v);                                                                   \
        }                                                                                          \
    } while (false)

// Parse enum value from the parameters of command line.
#define PARSE_OPT_ENUM(enum_val, invalid_val, ...)                                                 \
    do {                                                                                           \
        const std::string __str(cmd(__VA_ARGS__, "").str());                                       \
        if (!__str.empty()) {                                                                      \
            const auto &__val = enum_from_string(__str.c_str(), invalid_val);                      \
            if (__val == invalid_val) {                                                            \
                SHELL_PRINTLN_ERROR("invalid enum: '{}'", __str);                                  \
                return false;                                                                      \
            }                                                                                      \
            enum_val = __val;                                                                      \
        }                                                                                          \
    } while (false)

#define RETURN_FALSE_IF_NOT(expr, ...)                                                             \
    do {                                                                                           \
        if (dsn_unlikely(!(expr))) {                                                               \
            fmt::print(stderr, "{}\n", fmt::format(__VA_ARGS__));                                  \
            return false;                                                                          \
        }                                                                                          \
    } while (false)

#define RETURN_FALSE_IF_NON_OK(expr, ...)                                                          \
    do {                                                                                           \
        const auto _ec = (expr);                                                                   \
        if (dsn_unlikely(_ec != dsn::ERR_OK)) {                                                    \
            fmt::print(stderr, "{}: {}\n", _ec, fmt::format(__VA_ARGS__));                         \
            return false;                                                                          \
        }                                                                                          \
    } while (false)

#define RETURN_FALSE_IF_NON_RDB_OK(expr, ...)                                                      \
    do {                                                                                           \
        const auto _s = (expr);                                                                    \
        if (dsn_unlikely(!_s.ok())) {                                                              \
            fmt::print(stderr, "{}: {}\n", _s.ToString(), fmt::format(__VA_ARGS__));               \
            return false;                                                                          \
        }                                                                                          \
    } while (false)

// Total aggregation over the fetched metrics. The only dimension is the metric name, which
// is also the key of `stat_var_map`.
class total_aggregate_stats : public aggregate_stats
{
public:
    total_aggregate_stats(const std::string &entity_type, stat_var_map &&stat_vars)
        : _my_entity_type(entity_type), _my_stat_vars(std::move(stat_vars))
    {
    }

    ~total_aggregate_stats() = default;

protected:
    dsn::error_s get_stat_vars(const std::string &entity_type,
                               const dsn::metric_entity::attr_map &entity_attrs,
                               stat_var_map **stat_vars) override
    {
        *stat_vars = (entity_type == _my_entity_type) ? &_my_stat_vars : nullptr;
        return dsn::error_s::ok();
    }

    void calc_rates(double duration_s) override
    {
        for (auto &stat_var : _my_stat_vars) {
            *stat_var.second /= duration_s;
        }
    }

private:
    DISALLOW_COPY_AND_ASSIGN(total_aggregate_stats);

    const std::string _my_entity_type;
    stat_var_map _my_stat_vars;
};

using table_stat_map = std::unordered_map<int32_t, stat_var_map>;

// Table-level aggregation over the fetched metrics. There are 2 dimensions for the aggregation:
// * the table id, from the attributes of the metric entity;
// * the metric name, which is also the key of `stat_var_map`.
//
// It should be noted that `partitions` argument is also provided as the filter. The reason is
// that partition-level metrics from a node should be excluded under some circumstances. For
// example, the partition-level QPS we care about must be from the primary replica. The fetched
// metrics would be ignored once they are from a node that is not the primary replica of the
// target partition. However, empty `partitions` means there is no restriction.
class table_aggregate_stats : public aggregate_stats
{
public:
    table_aggregate_stats(const std::string &entity_type,
                          table_stat_map &&table_stats,
                          const std::unordered_set<dsn::gpid> &partitions)
        : _my_entity_type(entity_type),
          _my_table_stats(std::move(table_stats)),
          _my_partitions(std::move(partitions))
    {
    }

    ~table_aggregate_stats() override = default;

protected:
    dsn::error_s get_stat_vars(const std::string &entity_type,
                               const dsn::metric_entity::attr_map &entity_attrs,
                               stat_var_map **stat_vars) override
    {
        RETURN_NULL_STAT_VARS_IF(entity_type != _my_entity_type);

        int32_t metric_table_id;
        RETURN_NULL_STAT_VARS_IF_NOT_OK(dsn::parse_metric_table_id(entity_attrs, metric_table_id));

        // Empty `_my_partitions` means there is no restriction; otherwise, the partition id
        // should be found in `_my_partitions`.
        if (!_my_partitions.empty()) {
            int32_t metric_partition_id;
            RETURN_NULL_STAT_VARS_IF_NOT_OK(
                dsn::parse_metric_partition_id(entity_attrs, metric_partition_id));

            dsn::gpid metric_pid(metric_table_id, metric_partition_id);
            RETURN_NULL_STAT_VARS_IF(_my_partitions.find(metric_pid) == _my_partitions.end());
        }

        const auto &table_stat = _my_table_stats.find(metric_table_id);
        CHECK_TRUE(table_stat != _my_table_stats.end());

        *stat_vars = &table_stat->second;
        return dsn::error_s::ok();
    }

    void calc_rates(double duration_s) override
    {
        for (auto &table_stats : _my_table_stats) {
            for (auto &stat_var : table_stats.second) {
                *stat_var.second /= duration_s;
            }
        }
    }

private:
    DISALLOW_COPY_AND_ASSIGN(table_aggregate_stats);

    const std::string _my_entity_type;
    table_stat_map _my_table_stats;
    std::unordered_set<dsn::gpid> _my_partitions;
};

using partition_stat_map = std::unordered_map<dsn::gpid, stat_var_map>;

// Partition-level aggregation over the fetched metrics. There are 3 dimensions for the aggregation:
// * the table id, from the attributes of the metric entity;
// * the partition id, also from the attributes of the metric entity;
// * the metric name, which is also the key of `stat_var_map`.
class partition_aggregate_stats : public aggregate_stats
{
public:
    partition_aggregate_stats(const std::string &entity_type, partition_stat_map &&partition_stats)
        : _my_entity_type(entity_type), _my_partition_stats(std::move(partition_stats))
    {
    }

    ~partition_aggregate_stats() override = default;

protected:
    dsn::error_s get_stat_vars(const std::string &entity_type,
                               const dsn::metric_entity::attr_map &entity_attrs,
                               stat_var_map **stat_vars) override
    {
        RETURN_NULL_STAT_VARS_IF(entity_type != _my_entity_type);

        int32_t metric_table_id;
        RETURN_NULL_STAT_VARS_IF_NOT_OK(dsn::parse_metric_table_id(entity_attrs, metric_table_id));

        int32_t metric_partition_id;
        RETURN_NULL_STAT_VARS_IF_NOT_OK(
            dsn::parse_metric_partition_id(entity_attrs, metric_partition_id));

        dsn::gpid metric_pid(metric_table_id, metric_partition_id);
        const auto &partition_stat = _my_partition_stats.find(metric_pid);
        RETURN_NULL_STAT_VARS_IF(partition_stat == _my_partition_stats.end());

        *stat_vars = &partition_stat->second;
        return dsn::error_s::ok();
    }

    void calc_rates(double duration_s) override
    {
        for (auto &partition_stats : _my_partition_stats) {
            for (auto &stat_var : partition_stats.second) {
                *stat_var.second /= duration_s;
            }
        }
    }

private:
    DISALLOW_COPY_AND_ASSIGN(partition_aggregate_stats);

    const std::string _my_entity_type;
    partition_stat_map _my_partition_stats;
};

inline std::vector<std::pair<bool, std::string>>
call_remote_command(shell_context *sc,
                    const std::vector<node_desc> &nodes,
                    const std::string &cmd,
                    const std::vector<std::string> &arguments)
{
    std::vector<std::pair<bool, std::string>> results;
    std::vector<dsn::task_ptr> tasks;
    tasks.resize(nodes.size());
    results.resize(nodes.size());
    for (int i = 0; i < nodes.size(); ++i) {
        auto callback = [&results, i](::dsn::error_code err, const std::string &resp) {
            if (err == ::dsn::ERR_OK) {
                results[i].first = true;
                results[i].second = resp;
            } else {
                results[i].first = false;
                results[i].second = err.to_string();
            }
        };
        tasks[i] = dsn::dist::cmd::async_call_remote(
            dsn::dns_resolver::instance().resolve_address(nodes[i].hp),
            cmd,
            arguments,
            callback,
            std::chrono::milliseconds(5000));
    }
    for (int i = 0; i < nodes.size(); ++i) {
        tasks[i]->wait();
    }
    return results;
}

inline bool parse_app_pegasus_perf_counter_name(const std::string &name,
                                                int32_t &app_id,
                                                int32_t &partition_index,
                                                std::string &counter_name)
{
    std::string::size_type find = name.find_last_of('@');
    if (find == std::string::npos)
        return false;
    int n = sscanf(name.c_str() + find + 1, "%d.%d", &app_id, &partition_index);
    if (n != 2)
        return false;
    std::string::size_type find2 = name.find_last_of('*');
    if (find2 == std::string::npos)
        return false;
    counter_name = name.substr(find2 + 1, find - find2 - 1);
    return true;
}

inline bool parse_app_perf_counter_name(const std::string &name,
                                        std::string &app_name,
                                        std::string &counter_name)
{
    /**
     * name format:
     *   1.{node}*{section}*{counter_name}@{app_name}.{percent_line}
     *   2.{node}*{section}*{counter_name}@{app_name}
     */
    std::string::size_type find = name.find_last_of('@');
    if (find == std::string::npos)
        return false;

    std::string::size_type find2 = name.find_last_of('.');
    if (find2 == std::string::npos) {
        app_name = name.substr(find + 1);
    } else {
        app_name = name.substr(find + 1, find2 - find - 1);
    }

    std::string::size_type find3 = name.find_last_of('*');
    if (find3 == std::string::npos)
        return false;
    counter_name = name.substr(find3 + 1, find - find3 - 1);
    return true;
}

struct row_data
{
    row_data() = default;
    explicit row_data(const std::string &name) : row_name(name) {}

    double get_total_read_qps() const { return get_qps + multi_get_qps + batch_get_qps + scan_qps; }

    double get_total_write_qps() const
    {
        return put_qps + remove_qps + multi_put_qps + multi_remove_qps + check_and_set_qps +
               check_and_mutate_qps + incr_qps + duplicate_qps;
    }

    double get_total_read_bytes() const
    {
        return get_bytes + multi_get_bytes + batch_get_bytes + scan_bytes;
    }

    double get_total_write_bytes() const
    {
        return put_bytes + multi_put_bytes + check_and_set_bytes + check_and_mutate_bytes;
    }

    void aggregate(const row_data &row)
    {
        get_qps += row.get_qps;
        multi_get_qps += row.multi_get_qps;
        batch_get_qps += row.batch_get_qps;
        put_qps += row.put_qps;
        multi_put_qps += row.multi_put_qps;
        remove_qps += row.remove_qps;
        multi_remove_qps += row.multi_remove_qps;
        incr_qps += row.incr_qps;
        check_and_set_qps += row.check_and_set_qps;
        check_and_mutate_qps += row.check_and_mutate_qps;
        scan_qps += row.scan_qps;
        duplicate_qps += row.duplicate_qps;
        dup_shipped_ops += row.dup_shipped_ops;
        dup_failed_shipping_ops += row.dup_failed_shipping_ops;
        dup_recent_mutation_loss_count += row.dup_recent_mutation_loss_count;
        recent_read_cu += row.recent_read_cu;
        recent_write_cu += row.recent_write_cu;
        recent_expire_count += row.recent_expire_count;
        recent_filter_count += row.recent_filter_count;
        recent_abnormal_count += row.recent_abnormal_count;
        recent_write_throttling_delay_count += row.recent_write_throttling_delay_count;
        recent_write_throttling_reject_count += row.recent_write_throttling_reject_count;
        recent_read_throttling_delay_count += row.recent_read_throttling_delay_count;
        recent_read_throttling_reject_count += row.recent_read_throttling_reject_count;
        recent_backup_request_throttling_delay_count +=
            row.recent_backup_request_throttling_delay_count;
        recent_backup_request_throttling_reject_count +=
            row.recent_backup_request_throttling_reject_count;
        recent_write_splitting_reject_count += row.recent_write_splitting_reject_count;
        recent_read_splitting_reject_count += row.recent_read_splitting_reject_count;
        recent_write_bulk_load_ingestion_reject_count +=
            row.recent_write_bulk_load_ingestion_reject_count;
        storage_mb += row.storage_mb;
        storage_count += row.storage_count;
        rdb_block_cache_hit_count += row.rdb_block_cache_hit_count;
        rdb_block_cache_total_count += row.rdb_block_cache_total_count;
        rdb_index_and_filter_blocks_mem_usage += row.rdb_index_and_filter_blocks_mem_usage;
        rdb_memtable_mem_usage += row.rdb_memtable_mem_usage;
        rdb_estimate_num_keys += row.rdb_estimate_num_keys;
        rdb_bf_seek_negatives += row.rdb_bf_seek_negatives;
        rdb_bf_seek_total += row.rdb_bf_seek_total;
        rdb_bf_point_positive_true += row.rdb_bf_point_positive_true;
        rdb_bf_point_positive_total += row.rdb_bf_point_positive_total;
        rdb_bf_point_negatives += row.rdb_bf_point_negatives;
        backup_request_qps += row.backup_request_qps;
        backup_request_bytes += row.backup_request_bytes;
        get_bytes += row.get_bytes;
        multi_get_bytes += row.multi_get_bytes;
        batch_get_bytes += row.batch_get_bytes;
        scan_bytes += row.scan_bytes;
        put_bytes += row.put_bytes;
        multi_put_bytes += row.multi_put_bytes;
        check_and_set_bytes += row.check_and_set_bytes;
        check_and_mutate_bytes += row.check_and_mutate_bytes;
        recent_rdb_compaction_input_bytes += row.recent_rdb_compaction_input_bytes;
        recent_rdb_compaction_output_bytes += row.recent_rdb_compaction_output_bytes;
        rdb_read_l2andup_hit_count += row.rdb_read_l2andup_hit_count;
        rdb_read_l1_hit_count += row.rdb_read_l1_hit_count;
        rdb_read_l0_hit_count += row.rdb_read_l0_hit_count;
        rdb_read_memtable_hit_count += row.rdb_read_memtable_hit_count;
        rdb_write_amplification += row.rdb_write_amplification;
        rdb_read_amplification += row.rdb_read_amplification;
    }

    std::string row_name;
    int32_t app_id = 0;
    int32_t partition_count = 0;
    double get_qps = 0;
    double multi_get_qps = 0;
    double batch_get_qps = 0;
    double put_qps = 0;
    double multi_put_qps = 0;
    double remove_qps = 0;
    double multi_remove_qps = 0;
    double incr_qps = 0;
    double check_and_set_qps = 0;
    double check_and_mutate_qps = 0;
    double scan_qps = 0;
    double duplicate_qps = 0;
    double dup_shipped_ops = 0;
    double dup_failed_shipping_ops = 0;
    double dup_recent_mutation_loss_count = 0;
    double recent_read_cu = 0;
    double recent_write_cu = 0;
    double recent_expire_count = 0;
    double recent_filter_count = 0;
    double recent_abnormal_count = 0;
    double recent_write_throttling_delay_count = 0;
    double recent_write_throttling_reject_count = 0;
    double recent_read_throttling_delay_count = 0;
    double recent_read_throttling_reject_count = 0;
    double recent_backup_request_throttling_delay_count = 0;
    double recent_backup_request_throttling_reject_count = 0;
    double recent_write_splitting_reject_count = 0;
    double recent_read_splitting_reject_count = 0;
    double recent_write_bulk_load_ingestion_reject_count = 0;
    double storage_mb = 0;
    double storage_count = 0;
    double rdb_block_cache_hit_count = 0;
    double rdb_block_cache_total_count = 0;
    double rdb_index_and_filter_blocks_mem_usage = 0;
    double rdb_memtable_mem_usage = 0;
    double rdb_estimate_num_keys = 0;
    double rdb_bf_seek_negatives = 0;
    double rdb_bf_seek_total = 0;
    double rdb_bf_point_positive_true = 0;
    double rdb_bf_point_positive_total = 0;
    double rdb_bf_point_negatives = 0;
    double backup_request_qps = 0;
    double backup_request_bytes = 0;
    double get_bytes = 0;
    double multi_get_bytes = 0;
    double batch_get_bytes = 0;
    double scan_bytes = 0;
    double put_bytes = 0;
    double multi_put_bytes = 0;
    double check_and_set_bytes = 0;
    double check_and_mutate_bytes = 0;
    double recent_rdb_compaction_input_bytes = 0;
    double recent_rdb_compaction_output_bytes = 0;
    double rdb_read_l2andup_hit_count = 0;
    double rdb_read_l1_hit_count = 0;
    double rdb_read_l0_hit_count = 0;
    double rdb_read_memtable_hit_count = 0;
    double rdb_write_amplification = 0;
    double rdb_read_amplification = 0;
};

// TODO(wangdan): there are still dozens of fields to be added to the following functions.
inline dsn::metric_filters row_data_filters()
{
    dsn::metric_filters filters;
    filters.with_metric_fields = {dsn::kMetricNameField, dsn::kMetricSingleValueField};
    filters.entity_types = {"replica"};
    filters.entity_metrics = {
        "get_requests",
        "multi_get_requests",
        "batch_get_requests",
        "put_requests",
        "multi_put_requests",
        "remove_requests",
        "multi_remove_requests",
        "incr_requests",
        "check_and_set_requests",
        "check_and_mutate_requests",
        "scan_requests",
        "dup_requests",
        "dup_shipped_successful_requests",
        "dup_shipped_failed_requests",
        "dup_recent_lost_mutations",
        "read_capacity_units",
        "write_capacity_units",
        "read_expired_values",
        "read_filtered_values",
        "abnormal_read_requests",
        "throttling_delayed_write_requests",
        "throttling_rejected_write_requests",
        "throttling_delayed_read_requests",
        "throttling_rejected_read_requests",
        "throttling_delayed_backup_requests",
        "throttling_rejected_backup_requests",
        "splitting_rejected_write_requests",
        "splitting_rejected_read_requests",
        "bulk_load_ingestion_rejected_write_requests",
        "rdb_total_sst_size_mb",
        "rdb_total_sst_files",
        "rdb_block_cache_hit_count",
        "rdb_block_cache_total_count",
        "rdb_index_and_filter_blocks_mem_usage_bytes",
        "rdb_memtable_mem_usage_bytes",
        "rdb_estimated_keys",
        "rdb_bloom_filter_seek_negatives",
        "rdb_bloom_filter_seek_total",
        "rdb_bloom_filter_point_lookup_true_positives",
        "rdb_bloom_filter_point_lookup_positives",
        "rdb_bloom_filter_point_lookup_negatives",
        "backup_requests",
        "backup_request_bytes",
        "get_bytes",
        "multi_get_bytes",
        "batch_get_bytes",
        "scan_bytes",
        "put_bytes",
        "multi_put_bytes",
        "check_and_set_bytes",
        "check_and_mutate_bytes",
        "rdb_compaction_input_bytes",
        "rdb_compaction_output_bytes",
        "rdb_l2_and_up_hit_count",
        "rdb_l1_hit_count",
        "rdb_l0_hit_count",
        "rdb_memtable_hit_count",
        "rdb_write_amplification",
        "rdb_read_amplification",
    };
    return filters;
}

inline dsn::metric_filters row_data_filters(int32_t table_id)
{
    auto filters = row_data_filters();
    filters.entity_attrs = {"table_id", std::to_string(table_id)};
    return filters;
}

#define BIND_ROW(metric_name, member)                                                              \
    {                                                                                              \
#metric_name, &row.member                                                                  \
    }

inline stat_var_map create_sums(row_data &row)
{
    return stat_var_map({
        BIND_ROW(dup_recent_lost_mutations, dup_recent_mutation_loss_count),
        BIND_ROW(rdb_total_sst_size_mb, storage_mb),
        BIND_ROW(rdb_total_sst_files, storage_count),
        BIND_ROW(rdb_block_cache_hit_count, rdb_block_cache_hit_count),
        BIND_ROW(rdb_block_cache_total_count, rdb_block_cache_total_count),
        BIND_ROW(rdb_index_and_filter_blocks_mem_usage_bytes,
                 rdb_index_and_filter_blocks_mem_usage),
        BIND_ROW(rdb_memtable_mem_usage_bytes, rdb_memtable_mem_usage),
        BIND_ROW(rdb_estimated_keys, rdb_estimate_num_keys),
        BIND_ROW(rdb_bloom_filter_seek_negatives, rdb_bf_seek_negatives),
        BIND_ROW(rdb_bloom_filter_seek_total, rdb_bf_seek_total),
        BIND_ROW(rdb_bloom_filter_point_lookup_true_positives, rdb_bf_point_positive_true),
        BIND_ROW(rdb_bloom_filter_point_lookup_positives, rdb_bf_point_positive_total),
        BIND_ROW(rdb_bloom_filter_point_lookup_negatives, rdb_bf_point_negatives),
        BIND_ROW(rdb_l2_and_up_hit_count, rdb_read_l2andup_hit_count),
        BIND_ROW(rdb_l1_hit_count, rdb_read_l1_hit_count),
        BIND_ROW(rdb_l0_hit_count, rdb_read_l0_hit_count),
        BIND_ROW(rdb_memtable_hit_count, rdb_read_memtable_hit_count),
        BIND_ROW(rdb_write_amplification, rdb_write_amplification),
        BIND_ROW(rdb_read_amplification, rdb_read_amplification),
    });
}

inline stat_var_map create_increases(row_data &row)
{
    return stat_var_map({
        BIND_ROW(read_capacity_units, recent_read_cu),
        BIND_ROW(write_capacity_units, recent_write_cu),
        BIND_ROW(read_expired_values, recent_expire_count),
        BIND_ROW(read_filtered_values, recent_filter_count),
        BIND_ROW(abnormal_read_requests, recent_abnormal_count),
        BIND_ROW(throttling_delayed_write_requests, recent_write_throttling_delay_count),
        BIND_ROW(throttling_rejected_write_requests, recent_write_throttling_reject_count),
        BIND_ROW(throttling_delayed_read_requests, recent_read_throttling_delay_count),
        BIND_ROW(throttling_rejected_read_requests, recent_read_throttling_reject_count),
        BIND_ROW(throttling_delayed_backup_requests, recent_backup_request_throttling_delay_count),
        BIND_ROW(throttling_rejected_backup_requests,
                 recent_backup_request_throttling_reject_count),
        BIND_ROW(splitting_rejected_write_requests, recent_write_splitting_reject_count),
        BIND_ROW(splitting_rejected_read_requests, recent_read_splitting_reject_count),
        BIND_ROW(bulk_load_ingestion_rejected_write_requests,
                 recent_write_bulk_load_ingestion_reject_count),
        BIND_ROW(rdb_compaction_input_bytes, recent_rdb_compaction_input_bytes),
        BIND_ROW(rdb_compaction_output_bytes, recent_rdb_compaction_output_bytes),
    });
}

inline stat_var_map create_rates(row_data &row)
{
    return stat_var_map({
        BIND_ROW(get_requests, get_qps),
        BIND_ROW(multi_get_requests, multi_get_qps),
        BIND_ROW(batch_get_requests, batch_get_qps),
        BIND_ROW(put_requests, put_qps),
        BIND_ROW(multi_put_requests, multi_put_qps),
        BIND_ROW(remove_requests, remove_qps),
        BIND_ROW(multi_remove_requests, multi_remove_qps),
        BIND_ROW(incr_requests, incr_qps),
        BIND_ROW(check_and_set_requests, check_and_set_qps),
        BIND_ROW(check_and_mutate_requests, check_and_mutate_qps),
        BIND_ROW(scan_requests, scan_qps),
        BIND_ROW(dup_requests, duplicate_qps),
        BIND_ROW(dup_shipped_successful_requests, dup_shipped_ops),
        BIND_ROW(dup_shipped_failed_requests, dup_failed_shipping_ops),
        BIND_ROW(backup_requests, backup_request_qps),
        BIND_ROW(backup_request_bytes, backup_request_bytes),
        BIND_ROW(get_bytes, get_bytes),
        BIND_ROW(multi_get_bytes, multi_get_bytes),
        BIND_ROW(batch_get_bytes, batch_get_bytes),
        BIND_ROW(scan_bytes, scan_bytes),
        BIND_ROW(put_bytes, put_bytes),
        BIND_ROW(multi_put_bytes, multi_put_bytes),
        BIND_ROW(check_and_set_bytes, check_and_set_bytes),
        BIND_ROW(check_and_mutate_bytes, check_and_mutate_bytes),
    });
}

#undef BIND_ROW

// Given all tables, create all aggregations needed for the table-level stats. All selected
// partitions should have their primary replicas on this node.
inline std::unique_ptr<aggregate_stats_calcs> create_table_aggregate_stats_calcs(
    const std::map<int32_t, std::vector<dsn::partition_configuration>> &pcs_by_appid,
    const dsn::host_port &node,
    const std::string &entity_type,
    std::vector<row_data> &rows)
{
    table_stat_map sums;
    table_stat_map increases;
    table_stat_map rates;
    std::unordered_set<dsn::gpid> partitions;
    for (auto &row : rows) {
        const std::vector<std::pair<table_stat_map *, std::function<stat_var_map(row_data &)>>>
            processors = {
                {&sums, create_sums},
                {&increases, create_increases},
                {&rates, create_rates},
            };
        for (auto &processor : processors) {
            // Put both dimensions of table id and metric name into filters for each kind of
            // aggregation.
            processor.first->emplace(row.app_id, processor.second(row));
        }

        const auto &iter = pcs_by_appid.find(row.app_id);
        CHECK(iter != pcs_by_appid.end(),
              "table could not be found in pcs_by_appid: table_id={}",
              row.app_id);

        for (const auto &pc : iter->second) {
            if (pc.hp_primary != node) {
                // Ignore once the replica of the metrics is not the primary of the partition.
                continue;
            }

            partitions.insert(pc.pid);
        }
    }

    auto calcs = std::make_unique<aggregate_stats_calcs>();
    calcs->create_sums<table_aggregate_stats>(entity_type, std::move(sums), partitions);
    calcs->create_increases<table_aggregate_stats>(entity_type, std::move(increases), partitions);
    calcs->create_rates<table_aggregate_stats>(entity_type, std::move(rates), partitions);
    return calcs;
}

// Given a table and all of its partitions, create all aggregations needed for the partition-level
// stats. All selected partitions should have their primary replicas on this node.
inline std::unique_ptr<aggregate_stats_calcs>
create_partition_aggregate_stats_calcs(const int32_t table_id,
                                       const std::vector<dsn::partition_configuration> &pcs,
                                       const dsn::host_port &node,
                                       const std::string &entity_type,
                                       std::vector<row_data> &rows)
{
    CHECK_EQ(rows.size(), pcs.size());

    partition_stat_map sums;
    partition_stat_map increases;
    partition_stat_map rates;
    for (size_t i = 0; i < rows.size(); ++i) {
        if (pcs[i].hp_primary != node) {
            // Ignore once the replica of the metrics is not the primary of the partition.
            continue;
        }

        const std::vector<std::pair<partition_stat_map *, std::function<stat_var_map(row_data &)>>>
            processors = {
                {&sums, create_sums},
                {&increases, create_increases},
                {&rates, create_rates},
            };
        for (auto &processor : processors) {
            // Put all dimensions of table id, partition_id,  and metric name into filters for
            // each kind of aggregation.
            processor.first->emplace(dsn::gpid(table_id, i), processor.second(rows[i]));
        }
    }

    auto calcs = std::make_unique<aggregate_stats_calcs>();
    calcs->create_sums<partition_aggregate_stats>(entity_type, std::move(sums));
    calcs->create_increases<partition_aggregate_stats>(entity_type, std::move(increases));
    calcs->create_rates<partition_aggregate_stats>(entity_type, std::move(rates));
    return calcs;
}

inline bool
update_app_pegasus_perf_counter(row_data &row, const std::string &counter_name, double value)
{
    if (counter_name == "get_qps")
        row.get_qps += value;
    else if (counter_name == "multi_get_qps")
        row.multi_get_qps += value;
    else if (counter_name == "batch_get_qps")
        row.batch_get_qps += value;
    else if (counter_name == "put_qps")
        row.put_qps += value;
    else if (counter_name == "multi_put_qps")
        row.multi_put_qps += value;
    else if (counter_name == "remove_qps")
        row.remove_qps += value;
    else if (counter_name == "multi_remove_qps")
        row.multi_remove_qps += value;
    else if (counter_name == "incr_qps")
        row.incr_qps += value;
    else if (counter_name == "check_and_set_qps")
        row.check_and_set_qps += value;
    else if (counter_name == "check_and_mutate_qps")
        row.check_and_mutate_qps += value;
    else if (counter_name == "scan_qps")
        row.scan_qps += value;
    else if (counter_name == "duplicate_qps")
        row.duplicate_qps += value;
    else if (counter_name == "dup_shipped_ops")
        row.dup_shipped_ops += value;
    else if (counter_name == "dup_failed_shipping_ops")
        row.dup_failed_shipping_ops += value;
    else if (counter_name == "dup_recent_mutation_loss_count")
        row.dup_recent_mutation_loss_count += value;
    else if (counter_name == "recent.read.cu")
        row.recent_read_cu += value;
    else if (counter_name == "recent.write.cu")
        row.recent_write_cu += value;
    else if (counter_name == "recent.expire.count")
        row.recent_expire_count += value;
    else if (counter_name == "recent.filter.count")
        row.recent_filter_count += value;
    else if (counter_name == "recent.abnormal.count")
        row.recent_abnormal_count += value;
    else if (counter_name == "recent.write.throttling.delay.count")
        row.recent_write_throttling_delay_count += value;
    else if (counter_name == "recent.write.throttling.reject.count")
        row.recent_write_throttling_reject_count += value;
    else if (counter_name == "recent.read.throttling.delay.count")
        row.recent_read_throttling_delay_count += value;
    else if (counter_name == "recent.read.throttling.reject.count")
        row.recent_read_throttling_reject_count += value;
    else if (counter_name == "recent.backup.request.throttling.delay.count")
        row.recent_backup_request_throttling_delay_count += value;
    else if (counter_name == "recent.backup.request.throttling.reject.count")
        row.recent_backup_request_throttling_reject_count += value;
    else if (counter_name == "recent.write.splitting.reject.count")
        row.recent_write_splitting_reject_count += value;
    else if (counter_name == "recent.read.splitting.reject.count")
        row.recent_read_splitting_reject_count += value;
    else if (counter_name == "recent.write.bulk.load.ingestion.reject.count")
        row.recent_write_bulk_load_ingestion_reject_count += value;
    else if (counter_name == "disk.storage.sst(MB)")
        row.storage_mb += value;
    else if (counter_name == "disk.storage.sst.count")
        row.storage_count += value;
    else if (counter_name == "rdb.block_cache.hit_count")
        row.rdb_block_cache_hit_count += value;
    else if (counter_name == "rdb.block_cache.total_count")
        row.rdb_block_cache_total_count += value;
    else if (counter_name == "rdb.index_and_filter_blocks.memory_usage")
        row.rdb_index_and_filter_blocks_mem_usage += value;
    else if (counter_name == "rdb.memtable.memory_usage")
        row.rdb_memtable_mem_usage += value;
    else if (counter_name == "rdb.estimate_num_keys")
        row.rdb_estimate_num_keys += value;
    else if (counter_name == "rdb.bf_seek_negatives")
        row.rdb_bf_seek_negatives += value;
    else if (counter_name == "rdb.bf_seek_total")
        row.rdb_bf_seek_total += value;
    else if (counter_name == "rdb.bf_point_positive_true")
        row.rdb_bf_point_positive_true += value;
    else if (counter_name == "rdb.bf_point_positive_total")
        row.rdb_bf_point_positive_total += value;
    else if (counter_name == "rdb.bf_point_negatives")
        row.rdb_bf_point_negatives += value;
    else if (counter_name == "backup_request_qps")
        row.backup_request_qps += value;
    else if (counter_name == "backup_request_bytes")
        row.backup_request_bytes += value;
    else if (counter_name == "get_bytes")
        row.get_bytes += value;
    else if (counter_name == "multi_get_bytes")
        row.multi_get_bytes += value;
    else if (counter_name == "batch_get_bytes")
        row.batch_get_bytes += value;
    else if (counter_name == "scan_bytes")
        row.scan_bytes += value;
    else if (counter_name == "put_bytes")
        row.put_bytes += value;
    else if (counter_name == "multi_put_bytes")
        row.multi_put_bytes += value;
    else if (counter_name == "check_and_set_bytes")
        row.check_and_set_bytes += value;
    else if (counter_name == "check_and_mutate_bytes")
        row.check_and_mutate_bytes += value;
    else if (counter_name == "recent_rdb_compaction_input_bytes")
        row.recent_rdb_compaction_input_bytes += value;
    else if (counter_name == "recent_rdb_compaction_output_bytes")
        row.recent_rdb_compaction_output_bytes += value;
    else if (counter_name == "rdb.read_l2andup_hit_count")
        row.rdb_read_l2andup_hit_count += value;
    else if (counter_name == "rdb.read_l1_hit_count")
        row.rdb_read_l1_hit_count += value;
    else if (counter_name == "rdb.read_l0_hit_count")
        row.rdb_read_l0_hit_count += value;
    else if (counter_name == "rdb.read_memtable_hit_count")
        row.rdb_read_memtable_hit_count += value;
    else if (counter_name == "rdb.write_amplification")
        row.rdb_write_amplification += value;
    else if (counter_name == "rdb.read_amplification")
        row.rdb_read_amplification += value;
    else
        return false;
    return true;
}

inline bool get_apps_and_nodes(shell_context *sc,
                               std::vector<::dsn::app_info> &apps,
                               std::vector<node_desc> &nodes)
{
    const auto &result = sc->ddl_client->list_apps(dsn::app_status::AS_AVAILABLE, apps);
    if (!result) {
        LOG_ERROR("list apps failed, error={}", result);
        return false;
    }

    if (!fill_nodes(sc, "replica-server", nodes)) {
        LOG_ERROR("get replica server node list failed");
        return false;
    }
    return true;
}

inline bool
get_app_partitions(shell_context *sc,
                   const std::vector<::dsn::app_info> &apps,
                   std::map<int32_t, std::vector<dsn::partition_configuration>> &pcs_by_appid)
{
    for (const ::dsn::app_info &app : apps) {
        int32_t app_id = 0;
        int32_t partition_count = 0;
        dsn::error_code err = sc->ddl_client->list_app(
            app.app_name, app_id, partition_count, pcs_by_appid[app.app_id]);
        if (err != ::dsn::ERR_OK) {
            LOG_ERROR("list app {} failed, error = {}", app.app_name, err);
            return false;
        }
        CHECK_EQ(app_id, app.app_id);
        CHECK_EQ(partition_count, app.partition_count);
    }
    return true;
}

inline bool decode_node_perf_counter_info(const dsn::host_port &hp,
                                          const std::pair<bool, std::string> &result,
                                          dsn::perf_counter_info &info)
{
    if (!result.first) {
        LOG_ERROR("query perf counter info from node {} failed", hp);
        return false;
    }
    dsn::blob bb(result.second.data(), 0, result.second.size());
    if (!dsn::json::json_forwarder<dsn::perf_counter_info>::decode(bb, info)) {
        LOG_ERROR("decode perf counter info from node {} failed, result = {}", hp, result.second);
        return false;
    }
    if (info.result != "OK") {
        LOG_ERROR(
            "query perf counter info from node {} returns error, error = {}", hp, info.result);
        return false;
    }
    return true;
}

// rows: key-app name, value-perf counters for each partition
inline bool get_app_partition_stat(shell_context *sc,
                                   std::map<std::string, std::vector<row_data>> &rows)
{
    // get apps and nodes
    std::vector<::dsn::app_info> apps;
    std::vector<node_desc> nodes;
    if (!get_apps_and_nodes(sc, apps, nodes)) {
        return false;
    }

    // get the relationship between app_id and app_name
    std::map<int32_t, std::string> app_id_name;
    std::map<std::string, int32_t> app_name_id;
    for (::dsn::app_info &app : apps) {
        app_id_name[app.app_id] = app.app_name;
        app_name_id[app.app_name] = app.app_id;
        rows[app.app_name].resize(app.partition_count);
    }

    // get app_id --> partitions
    std::map<int32_t, std::vector<dsn::partition_configuration>> pcs_by_appid;
    if (!get_app_partitions(sc, apps, pcs_by_appid)) {
        return false;
    }

    // get all of the perf counters with format ".*@.*"
    std::vector<std::pair<bool, std::string>> results =
        call_remote_command(sc, nodes, "perf-counters", {".*@.*"});

    for (int i = 0; i < nodes.size(); ++i) {
        // decode info of perf-counters on node i
        dsn::perf_counter_info info;
        if (!decode_node_perf_counter_info(nodes[i].hp, results[i], info)) {
            return false;
        }

        for (dsn::perf_counter_metric &m : info.counters) {
            // get app_id/partition_id/counter_name/app_name from the name of perf-counter
            int32_t app_id_x, partition_index_x;
            std::string counter_name;
            std::string app_name;

            if (parse_app_pegasus_perf_counter_name(
                    m.name, app_id_x, partition_index_x, counter_name)) {
                // only primary partition will be counted
                const auto find = pcs_by_appid.find(app_id_x);
                if (find != pcs_by_appid.end() &&
                    find->second[partition_index_x].hp_primary == nodes[i].hp) {
                    row_data &row = rows[app_id_name[app_id_x]][partition_index_x];
                    row.row_name = std::to_string(partition_index_x);
                    row.app_id = app_id_x;
                    update_app_pegasus_perf_counter(row, counter_name, m.value);
                }
            } else if (parse_app_perf_counter_name(m.name, app_name, counter_name)) {
                // if the app_name from perf-counter isn't existed(maybe the app was dropped), it
                // will be ignored.
                if (app_name_id.find(app_name) == app_name_id.end()) {
                    continue;
                }
                // perf-counter value will be set into partition index 0.
                row_data &row = rows[app_name][0];
                row.app_id = app_name_id[app_name];
                update_app_pegasus_perf_counter(row, counter_name, m.value);
            }
        }
    }
    return true;
}

// Aggregate the table-level stats for all tables since table name is not specified.
inline bool
get_table_stats(shell_context *sc, uint32_t sample_interval_ms, std::vector<row_data> &rows)
{
    std::vector<::dsn::app_info> apps;
    std::vector<node_desc> nodes;
    if (!get_apps_and_nodes(sc, apps, nodes)) {
        return false;
    }

    const auto &query_string = row_data_filters().to_query_string();
    const auto &results_start = get_metrics(nodes, query_string);
    std::this_thread::sleep_for(std::chrono::milliseconds(sample_interval_ms));
    const auto &results_end = get_metrics(nodes, query_string);

    std::map<int32_t, std::vector<dsn::partition_configuration>> pcs_by_appid;
    if (!get_app_partitions(sc, apps, pcs_by_appid)) {
        return false;
    }

    rows.clear();
    rows.reserve(apps.size());
    std::transform(
        apps.begin(), apps.end(), std::back_inserter(rows), [](const dsn::app_info &app) {
            row_data row;
            row.row_name = app.app_name;
            row.app_id = app.app_id;
            row.partition_count = app.partition_count;
            return row;
        });
    CHECK_EQ(rows.size(), pcs_by_appid.size());

    for (size_t i = 0; i < nodes.size(); ++i) {
        RETURN_SHELL_IF_GET_METRICS_FAILED(
            results_start[i], nodes[i], "starting row data requests");
        RETURN_SHELL_IF_GET_METRICS_FAILED(results_end[i], nodes[i], "ending row data requests");

        auto calcs = create_table_aggregate_stats_calcs(pcs_by_appid, nodes[i].hp, "replica", rows);
        RETURN_SHELL_IF_PARSE_METRICS_FAILED(
            calcs->aggregate_metrics(results_start[i].body(), results_end[i].body()),
            nodes[i],
            "aggregate row data requests");
    }

    return true;
}

// Aggregate the partition-level stats for the specified table.
inline bool get_partition_stats(shell_context *sc,
                                const std::string &table_name,
                                uint32_t sample_interval_ms,
                                std::vector<row_data> &rows)
{
    std::vector<node_desc> nodes;
    if (!fill_nodes(sc, "replica-server", nodes)) {
        LOG_ERROR("get replica server node list failed");
        return false;
    }

    int32_t table_id = 0;
    int32_t partition_count = 0;
    std::vector<dsn::partition_configuration> pcs;
    const auto &err = sc->ddl_client->list_app(table_name, table_id, partition_count, pcs);
    if (err != ::dsn::ERR_OK) {
        LOG_ERROR("list app {} failed, error = {}", table_name, err);
        return false;
    }
    CHECK_EQ(pcs.size(), partition_count);

    const auto &query_string = row_data_filters(table_id).to_query_string();
    const auto &results_start = get_metrics(nodes, query_string);
    std::this_thread::sleep_for(std::chrono::milliseconds(sample_interval_ms));
    const auto &results_end = get_metrics(nodes, query_string);

    rows.clear();
    rows.reserve(partition_count);
    for (int32_t i = 0; i < partition_count; ++i) {
        rows.emplace_back(std::to_string(i));
    }

    for (size_t i = 0; i < nodes.size(); ++i) {
        RETURN_SHELL_IF_GET_METRICS_FAILED(
            results_start[i], nodes[i], "starting row data requests for table(id={})", table_id);
        RETURN_SHELL_IF_GET_METRICS_FAILED(
            results_end[i], nodes[i], "ending row data requests for table(id={})", table_id);

        auto calcs =
            create_partition_aggregate_stats_calcs(table_id, pcs, nodes[i].hp, "replica", rows);
        RETURN_SHELL_IF_PARSE_METRICS_FAILED(
            calcs->aggregate_metrics(results_start[i].body(), results_end[i].body()),
            nodes[i],
            "aggregate row data requests for table(id={})",
            table_id);
    }

    return true;
}

inline bool get_app_stat(shell_context *sc,
                         const std::string &table_name,
                         uint32_t sample_interval_ms,
                         std::vector<row_data> &rows)
{
    if (table_name.empty()) {
        return get_table_stats(sc, sample_interval_ms, rows);
    }

    return get_partition_stats(sc, table_name, sample_interval_ms, rows);
}

struct node_capacity_unit_stat
{
    // timestamp when node perf_counter_info has updated.
    std::string timestamp;
    std::string node_address;
    // mapping: app_id --> (read_cu, write_cu)
    std::map<int32_t, std::pair<int64_t, int64_t>> cu_value_by_app;

    std::string dump_to_json() const
    {
        std::map<int32_t, std::vector<int64_t>> values;
        for (auto &kv : cu_value_by_app) {
            auto &pair = kv.second;
            if (pair.first != 0 || pair.second != 0)
                values.emplace(kv.first, std::vector<int64_t>{pair.first, pair.second});
        }
        std::stringstream out;
        rapidjson::OStreamWrapper wrapper(out);
        dsn::json::JsonWriter writer(wrapper);
        dsn::json::json_encode(writer, values);
        return out.str();
    }
};

inline bool get_capacity_unit_stat(shell_context *sc,
                                   std::vector<node_capacity_unit_stat> &nodes_stat)
{
    std::vector<node_desc> nodes;
    if (!fill_nodes(sc, "replica-server", nodes)) {
        LOG_ERROR("get replica server node list failed");
        return false;
    }

    std::vector<std::pair<bool, std::string>> results =
        call_remote_command(sc, nodes, "perf-counters-by-substr", {".cu@"});

    nodes_stat.resize(nodes.size());
    for (int i = 0; i < nodes.size(); ++i) {
        dsn::perf_counter_info info;
        if (!decode_node_perf_counter_info(nodes[i].hp, results[i], info)) {
            LOG_WARNING("decode perf counter from node({}) failed, just ignore it", nodes[i].hp);
            continue;
        }
        nodes_stat[i].timestamp = info.timestamp_str;
        nodes_stat[i].node_address =
            dsn::dns_resolver::instance().resolve_address(nodes[i].hp).to_string();
        for (dsn::perf_counter_metric &m : info.counters) {
            int32_t app_id, pidx;
            std::string counter_name;
            bool r = parse_app_pegasus_perf_counter_name(m.name, app_id, pidx, counter_name);
            CHECK(r, "name = {}", m.name);
            if (counter_name == "recent.read.cu") {
                nodes_stat[i].cu_value_by_app[app_id].first += (int64_t)m.value;
            } else if (counter_name == "recent.write.cu") {
                nodes_stat[i].cu_value_by_app[app_id].second += (int64_t)m.value;
            }
        }
    }
    return true;
}

struct app_storage_size_stat
{
    // timestamp when this stat is generated.
    std::string timestamp;
    // mapping: app_id --> [app_partition_count, stat_partition_count, storage_size_in_mb]
    std::map<int32_t, std::vector<int64_t>> st_value_by_app;

    std::string dump_to_json() const
    {
        std::stringstream out;
        rapidjson::OStreamWrapper wrapper(out);
        dsn::json::JsonWriter writer(wrapper);
        dsn::json::json_encode(writer, st_value_by_app);
        return out.str();
    }
};

inline bool get_storage_size_stat(shell_context *sc, app_storage_size_stat &st_stat)
{
    std::vector<::dsn::app_info> apps;
    std::vector<node_desc> nodes;
    if (!get_apps_and_nodes(sc, apps, nodes)) {
        LOG_ERROR("get apps and nodes failed");
        return false;
    }

    std::map<int32_t, std::vector<dsn::partition_configuration>> pcs_by_appid;
    if (!get_app_partitions(sc, apps, pcs_by_appid)) {
        LOG_ERROR("get app partitions failed");
        return false;
    }
    for (auto &[_, pcs] : pcs_by_appid) {
        for (auto &pc : pcs) {
            // use partition_flags to record if this partition's storage size is calculated,
            // because `pcs_by_appid' is a temporary variable, so we can re-use partition_flags.
            pc.partition_flags = 0;
        }
    }

    std::vector<std::pair<bool, std::string>> results = call_remote_command(
        sc, nodes, "perf-counters-by-prefix", {"replica*app.pegasus*disk.storage.sst(MB)"});

    for (int i = 0; i < nodes.size(); ++i) {
        dsn::perf_counter_info info;
        if (!decode_node_perf_counter_info(nodes[i].hp, results[i], info)) {
            LOG_WARNING("decode perf counter from node({}) failed, just ignore it", nodes[i].hp);
            continue;
        }
        for (dsn::perf_counter_metric &m : info.counters) {
            int32_t app_id_x, partition_index_x;
            std::string counter_name;
            bool parse_ret = parse_app_pegasus_perf_counter_name(
                m.name, app_id_x, partition_index_x, counter_name);
            CHECK(parse_ret, "name = {}", m.name);
            if (counter_name != "disk.storage.sst(MB)")
                continue;
            auto find = pcs_by_appid.find(app_id_x);
            if (find == pcs_by_appid.end()) // app id not found
                continue;
            auto &pc = find->second[partition_index_x];
            if (pc.hp_primary != nodes[i].hp) // not primary replica
                continue;
            if (pc.partition_flags != 0) // already calculated
                continue;
            pc.partition_flags = 1;
            int64_t app_partition_count = find->second.size();
            auto st_it = st_stat.st_value_by_app
                             .emplace(app_id_x, std::vector<int64_t>{app_partition_count, 0, 0})
                             .first;
            st_it->second[1]++;          // stat_partition_count
            st_it->second[2] += m.value; // storage_size_in_mb
        }
    }

    char buf[20];
    dsn::utils::time_ms_to_date_time(dsn_now_ms(), buf, sizeof(buf));
    st_stat.timestamp = buf;
    return true;
}
