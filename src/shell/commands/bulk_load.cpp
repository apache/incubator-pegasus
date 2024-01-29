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

// IWYU pragma: no_include <bits/getopt_core.h>
#include <boost/cstdint.hpp>
#include <boost/lexical_cast.hpp>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <fmt/core.h>
#include <getopt.h>
#include <stdio.h>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bulk_load_types.h"
#include "client/replication_ddl_client.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/task/task_spec.h"
#include "shell/command_executor.h"
#include "shell/commands.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/output_utils.h"

bool start_bulk_load(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"app_name", required_argument, 0, 'a'},
                                           {"cluster_name", required_argument, 0, 'c'},
                                           {"file_provider_type", required_argument, 0, 'p'},
                                           {"root_path", required_argument, 0, 'r'},
                                           {"ingest_behind", no_argument, 0, 'i'},
                                           {0, 0, 0, 0}};
    std::string app_name;
    std::string cluster_name;
    std::string file_provider_type;
    std::string remote_root_path;
    bool ingest_behind = false;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "a:c:p:r:i", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            app_name = optarg;
            break;
        case 'c':
            cluster_name = optarg;
            break;
        case 'p':
            file_provider_type = optarg;
            break;
        case 'r':
            remote_root_path = optarg;
            break;
        case 'i':
            ingest_behind = true;
            break;
        default:
            return false;
        }
    }

    if (app_name.empty()) {
        fprintf(stderr, "app_name should not be empty\n");
        return false;
    }
    if (cluster_name.empty()) {
        fprintf(stderr, "cluster_name should not be empty\n");
        return false;
    }

    if (file_provider_type.empty()) {
        fprintf(stderr, "file_provider_type should not be empty\n");
        return false;
    }

    if (remote_root_path.empty()) {
        fprintf(stderr, "remote_root_path should not be empty\n");
        return false;
    }

    auto err_resp = sc->ddl_client->start_bulk_load(
        app_name, cluster_name, file_provider_type, remote_root_path, ingest_behind);
    dsn::error_s err = err_resp.get_error();
    std::string hint_msg;
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
        hint_msg = err_resp.get_value().hint_msg;
    }
    if (!err.is_ok()) {
        fmt::print(stderr, "start bulk load failed, error={} [hint:\"{}\"]\n", err, hint_msg);
    } else {
        fmt::print(stdout, "start bulk load succeed\n");
    }

    return true;
}

// helper function for pause/restart bulk load
bool control_bulk_load_helper(command_executor *e,
                              shell_context *sc,
                              arguments args,
                              dsn::replication::bulk_load_control_type::type type)
{
    if (type != dsn::replication::bulk_load_control_type::BLC_PAUSE &&
        type != dsn::replication::bulk_load_control_type::BLC_RESTART) {
        return false;
    }

    static struct option long_options[] = {{"app_name", required_argument, 0, 'a'}, {0, 0, 0, 0}};
    std::string app_name;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "a:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            app_name = optarg;
            break;
        default:
            return false;
        }
    }

    if (app_name.empty()) {
        fprintf(stderr, "app_name should not be empty\n");
        return false;
    }

    auto err_resp = sc->ddl_client->control_bulk_load(app_name, type);
    dsn::error_s err = err_resp.get_error();
    std::string hint_msg;
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
        hint_msg = err_resp.get_value().hint_msg;
    }
    std::string type_str =
        type == dsn::replication::bulk_load_control_type::BLC_PAUSE ? "pause" : "restart";
    if (!err.is_ok()) {
        fmt::print(
            stderr, "{} bulk load failed, error={} [hint:\"{}\"]\n", type_str, err, hint_msg);
    } else {
        fmt::print(stdout, "{} bulk load succeed\n", type_str);
    }

    return true;
}

bool pause_bulk_load(command_executor *e, shell_context *sc, arguments args)
{
    return control_bulk_load_helper(
        e, sc, args, dsn::replication::bulk_load_control_type::BLC_PAUSE);
}

bool restart_bulk_load(command_executor *e, shell_context *sc, arguments args)
{
    return control_bulk_load_helper(
        e, sc, args, dsn::replication::bulk_load_control_type::BLC_RESTART);
}

bool cancel_bulk_load(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {
        {"app_name", required_argument, 0, 'a'}, {"forced", no_argument, 0, 'f'}, {0, 0, 0, 0}};
    std::string app_name;
    bool forced = false;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "a:f", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            app_name = optarg;
            break;
        case 'f':
            forced = true;
            break;
        default:
            return false;
        }
    }

    if (app_name.empty()) {
        fprintf(stderr, "app_name should not be empty\n");
        return false;
    }

    auto type = forced ? dsn::replication::bulk_load_control_type::BLC_FORCE_CANCEL
                       : dsn::replication::bulk_load_control_type::BLC_CANCEL;
    auto err_resp = sc->ddl_client->control_bulk_load(app_name, type);
    dsn::error_s err = err_resp.get_error();
    std::string hint_msg;
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
        hint_msg = err_resp.get_value().hint_msg;
    }
    if (!err.is_ok()) {
        fmt::print(stderr, "cancel bulk load failed, error={} [hint:\"{}\"]\n", err, hint_msg);
        if (err.code() == dsn::ERR_INVALID_STATE &&
            type == dsn::replication::bulk_load_control_type::BLC_CANCEL) {
            fmt::print(stderr, "you can force cancel bulk load by using \"-f\"\n");
        }
    } else {
        fmt::print(stdout, "cancel bulk load succeed\n");
    }

    return true;
}

// get short status name of bulk_load_status and ingestion_status
template <typename T>
static std::string get_short_status(T status)
{
    static_assert(std::is_same<T, dsn::replication::bulk_load_status::type>::value ||
                      std::is_same<T, dsn::replication::ingestion_status::type>::value,
                  "the given type is not bulk_load_status or ingestion_status");

    std::string str = dsn::enum_to_string(status);
    auto index = str.find_last_of(":");
    return str.substr(index + 1);
}

bool query_bulk_load_status(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"app_name", required_argument, 0, 'a'},
                                           {"partition_index", required_argument, 0, 'i'},
                                           {"detailed", no_argument, 0, 'd'},
                                           {0, 0, 0, 0}};

    std::string app_name;
    int32_t pidx = -1;
    bool detailed = false;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "a:i:d", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            app_name = optarg;
            break;
        case 'i':
            pidx = boost::lexical_cast<int32_t>(optarg);
            break;
        case 'd':
            detailed = true;
            break;
        default:
            return false;
        }
    }

    if (app_name.empty()) {
        fprintf(stderr, "app_name should not be empty\n");
        return false;
    }

    auto err_resp = sc->ddl_client->query_bulk_load(app_name);
    dsn::error_s err = err_resp.get_error();
    if (!err.is_ok()) {
        fmt::print(stderr, "query bulk load failed, error={}\n", err);
        return true;
    }
    auto resp = err_resp.get_value();

    std::string hint_msg;
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
        hint_msg = resp.hint_msg;
    }
    if (!err.is_ok()) {
        fmt::print(stderr, "query bulk load failed, error={} [hint:\"{}\"]\n", err, hint_msg);
        return true;
    }

    int partition_count = resp.partitions_status.size();
    if (pidx < -1 || pidx >= partition_count) {
        fmt::print(stderr,
                   "query bulk load failed, error={} [hint:\"invalid partition index\"]\n",
                   dsn::ERR_INVALID_PARAMETERS);
        return true;
    }

    // print query result
    dsn::utils::multi_table_printer mtp;

    bool all_partitions = (pidx == -1);
    bool print_ingestion_progress = (resp.app_status == bulk_load_status::BLS_INGESTING);
    bool print_download_progress = (resp.app_status == bulk_load_status::BLS_DOWNLOADING);

    std::unordered_map<int32_t, int32_t> partitions_progress;
    auto total_download_progress = 0, total_ingestion_progress = 0;
    if (print_download_progress) {
        for (auto i = 0; i < partition_count; ++i) {
            auto progress = 0;
            for (const auto &kv : resp.bulk_load_states[i]) {
                progress += kv.second.download_progress;
            }
            progress /= resp.max_replica_count;
            partitions_progress.insert(std::make_pair(i, progress));
            total_download_progress += progress;
        }
        total_download_progress /= partition_count;
    }

    // print all partitions
    if (detailed && all_partitions) {
        bool print_cleanup_flag = (resp.app_status == bulk_load_status::BLS_CANCELED ||
                                   resp.app_status == bulk_load_status::BLS_FAILED ||
                                   resp.app_status == bulk_load_status::BLS_SUCCEED);
        dsn::utils::table_printer tp_all("all partitions");
        tp_all.add_title("partition_index");
        tp_all.add_column("partition_status");
        if (print_download_progress) {
            tp_all.add_column("download_progress(%)");
        }
        if (print_cleanup_flag) {
            tp_all.add_column("is_cleaned_up");
        }

        for (auto i = 0; i < partition_count; ++i) {
            auto states = resp.bulk_load_states[i];
            tp_all.add_row(i);
            tp_all.append_data(get_short_status(resp.partitions_status[i]));
            if (print_download_progress) {
                tp_all.append_data(partitions_progress[i]);
            }
            if (print_ingestion_progress &&
                resp.partitions_status[i] == bulk_load_status::BLS_SUCCEED) {
                total_ingestion_progress += 1;
            }
            if (print_cleanup_flag) {
                bool is_cleanup = (states.size() == resp.max_replica_count);
                for (const auto &kv : states) {
                    is_cleanup = is_cleanup && kv.second.is_cleaned_up;
                }
                tp_all.append_data(is_cleanup ? "YES" : "NO");
            }
        }
        mtp.add(std::move(tp_all));
    }

    // print specific partition
    if (detailed && !all_partitions) {
        auto pstatus = resp.partitions_status[pidx];
        bool no_detailed =
            (pstatus == bulk_load_status::BLS_INVALID || pstatus == bulk_load_status::BLS_PAUSED ||
             pstatus == bulk_load_status::BLS_DOWNLOADED);
        if (!no_detailed) {
            bool p_prgress = (pstatus == bulk_load_status::BLS_DOWNLOADING);
            bool p_istatus = (pstatus == bulk_load_status::BLS_INGESTING);
            bool p_cleanup_flag = (pstatus == bulk_load_status::BLS_SUCCEED ||
                                   pstatus == bulk_load_status::BLS_CANCELED ||
                                   pstatus == bulk_load_status::BLS_FAILED);
            bool p_pause_flag = (pstatus == bulk_load_status::BLS_PAUSING);

            dsn::utils::table_printer tp_single("single partition");
            tp_single.add_title("partition_index");
            tp_single.add_column("node_address");
            if (p_prgress) {
                tp_single.add_column("download_progress(%)");
            }
            if (p_istatus) {
                tp_single.add_column("ingestion_status");
            }
            if (p_cleanup_flag) {
                tp_single.add_column("is_cleaned_up");
            }
            if (p_pause_flag) {
                tp_single.add_column("is_paused");
            }

            auto states = resp.bulk_load_states[pidx];
            for (auto iter = states.begin(); iter != states.end(); ++iter) {
                tp_single.add_row(pidx);
                tp_single.append_data(iter->first.to_string());
                if (p_prgress) {
                    tp_single.append_data(iter->second.download_progress);
                }
                if (p_istatus) {
                    tp_single.append_data(get_short_status(iter->second.ingest_status));
                }
                if (p_cleanup_flag) {
                    tp_single.append_data(iter->second.is_cleaned_up ? "YES" : "NO");
                }
                if (p_pause_flag) {
                    tp_single.append_data(iter->second.is_paused ? "YES" : "NO");
                }
            }
            mtp.add(std::move(tp_single));
        }
    }

    dsn::utils::table_printer tp_summary("summary");
    if (!all_partitions) {
        tp_summary.add_row_name_and_data("partition_bulk_load_status",
                                         get_short_status(resp.partitions_status[pidx]));
    }
    bool is_bulk_loading = resp.__isset.is_bulk_loading ? resp.is_bulk_loading : false;
    tp_summary.add_row_name_and_data("is_bulk_loading", is_bulk_loading ? "YES" : "NO");
    tp_summary.add_row_name_and_data("app_bulk_load_status", get_short_status(resp.app_status));
    if (bulk_load_status::BLS_FAILED == resp.app_status) {
        tp_summary.add_row_name_and_data("bulk_load_err", resp.err.to_string());
    }
    if (print_download_progress) {
        tp_summary.add_row_name_and_data("app_total_download_progress", total_download_progress);
    }
    if (print_ingestion_progress) {
        tp_summary.add_row_name_and_data("app_total_ingestion_progress",
                                         total_ingestion_progress * 100 / partition_count);
    }
    mtp.add(std::move(tp_summary));
    mtp.output(std::cout, tp_output_format::kTabular);

    return true;
}

bool clear_bulk_load(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"app_name", required_argument, 0, 'a'}, {0, 0, 0, 0}};
    std::string app_name;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "a:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            app_name = optarg;
            break;
        default:
            return false;
        }
    }

    auto err_resp = sc->ddl_client->clear_bulk_load(app_name);
    dsn::error_s err = err_resp.get_error();
    std::string hint_msg;
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
        hint_msg = err_resp.get_value().hint_msg;
    }
    if (!err.is_ok()) {
        fmt::print(stderr, "clear bulk load failed, error={} [hint:\"{}\"]\n", err, hint_msg);
    } else {
        fmt::print(stdout, "{}\n", hint_msg);
    }

    return true;
}
