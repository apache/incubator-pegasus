// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <fmt/core.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "base/meta_store.h"
#include "common/gpid.h"
#include "dsn.layer2_types.h"
#include "replica/replica_stub.h"
#include "replica/replication_app_base.h"
#include "shell/argh.h"
#include "shell/command_executor.h"
#include "shell/commands.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/load_dump_object.h"

const std::string local_replica_check_help = "<replica_dir> [--verify_sst] [--verify_checksum]";

namespace {

enum class check_severity
{
    kPass,
    kWarn,
    kFail
};

struct check_context
{
    bool verify_sst = false;
    bool verify_checksum = false;
    bool has_failure = false;
    bool has_warning = false;
};

void print_result(check_context *ctx, check_severity sev, const std::string &message)
{
    switch (sev) {
    case check_severity::kPass:
        fmt::print(stdout, "[PASS] {}\n", message);
        break;
    case check_severity::kWarn:
        ctx->has_warning = true;
        fmt::print(stdout, "[WARN] {}\n", message);
        break;
    case check_severity::kFail:
        ctx->has_failure = true;
        fmt::print(stdout, "[FAIL] {}\n", message);
        break;
    }
}

bool has_prefix(const std::string &name, const std::string &prefix)
{
    return name.size() >= prefix.size() && name.compare(0, prefix.size(), prefix) == 0;
}

bool has_suffix(const std::string &name, const std::string &suffix)
{
    return name.size() >= suffix.size() &&
           name.compare(name.size() - suffix.size(), suffix.size(), suffix) == 0;
}

void check_required_files(const std::string &replica_dir,
                          const std::string &rdb_dir,
                          check_context *ctx)
{
    const auto app_info_path = dsn::utils::filesystem::path_combine(
        replica_dir, dsn::replication::replica_app_info::kAppInfo);
    const auto init_info_path = dsn::utils::filesystem::path_combine(
        replica_dir, dsn::replication::replica_init_info::kInitInfo);

    print_result(ctx,
                 dsn::utils::filesystem::directory_exists(replica_dir) ? check_severity::kPass
                                                                       : check_severity::kFail,
                 fmt::format("replica dir {}", replica_dir));
    print_result(ctx,
                 dsn::utils::filesystem::file_exists(app_info_path) ? check_severity::kPass
                                                                    : check_severity::kFail,
                 fmt::format("found {}", app_info_path));
    print_result(ctx,
                 dsn::utils::filesystem::file_exists(init_info_path) ? check_severity::kPass
                                                                     : check_severity::kFail,
                 fmt::format("found {}", init_info_path));
    print_result(ctx,
                 dsn::utils::filesystem::directory_exists(rdb_dir) ? check_severity::kPass
                                                                   : check_severity::kFail,
                 fmt::format("found rocksdb dir {}", rdb_dir));
}

bool check_replica_metadata(const std::string &replica_dir, check_context *ctx, dsn::gpid *pid)
{
    dsn::app_info ai;
    std::string hint_message;
    if (!dsn::replication::replica_stub::validate_replica_dir(
            replica_dir, ai, *pid, hint_message)) {
        print_result(ctx, check_severity::kFail, hint_message);
        return false;
    }

    print_result(ctx,
                 check_severity::kPass,
                 fmt::format("replica dir parsed: gpid={}, app_name='{}', app_type='{}', "
                             "partition_count={}, app_id={}",
                             pid->to_string(),
                             ai.app_name,
                             ai.app_type,
                             ai.partition_count,
                             ai.app_id));

    dsn::replication::replica_init_info init_info;
    const auto init_info_path = dsn::utils::filesystem::path_combine(
        replica_dir, dsn::replication::replica_init_info::kInitInfo);
    auto err = dsn::utils::load_rjobj_from_file(init_info_path, &init_info);
    if (err != dsn::ERR_OK) {
        print_result(ctx,
                     check_severity::kFail,
                     fmt::format("load '{}' failed: {}", init_info_path, err.to_string()));
        return false;
    }

    print_result(ctx,
                 check_severity::kPass,
                 fmt::format("init-info loaded: init_ballot={}, init_durable_decree={}",
                             init_info.init_ballot,
                             init_info.init_durable_decree));
    return true;
}

bool open_readonly_db(const std::string &rdb_dir,
                      rocksdb::DB **db,
                      std::vector<rocksdb::ColumnFamilyHandle *> *cf_handles,
                      std::string *error)
{
    std::vector<std::string> column_families;
    auto status = rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(), rdb_dir, &column_families);
    if (!status.ok()) {
        *error = fmt::format("ListColumnFamilies failed: {}", status.ToString());
        return false;
    }

    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
    cf_descs.reserve(column_families.size());
    for (const auto &cf : column_families) {
        cf_descs.emplace_back(cf, rocksdb::ColumnFamilyOptions());
    }

    status = rocksdb::DB::OpenForReadOnly(rocksdb::DBOptions(), rdb_dir, cf_descs, cf_handles, db);
    if (!status.ok()) {
        *error = fmt::format("OpenForReadOnly failed: {}", status.ToString());
        return false;
    }
    return true;
}

void close_db(rocksdb::DB **db, std::vector<rocksdb::ColumnFamilyHandle *> *cf_handles)
{
    for (auto *cf : *cf_handles) {
        delete cf;
    }
    cf_handles->clear();
    delete *db;
    *db = nullptr;
}

void check_rdb_files(const std::string &rdb_dir,
                     check_context *ctx,
                     std::vector<std::string> *sst_files)
{
    std::vector<std::string> files;
    if (!dsn::utils::filesystem::get_subfiles(rdb_dir, files, false)) {
        print_result(
            ctx, check_severity::kFail, fmt::format("list files under '{}' failed", rdb_dir));
        return;
    }

    int current_count = 0;
    int manifest_count = 0;
    int options_count = 0;
    for (const auto &file : files) {
        const auto name = dsn::utils::filesystem::get_file_name(file);
        if (name == "CURRENT") {
            ++current_count;
        } else if (has_prefix(name, "MANIFEST-")) {
            ++manifest_count;
        } else if (has_prefix(name, "OPTIONS-")) {
            ++options_count;
        } else if (has_suffix(name, ".sst")) {
            sst_files->push_back(file);
        }
    }

    print_result(ctx,
                 current_count > 0 ? check_severity::kPass : check_severity::kFail,
                 fmt::format("CURRENT count = {}", current_count));
    print_result(ctx,
                 manifest_count > 0 ? check_severity::kPass : check_severity::kFail,
                 fmt::format("MANIFEST count = {}", manifest_count));
    print_result(ctx,
                 options_count > 0 ? check_severity::kPass : check_severity::kWarn,
                 fmt::format("OPTIONS count = {}", options_count));
    print_result(ctx,
                 !sst_files->empty() ? check_severity::kPass : check_severity::kWarn,
                 fmt::format("sst file count = {}", sst_files->size()));
}

void check_column_families(const std::vector<rocksdb::ColumnFamilyHandle *> &cf_handles,
                           check_context *ctx,
                           rocksdb::ColumnFamilyHandle **meta_cf)
{
    std::set<std::string> names;
    for (auto *cf : cf_handles) {
        names.insert(cf->GetName());
    }
    print_result(
        ctx,
        names.count(pegasus::server::meta_store::DATA_COLUMN_FAMILY_NAME) == 1
            ? check_severity::kPass
            : check_severity::kFail,
        fmt::format("column family '{}'", pegasus::server::meta_store::DATA_COLUMN_FAMILY_NAME));
    print_result(
        ctx,
        names.count(pegasus::server::meta_store::META_COLUMN_FAMILY_NAME) == 1
            ? check_severity::kPass
            : check_severity::kFail,
        fmt::format("column family '{}'", pegasus::server::meta_store::META_COLUMN_FAMILY_NAME));

    *meta_cf = nullptr;
    for (auto *cf : cf_handles) {
        if (cf->GetName() == pegasus::server::meta_store::META_COLUMN_FAMILY_NAME) {
            *meta_cf = cf;
            break;
        }
    }
}

void check_meta_cf(rocksdb::DB *db, rocksdb::ColumnFamilyHandle *meta_cf, check_context *ctx)
{
    if (meta_cf == nullptr) {
        print_result(ctx, check_severity::kFail, "meta column family handle not found");
        return;
    }

    pegasus::server::meta_store ms("local_replica_check", db, meta_cf);

    uint64_t decree = 0;
    auto err = ms.get_last_flushed_decree(&decree);
    print_result(ctx,
                 err == dsn::ERR_OK ? check_severity::kPass : check_severity::kFail,
                 err == dsn::ERR_OK
                     ? fmt::format("pegasus_last_flushed_decree={}", decree)
                     : fmt::format("read pegasus_last_flushed_decree failed: {}", err.to_string()));

    uint32_t data_version = 0;
    err = ms.get_data_version(&data_version);
    print_result(ctx,
                 err == dsn::ERR_OK ? check_severity::kPass : check_severity::kFail,
                 err == dsn::ERR_OK
                     ? fmt::format("pegasus_data_version={}", data_version)
                     : fmt::format("read pegasus_data_version failed: {}", err.to_string()));

    uint64_t last_manual_compact_finish_time = 0;
    err = ms.get_last_manual_compact_finish_time(&last_manual_compact_finish_time);
    print_result(ctx,
                 err == dsn::ERR_OK ? check_severity::kPass : check_severity::kFail,
                 err == dsn::ERR_OK
                     ? fmt::format("pegasus_last_manual_compact_finish_time={}",
                                   last_manual_compact_finish_time)
                     : fmt::format("read pegasus_last_manual_compact_finish_time failed: {}",
                                   err.to_string()));

    std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator({}, meta_cf));
    uint64_t meta_kv_count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ++meta_kv_count;
    }
    print_result(ctx,
                 iter->status().ok() ? check_severity::kPass : check_severity::kWarn,
                 iter->status().ok() ? fmt::format("meta_cf kv count={}", meta_kv_count)
                                     : fmt::format("iterate meta_cf ended with error: {}",
                                                   iter->status().ToString()));
}

void verify_sst_files(const std::vector<std::string> &sst_files, check_context *ctx)
{
    uint64_t unreadable_count = 0;
    for (const auto &sst_file : sst_files) {
        rocksdb::SstFileReader reader{rocksdb::Options()};
        auto status = reader.Open(sst_file);
        if (!status.ok()) {
            ++unreadable_count;
            print_result(ctx,
                         check_severity::kWarn,
                         fmt::format("sst open failed '{}': {}", sst_file, status.ToString()));
            continue;
        }

        if (ctx->verify_checksum) {
            status = reader.VerifyChecksum();
            if (!status.ok()) {
                ++unreadable_count;
                print_result(
                    ctx,
                    check_severity::kWarn,
                    fmt::format("sst checksum failed '{}': {}", sst_file, status.ToString()));
            }
        }
    }

    print_result(ctx,
                 unreadable_count == 0 ? check_severity::kPass : check_severity::kWarn,
                 fmt::format("sst verification finished: total={}, problematic={}",
                             sst_files.size(),
                             unreadable_count));
}

} // namespace

bool local_replica_check(command_executor *e, shell_context *sc, arguments args)
{
    argh::parser cmd(args.argc, args.argv);
    if (cmd.pos_args().size() < 1) {
        return false;
    }

    const std::string replica_dir = cmd(1).str();
    check_context ctx;
    ctx.verify_sst = cmd["--verify_sst"];
    ctx.verify_checksum = cmd["--verify_checksum"];
    if (ctx.verify_checksum) {
        ctx.verify_sst = true;
    }

    const auto rdb_dir = dsn::utils::filesystem::path_combine(
        dsn::utils::filesystem::path_combine(replica_dir,
                                             dsn::replication::replication_app_base::kDataDir),
        dsn::replication::replication_app_base::kRdbDir);

    fmt::print(stdout, "Checking replica '{}'\n", replica_dir);
    check_required_files(replica_dir, rdb_dir, &ctx);

    dsn::gpid pid;
    check_replica_metadata(replica_dir, &ctx, &pid);

    std::vector<std::string> sst_files;
    check_rdb_files(rdb_dir, &ctx, &sst_files);

    rocksdb::DB *db = nullptr;
    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
    std::string open_error;
    if (!open_readonly_db(rdb_dir, &db, &cf_handles, &open_error)) {
        print_result(&ctx, check_severity::kFail, open_error);
    } else {
        print_result(&ctx, check_severity::kPass, "rocksdb OpenForReadOnly succeeded");
        rocksdb::ColumnFamilyHandle *meta_cf = nullptr;
        check_column_families(cf_handles, &ctx, &meta_cf);
        check_meta_cf(db, meta_cf, &ctx);
        close_db(&db, &cf_handles);
    }

    if (ctx.verify_sst) {
        verify_sst_files(sst_files, &ctx);
    }

    fmt::print(stdout,
               "Summary: {}\n",
               ctx.has_failure ? "FAIL" : (ctx.has_warning ? "PASS_WITH_WARNINGS" : "PASS"));
    return true;
}
