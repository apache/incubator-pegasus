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
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <rocksdb/table_properties.h>
#include <rocksdb/write_batch.h>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/meta_store.h"
#include "common/gpid.h"
#include "dsn.layer2_types.h"
#include "pegasus_value_schema.h"
#include "replica/replica_stub.h"
#include "replica/replication_app_base.h"
#include "shell/argh.h"
#include "shell/command_executor.h"
#include "shell/commands.h"
#include "utils/defer.h"
#include "utils/endians.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/load_dump_object.h"
#include "utils/string_conv.h"

const std::string local_replica_recover_help =
    "<src_replica_dir> <dst_replica_dir> [--force_sst_scan] [--verify_checksum] "
    "[--write_batch_count num] [--pegasus_data_version num] [--last_flushed_decree num] "
    "[--last_manual_compact_finish_time num]";

namespace {

const std::string kPegasusDataVersion = "pegasus_data_version";
const std::string kPegasusLastFlushedDecree = "pegasus_last_flushed_decree";
const std::string kPegasusLastManualCompactFinishTime = "pegasus_last_manual_compact_finish_time";

struct recover_options
{
    std::string src_replica_dir;
    std::string dst_replica_dir;
    std::string src_rdb_dir;
    std::string dst_rdb_dir;
    std::string sst_report_file;
    bool force_sst_scan = false;
    bool verify_checksum = false;
    uint64_t write_batch_count = 1000;
    bool has_data_version_override = false;
    uint32_t data_version_override = 0;
    bool has_last_flushed_decree_override = false;
    uint64_t last_flushed_decree_override = 0;
    bool has_last_manual_compact_finish_time_override = false;
    uint64_t last_manual_compact_finish_time_override = 0;
};

struct recovered_metadata
{
    std::map<std::string, std::string> meta_kvs;
    bool has_data_version = false;
    uint32_t data_version = 0;
    bool has_last_flushed_decree = false;
    uint64_t last_flushed_decree = 0;
    bool has_last_manual_compact_finish_time = false;
    uint64_t last_manual_compact_finish_time = 0;
};

struct recovery_stats
{
    uint64_t data_kv_count = 0;
    uint64_t meta_kv_count = 0;
    uint64_t scanned_sst_count = 0;
    uint64_t skipped_bad_sst_count = 0;
};

struct sst_recovery_record
{
    std::string path;
    std::string column_family_name;
    uint64_t data_kv_count = 0;
    uint64_t meta_kv_count = 0;
    uint64_t invalid_key_count = 0;
    bool skipped = false;
    bool partial = false;
    std::string skip_reason;
    std::string partial_reason;
};

struct sst_recovery_report
{
    bool used_sst_scan = false;
    bool completed = false;
    std::string source_rdb_dir;
    std::vector<sst_recovery_record> records;
};

bool parse_u64(const std::string &value, uint64_t *result)
{
    return dsn::buf2uint64(value, *result);
}

bool parse_u32(const std::string &value, uint32_t *result)
{
    uint64_t tmp = 0;
    if (!parse_u64(value, &tmp) || tmp > UINT32_MAX) {
        return false;
    }
    *result = static_cast<uint32_t>(tmp);
    return true;
}

void refresh_required_metadata(recovered_metadata *meta)
{
    auto refresh_u64 = [meta](const std::string &key, bool *has_value, uint64_t *value) {
        const auto it = meta->meta_kvs.find(key);
        if (it == meta->meta_kvs.end()) {
            return;
        }
        uint64_t parsed = 0;
        if (parse_u64(it->second, &parsed)) {
            *has_value = true;
            *value = parsed;
        }
    };

    refresh_u64(
        kPegasusLastFlushedDecree, &meta->has_last_flushed_decree, &meta->last_flushed_decree);
    refresh_u64(kPegasusLastManualCompactFinishTime,
                &meta->has_last_manual_compact_finish_time,
                &meta->last_manual_compact_finish_time);

    const auto it = meta->meta_kvs.find(kPegasusDataVersion);
    if (it != meta->meta_kvs.end()) {
        uint32_t parsed = 0;
        if (parse_u32(it->second, &parsed)) {
            meta->has_data_version = true;
            meta->data_version = parsed;
        }
    }
}

uint64_t extract_sst_number(const std::string &filename)
{
    unsigned long long file_number = 0;
    const auto basename = dsn::utils::filesystem::get_file_name(filename);
    if (sscanf(basename.c_str(), "%llu.sst", &file_number) != 1) {
        return UINT64_MAX;
    }
    return static_cast<uint64_t>(file_number);
}

bool is_data_key(const rocksdb::Slice &key)
{
    if (key.empty() || key.size() < sizeof(uint16_t)) {
        return false;
    }

    uint16_t hash_key_len = 0;
    memcpy(&hash_key_len, key.data(), sizeof(uint16_t));
    hash_key_len = dsn::endian::ntoh(hash_key_len);
    return key.size() >= sizeof(uint16_t) + hash_key_len;
}

bool open_rocksdb(const rocksdb::DBOptions &db_opts,
                  const std::string &rdb_dir,
                  bool read_only,
                  const std::vector<rocksdb::ColumnFamilyDescriptor> &cf_descs,
                  std::vector<rocksdb::ColumnFamilyHandle *> *cf_handles,
                  rocksdb::DB **db,
                  std::string *err)
{
    CHECK_NOTNULL(cf_handles, "");
    CHECK_NOTNULL(db, "");
    rocksdb::Status status;
    if (read_only) {
        status = rocksdb::DB::OpenForReadOnly(db_opts, rdb_dir, cf_descs, cf_handles, db);
    } else {
        status = rocksdb::DB::Open(db_opts, rdb_dir, cf_descs, cf_handles, db);
    }

    if (!status.ok()) {
        if (err != nullptr) {
            *err = status.ToString();
        }
        return false;
    }

    if (cf_handles->size() != 2 ||
        (*cf_handles)[0]->GetName() != pegasus::server::meta_store::DATA_COLUMN_FAMILY_NAME ||
        (*cf_handles)[1]->GetName() != pegasus::server::meta_store::META_COLUMN_FAMILY_NAME) {
        if (err != nullptr) {
            *err = "unexpected column families";
        }
        return false;
    }
    return true;
}

void release_db(std::vector<rocksdb::ColumnFamilyHandle *> *cf_handles, rocksdb::DB **db)
{
    CHECK_NOTNULL(cf_handles, "");
    CHECK_NOTNULL(db, "");
    for (auto *cf_handle : *cf_handles) {
        delete cf_handle;
    }
    cf_handles->clear();
    delete *db;
    *db = nullptr;
}

bool write_batch_put(rocksdb::DB *db,
                     rocksdb::ColumnFamilyHandle *cf,
                     rocksdb::WriteBatch *batch,
                     rocksdb::WriteOptions *write_options,
                     const rocksdb::Slice &key,
                     const rocksdb::Slice &value,
                     uint64_t write_batch_count,
                     uint64_t *pending_count,
                     std::string *err)
{
    auto status = batch->Put(cf, key, value);
    if (!status.ok()) {
        if (err != nullptr) {
            *err = status.ToString();
        }
        return false;
    }

    ++(*pending_count);
    if (*pending_count < write_batch_count) {
        return true;
    }

    status = db->Write(*write_options, batch);
    if (!status.ok()) {
        if (err != nullptr) {
            *err = status.ToString();
        }
        return false;
    }
    batch->Clear();
    *pending_count = 0;
    return true;
}

bool flush_write_batch(rocksdb::DB *db,
                       rocksdb::WriteBatch *batch,
                       rocksdb::WriteOptions *write_options,
                       uint64_t *pending_count,
                       std::string *err)
{
    if (*pending_count == 0) {
        return true;
    }
    auto status = db->Write(*write_options, batch);
    if (!status.ok()) {
        if (err != nullptr) {
            *err = status.ToString();
        }
        return false;
    }
    batch->Clear();
    *pending_count = 0;
    return true;
}

bool copy_meta_cf(rocksdb::DB *src_db,
                  rocksdb::ColumnFamilyHandle *src_meta_cf,
                  rocksdb::DB *dst_db,
                  rocksdb::ColumnFamilyHandle *dst_meta_cf,
                  const recover_options &opts,
                  recovered_metadata *meta,
                  recovery_stats *stats)
{
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions write_options;
    write_options.disableWAL = true;
    uint64_t pending_count = 0;
    std::string err;
    std::unique_ptr<rocksdb::Iterator> iter(src_db->NewIterator({}, src_meta_cf));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        if (!write_batch_put(dst_db,
                             dst_meta_cf,
                             &batch,
                             &write_options,
                             iter->key(),
                             iter->value(),
                             opts.write_batch_count,
                             &pending_count,
                             &err)) {
            fmt::print(stderr, "ERROR: failed to copy meta cf: {}\n", err);
            return false;
        }
        meta->meta_kvs[iter->key().ToString()] = iter->value().ToString();
        ++stats->meta_kv_count;
    }
    if (!iter->status().ok()) {
        fmt::print(stderr, "ERROR: iterate meta cf failed: {}\n", iter->status().ToString());
        return false;
    }
    if (!flush_write_batch(dst_db, &batch, &write_options, &pending_count, &err)) {
        fmt::print(stderr, "ERROR: failed to flush meta cf batch: {}\n", err);
        return false;
    }
    refresh_required_metadata(meta);
    return true;
}

bool recover_from_db_view(const recover_options &opts,
                          rocksdb::DB *dst_db,
                          rocksdb::ColumnFamilyHandle *dst_data_cf,
                          rocksdb::ColumnFamilyHandle *dst_meta_cf,
                          recovered_metadata *meta,
                          recovery_stats *stats,
                          bool *should_fallback)
{
    *should_fallback = false;
    const std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs(
        {{pegasus::server::meta_store::DATA_COLUMN_FAMILY_NAME, {}},
         {pegasus::server::meta_store::META_COLUMN_FAMILY_NAME, {}}});
    std::vector<rocksdb::ColumnFamilyHandle *> src_cf_handles;
    rocksdb::DB *src_db = nullptr;
    std::string err;
    if (!open_rocksdb({}, opts.src_rdb_dir, true, cf_descs, &src_cf_handles, &src_db, &err)) {
        *should_fallback = true;
        fmt::print(stdout, "WARNING: open source rocksdb failed, fallback to sst scan: {}\n", err);
        return false;
    }

    auto cleanup =
        dsn::defer([&src_cf_handles, &src_db]() { release_db(&src_cf_handles, &src_db); });

    if (!copy_meta_cf(src_db, src_cf_handles[1], dst_db, dst_meta_cf, opts, meta, stats)) {
        return false;
    }

    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions write_options;
    write_options.disableWAL = true;
    uint64_t pending_count = 0;
    std::unique_ptr<rocksdb::Iterator> iter(src_db->NewIterator({}, src_cf_handles[0]));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        if (iter->key().empty()) {
            continue;
        }
        if (!write_batch_put(dst_db,
                             dst_data_cf,
                             &batch,
                             &write_options,
                             iter->key(),
                             iter->value(),
                             opts.write_batch_count,
                             &pending_count,
                             &err)) {
            fmt::print(stderr, "ERROR: failed to copy data kv from db view: {}\n", err);
            return false;
        }
        ++stats->data_kv_count;
    }
    if (!iter->status().ok()) {
        fmt::print(stderr, "ERROR: iterate source db failed: {}\n", iter->status().ToString());
        return false;
    }
    if (!flush_write_batch(dst_db, &batch, &write_options, &pending_count, &err)) {
        fmt::print(stderr, "ERROR: failed to flush data batch: {}\n", err);
        return false;
    }
    return true;
}

bool recover_from_sst_scan(const recover_options &opts,
                           rocksdb::DB *dst_db,
                           rocksdb::ColumnFamilyHandle *dst_data_cf,
                           rocksdb::ColumnFamilyHandle *dst_meta_cf,
                           recovered_metadata *meta,
                           recovery_stats *stats,
                           sst_recovery_report *report)
{
    report->used_sst_scan = true;
    report->source_rdb_dir = opts.src_rdb_dir;
    std::vector<std::string> sst_files;
    if (!dsn::utils::filesystem::get_subfiles(opts.src_rdb_dir, sst_files, false)) {
        fmt::print(stderr, "ERROR: get sub-files from '{}' failed\n", opts.src_rdb_dir);
        return false;
    }

    std::vector<std::string> ordered_sst_files;
    ordered_sst_files.reserve(sst_files.size());
    for (const auto &file : sst_files) {
        if (file.size() >= 4 && file.substr(file.size() - 4) == ".sst") {
            ordered_sst_files.push_back(file);
        }
    }
    std::sort(ordered_sst_files.begin(),
              ordered_sst_files.end(),
              [](const std::string &lhs, const std::string &rhs) {
                  return extract_sst_number(lhs) < extract_sst_number(rhs);
              });

    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions write_options;
    write_options.disableWAL = true;
    uint64_t pending_count = 0;
    std::string err;
    for (const auto &sst_file : ordered_sst_files) {
        ++stats->scanned_sst_count;
        const auto &full_path = sst_file;
        report->records.emplace_back();
        auto &record = report->records.back();
        record.path = full_path;
        rocksdb::SstFileReader reader{rocksdb::Options()};
        auto status = reader.Open(full_path);
        if (!status.ok()) {
            ++stats->skipped_bad_sst_count;
            record.skipped = true;
            record.skip_reason = status.ToString();
            fmt::print(
                stdout, "WARNING: skip unreadable sst '{}': {}\n", full_path, status.ToString());
            continue;
        }
        if (opts.verify_checksum) {
            status = reader.VerifyChecksum();
            if (!status.ok()) {
                ++stats->skipped_bad_sst_count;
                record.skipped = true;
                record.skip_reason = fmt::format("checksum failed: {}", status.ToString());
                fmt::print(stdout,
                           "WARNING: skip checksum-failed sst '{}': {}\n",
                           full_path,
                           status.ToString());
                continue;
            }
        }

        const auto table_properties = reader.GetTableProperties();
        const auto &cf_name = table_properties->column_family_name;
        record.column_family_name = cf_name;
        std::unique_ptr<rocksdb::Iterator> iter(reader.NewIterator({}));
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            if (cf_name == pegasus::server::meta_store::META_COLUMN_FAMILY_NAME) {
                if (!write_batch_put(dst_db,
                                     dst_meta_cf,
                                     &batch,
                                     &write_options,
                                     iter->key(),
                                     iter->value(),
                                     opts.write_batch_count,
                                     &pending_count,
                                     &err)) {
                    fmt::print(stderr, "ERROR: failed to write meta kv from sst: {}\n", err);
                    return false;
                }
                meta->meta_kvs[iter->key().ToString()] = iter->value().ToString();
                ++stats->meta_kv_count;
                ++record.meta_kv_count;
                continue;
            }

            if (cf_name != pegasus::server::meta_store::DATA_COLUMN_FAMILY_NAME) {
                continue;
            }

            if (iter->key().empty()) {
                continue;
            }
            if (!is_data_key(iter->key())) {
                ++record.invalid_key_count;
                fmt::print(stdout,
                           "WARNING: skip invalid pegasus key in '{}', key_size={}\n",
                           full_path,
                           iter->key().size());
                continue;
            }
            if (!write_batch_put(dst_db,
                                 dst_data_cf,
                                 &batch,
                                 &write_options,
                                 iter->key(),
                                 iter->value(),
                                 opts.write_batch_count,
                                 &pending_count,
                                 &err)) {
                fmt::print(stderr, "ERROR: failed to write data kv from sst: {}\n", err);
                return false;
            }
            ++stats->data_kv_count;
            ++record.data_kv_count;
        }
        if (!iter->status().ok()) {
            record.partial = true;
            record.partial_reason = iter->status().ToString();
            fmt::print(stdout,
                       "WARNING: iterator on sst '{}' stopped with error: {}\n",
                       full_path,
                       iter->status().ToString());
        }
    }

    if (!flush_write_batch(dst_db, &batch, &write_options, &pending_count, &err)) {
        fmt::print(stderr, "ERROR: failed to flush recovery batch: {}\n", err);
        return false;
    }
    refresh_required_metadata(meta);
    report->completed = true;
    return true;
}

bool dump_sst_recovery_report(const recover_options &opts,
                              const recovery_stats &stats,
                              const sst_recovery_report &report)
{
    if (!report.used_sst_scan) {
        return true;
    }

    std::ofstream os(opts.sst_report_file);
    if (!os) {
        fmt::print(stderr, "ERROR: open sst recovery report '{}' failed\n", opts.sst_report_file);
        return false;
    }

    os << "sst_recovery_report\n";
    os << "source_rdb_dir: " << report.source_rdb_dir << "\n";
    os << "completed: " << (report.completed ? "true" : "false") << "\n";
    os << "scanned_sst_count: " << stats.scanned_sst_count << "\n";
    os << "skipped_bad_sst_count: " << stats.skipped_bad_sst_count << "\n";
    os << "data_kv_count: " << stats.data_kv_count << "\n";
    os << "meta_kv_count: " << stats.meta_kv_count << "\n";
    os << "\n";

    for (const auto &record : report.records) {
        os << "file: " << record.path << "\n";
        os << "  column_family: "
           << (record.column_family_name.empty() ? "<unknown>" : record.column_family_name) << "\n";
        os << "  skipped: " << (record.skipped ? "true" : "false") << "\n";
        if (!record.skip_reason.empty()) {
            os << "  skip_reason: " << record.skip_reason << "\n";
        }
        os << "  partial: " << (record.partial ? "true" : "false") << "\n";
        if (!record.partial_reason.empty()) {
            os << "  partial_reason: " << record.partial_reason << "\n";
        }
        os << "  data_kv_count: " << record.data_kv_count << "\n";
        os << "  meta_kv_count: " << record.meta_kv_count << "\n";
        os << "  invalid_key_count: " << record.invalid_key_count << "\n";
        os << "\n";
    }
    return true;
}

bool infer_data_version(rocksdb::DB *db, rocksdb::ColumnFamilyHandle *data_cf, uint32_t *version)
{
    std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator({}, data_cf));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        if (iter->key().empty() || iter->value().empty()) {
            continue;
        }
        const uint8_t first_byte = static_cast<uint8_t>(iter->value().data()[0]);
        if ((first_byte & 0x80) != 0) {
            // Value schema v2 is encoded in the value body, but meta_cf still stores
            // pegasus_data_version as PEGASUS_DATA_VERSION_MAX(=1).
            *version = pegasus::PEGASUS_DATA_VERSION_MAX;
            return true;
        }
        break;
    }
    return false;
}

bool finalize_metadata(const recover_options &opts,
                       rocksdb::DB *dst_db,
                       rocksdb::ColumnFamilyHandle *dst_data_cf,
                       rocksdb::ColumnFamilyHandle *dst_meta_cf,
                       recovered_metadata *meta,
                       const recovery_stats &stats)
{
    if (opts.has_data_version_override) {
        meta->has_data_version = true;
        meta->data_version = opts.data_version_override;
        meta->meta_kvs[kPegasusDataVersion] = std::to_string(opts.data_version_override);
    } else if (!meta->has_data_version) {
        uint32_t inferred_version = 0;
        if (infer_data_version(dst_db, dst_data_cf, &inferred_version)) {
            meta->has_data_version = true;
            meta->data_version = inferred_version;
            meta->meta_kvs[kPegasusDataVersion] = std::to_string(inferred_version);
        }
    }

    if (!meta->has_data_version) {
        if (stats.data_kv_count == 0) {
            meta->has_data_version = true;
            meta->data_version = pegasus::PEGASUS_DATA_VERSION_MAX;
            meta->meta_kvs[kPegasusDataVersion] = std::to_string(pegasus::PEGASUS_DATA_VERSION_MAX);
            fmt::print(stdout,
                       "WARNING: no user kv recovered, fallback pegasus_data_version to {}\n",
                       pegasus::PEGASUS_DATA_VERSION_MAX);
        } else {
            fmt::print(stderr,
                       "ERROR: failed to recover pegasus_data_version from source data. "
                       "Auto fallback is unsafe here because legacy value layout may be v0 or v1. "
                       "Please rerun with --pegasus_data_version 0 or 1; for modern replicas, "
                       "--pegasus_data_version 1 is usually correct.\n");
            return false;
        }
    }

    if (opts.has_last_flushed_decree_override) {
        meta->has_last_flushed_decree = true;
        meta->last_flushed_decree = opts.last_flushed_decree_override;
        meta->meta_kvs[kPegasusLastFlushedDecree] =
            std::to_string(opts.last_flushed_decree_override);
    } else if (!meta->has_last_flushed_decree) {
        meta->has_last_flushed_decree = true;
        meta->last_flushed_decree = 0;
        meta->meta_kvs[kPegasusLastFlushedDecree] = "0";
    }

    if (opts.has_last_manual_compact_finish_time_override) {
        meta->has_last_manual_compact_finish_time = true;
        meta->last_manual_compact_finish_time = opts.last_manual_compact_finish_time_override;
        meta->meta_kvs[kPegasusLastManualCompactFinishTime] =
            std::to_string(opts.last_manual_compact_finish_time_override);
    } else if (!meta->has_last_manual_compact_finish_time) {
        meta->has_last_manual_compact_finish_time = true;
        meta->last_manual_compact_finish_time = 0;
        meta->meta_kvs[kPegasusLastManualCompactFinishTime] = "0";
    }

    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions write_options;
    write_options.disableWAL = true;
    for (const auto &[key, value] : meta->meta_kvs) {
        auto status = batch.Put(dst_meta_cf, key, value);
        if (!status.ok()) {
            fmt::print(stderr, "ERROR: failed to write meta '{}': {}\n", key, status.ToString());
            return false;
        }
    }
    auto status = dst_db->Write(write_options, &batch);
    if (!status.ok()) {
        fmt::print(stderr, "ERROR: failed to write finalized meta cf: {}\n", status.ToString());
        return false;
    }

    rocksdb::FlushOptions flush_options;
    flush_options.wait = true;
    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles = {dst_data_cf, dst_meta_cf};
    status = dst_db->Flush(flush_options, cf_handles);
    if (!status.ok()) {
        fmt::print(stderr, "ERROR: failed to flush recovered db: {}\n", status.ToString());
        return false;
    }
    return true;
}

bool dump_replica_metadata(const recover_options &opts, const dsn::app_info &app_info)
{
    dsn::app_info new_app_info(app_info);
    dsn::replication::replica_app_info replica_app(&new_app_info);
    const auto app_info_path = dsn::utils::filesystem::path_combine(
        opts.dst_replica_dir, dsn::replication::replica_app_info::kAppInfo);
    auto err = replica_app.store(app_info_path);
    if (err != dsn::ERR_OK) {
        fmt::print(stderr, "ERROR: write replica_app_info '{}' failed: {}\n", app_info_path, err);
        return false;
    }

    dsn::replication::replica_init_info init_info;
    const auto src_init_info_path = dsn::utils::filesystem::path_combine(
        opts.src_replica_dir, dsn::replication::replica_init_info::kInitInfo);
    err = dsn::utils::load_rjobj_from_file(src_init_info_path, &init_info);
    if (err != dsn::ERR_OK) {
        fmt::print(
            stderr, "ERROR: load replica_init_info '{}' failed: {}\n", src_init_info_path, err);
        return false;
    }
    init_info.init_offset_in_shared_log = 0;
    init_info.init_offset_in_private_log = 0;

    const auto dst_init_info_path = dsn::utils::filesystem::path_combine(
        opts.dst_replica_dir, dsn::replication::replica_init_info::kInitInfo);
    err = dsn::utils::dump_rjobj_to_file(init_info, dst_init_info_path);
    if (err != dsn::ERR_OK) {
        fmt::print(
            stderr, "ERROR: write replica_init_info '{}' failed: {}\n", dst_init_info_path, err);
        return false;
    }
    return true;
}

bool prepare_target_dirs(const recover_options &opts)
{
    if (dsn::utils::filesystem::directory_exists(opts.dst_replica_dir)) {
        fmt::print(
            stderr, "ERROR: destination replica dir '{}' already exists\n", opts.dst_replica_dir);
        return false;
    }

    const auto dst_data_dir = dsn::utils::filesystem::path_combine(
        opts.dst_replica_dir, dsn::replication::replication_app_base::kDataDir);
    if (!dsn::utils::filesystem::create_directory(opts.dst_replica_dir) ||
        !dsn::utils::filesystem::create_directory(dst_data_dir) ||
        !dsn::utils::filesystem::create_directory(opts.dst_rdb_dir)) {
        fmt::print(
            stderr, "ERROR: create destination directory '{}' failed\n", opts.dst_replica_dir);
        return false;
    }
    return true;
}

bool parse_options(arguments args, recover_options *opts)
{
    argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);
    if (cmd.pos_args().size() < 2) {
        return false;
    }

    opts->src_replica_dir = cmd(1).str();
    opts->dst_replica_dir = cmd(2).str();
    opts->src_rdb_dir = dsn::utils::filesystem::path_combine(
        dsn::utils::filesystem::path_combine(opts->src_replica_dir,
                                             dsn::replication::replication_app_base::kDataDir),
        dsn::replication::replication_app_base::kRdbDir);
    opts->dst_rdb_dir = dsn::utils::filesystem::path_combine(
        dsn::utils::filesystem::path_combine(opts->dst_replica_dir,
                                             dsn::replication::replication_app_base::kDataDir),
        dsn::replication::replication_app_base::kRdbDir);
    opts->sst_report_file =
        dsn::utils::filesystem::path_combine(opts->dst_replica_dir, "sst_recovery_report.txt");
    opts->force_sst_scan = cmd["--force_sst_scan"];
    opts->verify_checksum = cmd["--verify_checksum"];

    if (cmd["--write_batch_count"]) {
        if (!parse_u64(cmd("--write_batch_count").str(), &opts->write_batch_count) ||
            opts->write_batch_count == 0) {
            fmt::print(stderr,
                       "ERROR: invalid --write_batch_count '{}'\n",
                       cmd("--write_batch_count").str());
            return false;
        }
    }
    if (cmd["--pegasus_data_version"]) {
        opts->has_data_version_override = true;
        if (!parse_u32(cmd("--pegasus_data_version").str(), &opts->data_version_override)) {
            fmt::print(stderr,
                       "ERROR: invalid --pegasus_data_version '{}'\n",
                       cmd("--pegasus_data_version").str());
            return false;
        }
    }
    if (cmd["--last_flushed_decree"]) {
        opts->has_last_flushed_decree_override = true;
        if (!parse_u64(cmd("--last_flushed_decree").str(), &opts->last_flushed_decree_override)) {
            fmt::print(stderr,
                       "ERROR: invalid --last_flushed_decree '{}'\n",
                       cmd("--last_flushed_decree").str());
            return false;
        }
    }
    if (cmd["--last_manual_compact_finish_time"]) {
        opts->has_last_manual_compact_finish_time_override = true;
        if (!parse_u64(cmd("--last_manual_compact_finish_time").str(),
                       &opts->last_manual_compact_finish_time_override)) {
            fmt::print(stderr,
                       "ERROR: invalid --last_manual_compact_finish_time '{}'\n",
                       cmd("--last_manual_compact_finish_time").str());
            return false;
        }
    }
    return true;
}

} // namespace

bool local_replica_recover(command_executor *e, shell_context *sc, arguments args)
{
    recover_options opts;
    if (!parse_options(args, &opts)) {
        return false;
    }

    dsn::app_info app_info;
    dsn::gpid pid;
    std::string hint_message;
    if (!replica_stub::validate_replica_dir(opts.src_replica_dir, app_info, pid, hint_message)) {
        fmt::print(stderr,
                   "ERROR: invalid source replica dir '{}': {}\n",
                   opts.src_replica_dir,
                   hint_message);
        return true;
    }
    if (!dsn::utils::filesystem::directory_exists(opts.src_rdb_dir)) {
        fmt::print(stderr, "ERROR: source rocksdb dir '{}' does not exist\n", opts.src_rdb_dir);
        return true;
    }
    if (!prepare_target_dirs(opts)) {
        return true;
    }

    const std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs(
        {{pegasus::server::meta_store::DATA_COLUMN_FAMILY_NAME, {}},
         {pegasus::server::meta_store::META_COLUMN_FAMILY_NAME, {}}});
    rocksdb::DBOptions dst_db_opts;
    dst_db_opts.create_if_missing = true;
    dst_db_opts.create_missing_column_families = true;
    std::vector<rocksdb::ColumnFamilyHandle *> dst_cf_handles;
    rocksdb::DB *dst_db = nullptr;
    std::string err;
    if (!open_rocksdb(
            dst_db_opts, opts.dst_rdb_dir, false, cf_descs, &dst_cf_handles, &dst_db, &err)) {
        fmt::print(
            stderr, "ERROR: open destination rocksdb '{}' failed: {}\n", opts.dst_rdb_dir, err);
        return true;
    }
    auto cleanup =
        dsn::defer([&dst_cf_handles, &dst_db]() { release_db(&dst_cf_handles, &dst_db); });

    recovered_metadata meta;
    recovery_stats stats;
    sst_recovery_report sst_report;
    auto report_cleanup = dsn::defer([&opts, &stats, &sst_report]() {
        if (sst_report.used_sst_scan) {
            dump_sst_recovery_report(opts, stats, sst_report);
        }
    });
    bool recovered = false;
    if (!opts.force_sst_scan) {
        bool should_fallback = false;
        recovered = recover_from_db_view(
            opts, dst_db, dst_cf_handles[0], dst_cf_handles[1], &meta, &stats, &should_fallback);
        if (!recovered && !should_fallback) {
            return true;
        }
        if (recovered) {
            should_fallback = false;
        }
        if (!recovered && should_fallback) {
            recovered = recover_from_sst_scan(
                opts, dst_db, dst_cf_handles[0], dst_cf_handles[1], &meta, &stats, &sst_report);
        }
    } else {
        recovered = recover_from_sst_scan(
            opts, dst_db, dst_cf_handles[0], dst_cf_handles[1], &meta, &stats, &sst_report);
    }
    if (!recovered) {
        return true;
    }

    if (!finalize_metadata(opts, dst_db, dst_cf_handles[0], dst_cf_handles[1], &meta, stats)) {
        return true;
    }
    if (!dump_replica_metadata(opts, app_info)) {
        return true;
    }
    report_cleanup.cancel();
    if (sst_report.used_sst_scan && !dump_sst_recovery_report(opts, stats, sst_report)) {
        return true;
    }

    fmt::print(stdout,
               "recovery finished\n"
               "  src_replica_dir: {}\n"
               "  dst_replica_dir: {}\n"
               "  gpid: {}.{}\n"
               "  data_kv_count: {}\n"
               "  meta_kv_count: {}\n"
               "  pegasus_data_version: {}\n"
               "  last_flushed_decree: {}\n"
               "  scanned_sst_count: {}\n"
               "  skipped_bad_sst_count: {}\n"
               "  sst_report_file: {}\n",
               opts.src_replica_dir,
               opts.dst_replica_dir,
               pid.get_app_id(),
               pid.get_partition_index(),
               stats.data_kv_count,
               stats.meta_kv_count,
               meta.data_version,
               meta.last_flushed_decree,
               stats.scanned_sst_count,
               stats.skipped_bad_sst_count,
               sst_report.used_sst_scan ? opts.sst_report_file : "<not generated>");
    return true;
}
