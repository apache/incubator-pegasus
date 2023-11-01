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

#include "disk_cleaner.h"

#include <boost/algorithm/string/predicate.hpp>
#include <fmt/core.h>
#include <stdint.h>
#include <sys/types.h>
#include <algorithm>
#include <atomic>
#include <cstring>

#include "common/fs_manager.h"
#include "metadata_types.h"
#include "runtime/api_layer1.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/macros.h"
#include "utils/string_conv.h"
#include "utils/string_view.h"

namespace dsn {
namespace replication {

DSN_DEFINE_uint64(
    replication,
    gc_disk_error_replica_interval_seconds,
    7 * 24 * 3600 /*7day*/,
    "Duration of error replica being removed, which is in a directory with '.err' suffixed");
DSN_TAG_VARIABLE(gc_disk_error_replica_interval_seconds, FT_MUTABLE);

DSN_DEFINE_uint64(
    replication,
    gc_disk_garbage_replica_interval_seconds,
    24 * 3600 /*1day*/,
    "Duration of garbaged replica being removed, which is in a directory with '.gar' suffixed");
DSN_TAG_VARIABLE(gc_disk_garbage_replica_interval_seconds, FT_MUTABLE);

DSN_DEFINE_uint64(replication,
                  gc_disk_migration_tmp_replica_interval_seconds,
                  24 * 3600 /*1day*/,
                  "Duration of disk-migration tmp replica being removed, which is in a directory "
                  "with '.tmp' suffixed");
DSN_TAG_VARIABLE(gc_disk_migration_tmp_replica_interval_seconds, FT_MUTABLE);

DSN_DEFINE_uint64(replication,
                  gc_disk_migration_origin_replica_interval_seconds,
                  7 * 24 * 3600 /*7day*/,
                  "Duration of disk-migration origin replica being removed, which is in a "
                  "directory with '.ori' suffixed");
DSN_TAG_VARIABLE(gc_disk_migration_origin_replica_interval_seconds, FT_MUTABLE);

const std::string kFolderSuffixErr = ".err";
const std::string kFolderSuffixGar = ".gar";
const std::string kFolderSuffixBak = ".bak";
const std::string kFolderSuffixOri = ".ori";
const std::string kFolderSuffixTmp = ".tmp";

namespace {

bool get_expiration_seconds_by_last_write_time(const std::string &path,
                                               uint64_t delay_seconds,
                                               uint64_t &expiration_seconds)
{
    time_t last_write_seconds;
    if (!dsn::utils::filesystem::last_write_time(path, last_write_seconds)) {
        LOG_WARNING("gc_disk: failed to get last write time of {}", path);
        return false;
    }

    expiration_seconds = static_cast<uint64_t>(last_write_seconds) + delay_seconds;
    return true;
}

// Unix timestamp in microseconds for 2010-01-01 00:00:00.
#define MIN_TIMESTAMP_US 1262275200000000
#define MIN_TIMESTAMP_US_LENGTH (sizeof(STRINGIFY(MIN_TIMESTAMP_US)) - 1)

bool parse_timestamp_us(const std::string &name, size_t suffix_size, uint64_t &timestamp_us)
{
    CHECK_GE(name.size(), suffix_size);

    if (suffix_size == name.size()) {
        return false;
    }

    const size_t end_idx = name.size() - suffix_size;
    auto begin_idx = name.find_last_of('.', end_idx - 1);
    if (begin_idx == std::string::npos || ++begin_idx >= end_idx) {
        return false;
    }

    const auto length = end_idx - begin_idx;
    if (length < MIN_TIMESTAMP_US_LENGTH) {
        return false;
    }

    const auto begin_itr = name.cbegin() + begin_idx;
    if (!std::all_of(begin_itr, begin_itr + length, ::isdigit)) {
        return false;
    }

    const auto ok =
        dsn::buf2uint64(dsn::string_view(name.data() + begin_idx, length), timestamp_us);
    return ok ? timestamp_us > MIN_TIMESTAMP_US : false;
}

bool get_expiration_seconds_by_timestamp(const std::string &name,
                                         const std::string &path,
                                         size_t suffix_size,
                                         uint64_t delay_seconds,
                                         uint64_t &expiration_seconds)
{
    uint64_t timestamp_us = 0;
    if (!parse_timestamp_us(name, suffix_size, timestamp_us)) {
        LOG_WARNING("gc_disk: failed to parse timestamp from {}, turn to "
                    "the last write time for {}",
                    name,
                    path);
        return get_expiration_seconds_by_last_write_time(path, delay_seconds, expiration_seconds);
    }

    expiration_seconds = timestamp_us / 1000000 + delay_seconds;
    return true;
}

} // anonymous namespace

error_s disk_remove_useless_dirs(const std::vector<std::shared_ptr<dir_node>> &dir_nodes,
                                 /*output*/ disk_cleaning_report &report)
{
    std::vector<std::string> sub_list;
    for (const auto &dn : dir_nodes) {
        // It's allowed to clear up the directory when it's SPACE_INSUFFICIENT, but not allowed when
        // it's IO_ERROR.
        if (dn->status == disk_status::IO_ERROR) {
            continue;
        }
        std::vector<std::string> tmp_list;
        if (!dsn::utils::filesystem::get_subdirectories(dn->full_dir, tmp_list, false)) {
            LOG_WARNING("gc_disk: failed to get subdirectories in {}", dn->full_dir);
            return error_s::make(ERR_OBJECT_NOT_FOUND, "failed to get subdirectories");
        }
        sub_list.insert(sub_list.end(), tmp_list.begin(), tmp_list.end());
    }

    for (const auto &path : sub_list) {
        uint64_t expiration_seconds = 0;

        // Note: don't delete ".bak" directory since it could be did by administrator.
        const auto name = dsn::utils::filesystem::get_file_name(path);
        if (boost::algorithm::ends_with(name, kFolderSuffixErr)) {
            report.error_replica_count++;
            if (!get_expiration_seconds_by_timestamp(name,
                                                     path,
                                                     kFolderSuffixErr.size(),
                                                     FLAGS_gc_disk_error_replica_interval_seconds,
                                                     expiration_seconds)) {
                continue;
            }
        } else if (boost::algorithm::ends_with(name, kFolderSuffixGar)) {
            report.garbage_replica_count++;
            if (!get_expiration_seconds_by_timestamp(name,
                                                     path,
                                                     kFolderSuffixGar.size(),
                                                     FLAGS_gc_disk_garbage_replica_interval_seconds,
                                                     expiration_seconds)) {
                continue;
            }
        } else if (boost::algorithm::ends_with(name, kFolderSuffixTmp)) {
            report.disk_migrate_tmp_count++;
            if (!get_expiration_seconds_by_last_write_time(
                    path,
                    FLAGS_gc_disk_migration_tmp_replica_interval_seconds,
                    expiration_seconds)) {
                continue;
            }
        } else if (boost::algorithm::ends_with(name, kFolderSuffixOri)) {
            report.disk_migrate_origin_count++;
            if (!get_expiration_seconds_by_last_write_time(
                    path,
                    FLAGS_gc_disk_migration_origin_replica_interval_seconds,
                    expiration_seconds)) {
                continue;
            }
        } else {
            continue;
        }

        const auto current_time_ms = dsn_now_ms();
        if (expiration_seconds <= current_time_ms / 1000) {
            if (dsn::utils::filesystem::remove_path(path)) {
                LOG_WARNING("gc_disk: replica_dir_op succeed to delete directory '{}'"
                            ", time_used_ms = {}",
                            path,
                            dsn_now_ms() - current_time_ms);
                report.remove_dir_count++;
            } else {
                LOG_WARNING("gc_disk: failed to delete directory '{}', time_used_ms = {}",
                            path,
                            dsn_now_ms() - current_time_ms);
            }
        } else {
            LOG_INFO("gc_disk: reserve directory '{}', wait_seconds = {}",
                     path,
                     expiration_seconds - current_time_ms / 1000);
        }
    }
    return error_s::ok();
}

bool is_data_dir_removable(const std::string &dir)
{
    return boost::algorithm::ends_with(dir, kFolderSuffixErr) ||
           boost::algorithm::ends_with(dir, kFolderSuffixGar) ||
           boost::algorithm::ends_with(dir, kFolderSuffixTmp) ||
           boost::algorithm::ends_with(dir, kFolderSuffixOri);
}

bool is_data_dir_invalid(const std::string &dir)
{
    return is_data_dir_removable(dir) || boost::algorithm::ends_with(dir, kFolderSuffixBak);
}

void move_to_err_path(const std::string &path, const std::string &log_prefix)
{
    const std::string new_path = fmt::format("{}.{}{}", path, dsn_now_us(), kFolderSuffixErr);
    CHECK(dsn::utils::filesystem::rename_path(path, new_path),
          "{}: failed to move directory from '{}' to '{}'",
          log_prefix,
          path,
          new_path);
    LOG_WARNING("{}: succeed to move directory from '{}' to '{}'", log_prefix, path, new_path);
}

} // namespace replication
} // namespace dsn
