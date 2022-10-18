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

#include <vector>
#include "utils/errors.h"
#include "perf_counter/perf_counter_wrapper.h"

namespace dsn {
namespace replication {
DSN_DECLARE_uint64(gc_disk_error_replica_interval_seconds);
DSN_DECLARE_uint64(gc_disk_garbage_replica_interval_seconds);
DSN_DECLARE_uint64(gc_disk_migration_tmp_replica_interval_seconds);
DSN_DECLARE_uint64(gc_disk_migration_origin_replica_interval_seconds);

// the invalid folder suffix, server will check disk folder and deal with them
extern const std::string kFolderSuffixErr; // replica error dir
extern const std::string kFolderSuffixGar; // replica closed and assign garbage dir
extern const std::string kFolderSuffixBak; // replica backup dir which can be restored
extern const std::string kFolderSuffixOri; // replica disk migration origin dir
extern const std::string kFolderSuffixTmp; // replica disk migration temp dir

struct disk_cleaning_report
{
    int remove_dir_count{0};

    int garbage_replica_count{0};
    int error_replica_count{0};
    int disk_migrate_tmp_count{0};
    int disk_migrate_origin_count{0};
};

// Removes the useless data from data directories.
extern error_s disk_remove_useless_dirs(const std::vector<std::string> &data_dirs,
                                        /*output*/ disk_cleaning_report &report);

inline bool is_data_dir_removable(const std::string &dir)
{
    if (dir.length() < 4) {
        return false;
    }
    const std::string folder_suffix = dir.substr(dir.length() - 4);
    return (folder_suffix == kFolderSuffixErr || folder_suffix == kFolderSuffixGar ||
            folder_suffix == kFolderSuffixTmp || folder_suffix == kFolderSuffixOri);
}

// Note: ".bak" is invalid but not allow delete, because it can be backed by administrator.
inline bool is_data_dir_invalid(const std::string &dir)
{
    if (dir.length() < 4) {
        return false;
    }
    const std::string folder_suffix = dir.substr(dir.length() - 4);
    return is_data_dir_removable(dir) || folder_suffix == kFolderSuffixBak;
}
} // namespace replication
} // namespace dsn
