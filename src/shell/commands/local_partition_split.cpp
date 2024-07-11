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

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/iterator.h>
#include <rocksdb/metadata.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/table_properties.h>
#include <rocksdb/threadpool.h>
#include <stdio.h>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/meta_store.h"
#include "base/pegasus_key_schema.h"
#include "base/value_schema_manager.h"
#include "client/partition_resolver.h"
#include "client/replication_ddl_client.h"
#include "common/gpid.h"
#include "common/replication_common.h"
#include "dsn.layer2_types.h"
#include "pegasus_value_schema.h"
#include "replica/replica_stub.h"
#include "replica/replication_app_base.h"
#include "shell/argh.h"
#include "shell/command_executor.h"
#include "shell/command_helper.h"
#include "shell/commands.h"
#include "utils/blob.h"
#include "utils/errors.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/load_dump_object.h"
#include "utils/output_utils.h"

const std::string local_partition_split_help =
    "<src_data_dirs> <dst_data_dirs> <src_app_id> "
    "<dst_app_id> <src_partition_ids> <src_partition_count> "
    "<dst_partition_count> <dst_app_name> [--post_full_compact] [--post_count] "
    "[--threads_per_data_dir num] [--threads_per_partition num]";

struct ToSplitPatition
{
    std::string replica_dir;
    dsn::app_info ai;
    dsn::replication::replica_init_info rii;
    int32_t pidx = 0;
};

struct LocalPartitionSplitContext
{
    // Parameters from the command line.
    std::vector<std::string> src_data_dirs;
    std::vector<std::string> dst_data_dirs;
    uint32_t src_app_id = 0;
    uint32_t dst_app_id = 0;
    std::set<uint32_t> src_partition_ids;
    uint32_t src_partition_count = 0;
    uint32_t dst_partition_count = 0;
    uint32_t threads_per_data_dir = 1;
    uint32_t threads_per_partition = 1;
    std::string dst_app_name;
    bool post_full_compact = false;
    bool post_count = false;

    // Calculate from the parameters above.
    uint32_t split_count = 0;
};

struct FileSplitResult
{
    std::string filename;
    bool success = false;
    std::vector<uint64_t> split_counts;
};

struct PartitionSplitResult
{
    std::string src_replica_dir;
    std::map<std::string, int64_t> key_count_by_dst_replica_dirs;
    bool success = false;
    std::vector<FileSplitResult> fsrs;
};

struct DataDirSplitResult
{
    std::string src_data_dir;
    std::string dst_data_dir;
    bool success = false;
    std::vector<PartitionSplitResult> psrs;
};

bool validate_parameters(LocalPartitionSplitContext &lpsc)
{
    // TODO(yingchun): check disk space.
    // Check <src_data_dirs> and <dst_data_dirs>.
    RETURN_FALSE_IF_NOT(
        lpsc.src_data_dirs.size() == lpsc.dst_data_dirs.size(),
        "invalid command, the list size of <src_data_dirs> and <dst_data_dirs> must be equal");

    // Check <dst_app_id>.
    RETURN_FALSE_IF_NOT(
        lpsc.src_app_id != lpsc.dst_app_id,
        "invalid command, <src_app_id> and <dst_app_id> should not be equal ({} vs. {})",
        lpsc.src_app_id,
        lpsc.dst_app_id);

    // Check <src_partition_ids>.
    for (const auto src_partition_id : lpsc.src_partition_ids) {
        RETURN_FALSE_IF_NOT(
            src_partition_id < lpsc.src_partition_count,
            "invalid command, partition ids in <src_partition_ids> should be in range [0, {})",
            lpsc.src_partition_count);
    }

    // Check <dst_partition_count>.
    RETURN_FALSE_IF_NOT(lpsc.dst_partition_count > lpsc.src_partition_count,
                        "invalid command, <dst_partition_count> should be larger than "
                        "<src_partition_count> ({} vs. {})",
                        lpsc.dst_partition_count,
                        lpsc.src_partition_count);
    lpsc.split_count = lpsc.dst_partition_count / lpsc.src_partition_count;
    const auto log2n = static_cast<uint32_t>(log2(lpsc.split_count));
    RETURN_FALSE_IF_NOT(pow(2, log2n) == lpsc.split_count,
                        "invalid command, <dst_partition_count> should be 2^n times of "
                        "<src_partition_count> ({} vs. {})",
                        lpsc.dst_partition_count,
                        lpsc.src_partition_count);

    const auto es = replication_ddl_client::validate_app_name(lpsc.dst_app_name);
    RETURN_FALSE_IF_NOT(es.is_ok(),
                        "invalid command, <dst_app_name> '{}' is invalid: {}",
                        lpsc.dst_app_name,
                        es.description());

    return true;
}

std::string construct_split_directory(const std::string &parent_dir,
                                      const ToSplitPatition &tsp,
                                      uint32_t dst_app_id,
                                      uint32_t split_index)
{
    return fmt::format("{}/{}.{}.pegasus",
                       parent_dir,
                       dst_app_id,
                       tsp.pidx + split_index * tsp.ai.partition_count);
}

bool split_file(const LocalPartitionSplitContext &lpsc,
                const ToSplitPatition &tsp,
                const rocksdb::LiveFileMetaData &file,
                const std::string &tmp_split_replicas_dir,
                uint32_t pegasus_data_version,
                FileSplitResult &sfr)
{
    const auto src_sst_file = dsn::utils::filesystem::path_combine(file.db_path, file.name);

    // 1. Open reader.
    // TODO(yingchun): improve options.
    auto reader = std::make_unique<rocksdb::SstFileReader>(rocksdb::Options());
    RETURN_FALSE_IF_NON_RDB_OK(
        reader->Open(src_sst_file), "open reader file '{}' failed", src_sst_file);
    RETURN_FALSE_IF_NON_RDB_OK(
        reader->VerifyChecksum(), "verify reader file '{}' failed", src_sst_file);

    // 2. Validate the files.
    const auto tbl_ppts = reader->GetTableProperties();
    // The metadata column family file has been skipped in the previous steps.
    CHECK_NE(tbl_ppts->column_family_name, pegasus::server::meta_store::META_COLUMN_FAMILY_NAME);
    // TODO(yingchun): It seems the SstFileReader could only read the live key-value
    //  pairs in the sst file. If a key-value pair is put in a higher level and deleted
    //  in a lower level, it can still be read when iterate the high level sst file,
    //  which means the deleted key-value pair will appear again.
    //  So it's needed to do a full compaction before using the 'local_partition_split'
    //  tool to remove this kind of keys from DB.
    //  We use the following validators to check the sst file.
    RETURN_FALSE_IF_NOT(tbl_ppts->num_deletions == 0,
                        "invalid sst file '{}', it contains {} deletions",
                        src_sst_file,
                        tbl_ppts->num_deletions);
    RETURN_FALSE_IF_NOT(tbl_ppts->num_merge_operands == 0,
                        "invalid sst file '{}', it contains {} merge_operands",
                        src_sst_file,
                        tbl_ppts->num_merge_operands);
    RETURN_FALSE_IF_NOT(tbl_ppts->num_range_deletions == 0,
                        "invalid sst file '{}', it contains {} range_deletions",
                        src_sst_file,
                        tbl_ppts->num_range_deletions);

    // 3. Prepare the split temporary directories.
    std::vector<std::string> dst_tmp_rdb_dirs;
    dst_tmp_rdb_dirs.resize(lpsc.split_count);
    for (int i = 0; i < lpsc.split_count; i++) {
        const auto dst_tmp_rdb_dir =
            construct_split_directory(tmp_split_replicas_dir, tsp, lpsc.dst_app_id, i);

        RETURN_FALSE_IF_NOT(dsn::utils::filesystem::create_directory(dst_tmp_rdb_dir),
                            "create directory '{}' failed",
                            dst_tmp_rdb_dir);

        dst_tmp_rdb_dirs[i] = dst_tmp_rdb_dir;
    }

    // 4. Iterate the sst file though sst reader, then split it to multiple sst files
    // though sst writers.
    std::shared_ptr<rocksdb::SstFileWriter> writers[lpsc.split_count];
    std::unique_ptr<rocksdb::Iterator> iter(reader->NewIterator({}));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        const auto &skey = iter->key();
        const auto &svalue = iter->value();
        // Skip empty write, see:
        // https://pegasus.apache.org/zh/2018/03/07/last_flushed_decree.html.
        if (skey.empty() && pegasus::value_schema_manager::instance()
                                .get_value_schema(pegasus_data_version)
                                ->extract_user_data(svalue.ToString())
                                .empty()) {
            continue;
        }

        // i. Calculate the hash value and corresponding new partition index.
        dsn::blob bb_key(skey.data(), 0, skey.size());
        uint64_t hash_value = pegasus::pegasus_key_hash(bb_key);
        const auto new_pidx = dsn::replication::partition_resolver::get_partition_index(
            static_cast<int>(lpsc.dst_partition_count), hash_value);
        CHECK_LE(0, new_pidx);
        CHECK_LT(new_pidx, lpsc.dst_partition_count);

        // ii. Calculate the writer index.
        const auto writer_idx = new_pidx / lpsc.src_partition_count;
        CHECK_LE(0, writer_idx);
        CHECK_LT(writer_idx, lpsc.split_count);

        // TODO(yingchun): improve to check expired data.

        // iii. Create the writer if needed.
        auto &dst_writer = writers[writer_idx];
        if (!dst_writer) {
            const auto dst_tmp_rdb_file =
                fmt::format("{}{}", dst_tmp_rdb_dirs[writer_idx], file.name);
            // TODO(yingchun): improve options.
            dst_writer =
                std::make_shared<rocksdb::SstFileWriter>(rocksdb::EnvOptions(), rocksdb::Options());
            RETURN_FALSE_IF_NON_RDB_OK(dst_writer->Open(dst_tmp_rdb_file),
                                       "open writer file '{}' failed",
                                       dst_tmp_rdb_file);
        }

        // iv. Write data to the new partition sst file.
        sfr.split_counts[writer_idx]++;
        RETURN_FALSE_IF_NON_RDB_OK(dst_writer->Put(skey, svalue),
                                   "write data failed when split from file {}",
                                   src_sst_file);
    }

    // 5. Finalize the writers.
    for (int i = 0; i < lpsc.split_count; i++) {
        // Skip the non-opened writer.
        if (sfr.split_counts[i] == 0) {
            CHECK_TRUE(writers[i] == nullptr);
            continue;
        }

        RETURN_FALSE_IF_NON_RDB_OK(writers[i]->Finish(nullptr),
                                   "finalize writer split from file '{}' failed",
                                   src_sst_file);
    }
    return true;
}

bool open_rocksdb(const rocksdb::DBOptions &db_opts,
                  const std::string &rdb_dir,
                  bool read_only,
                  const std::vector<rocksdb::ColumnFamilyDescriptor> &cf_dscs,
                  std::vector<rocksdb::ColumnFamilyHandle *> *cf_hdls,
                  rocksdb::DB **db)
{
    CHECK_NOTNULL(cf_hdls, "");
    CHECK_NOTNULL(db, "");
    if (read_only) {
        RETURN_FALSE_IF_NON_RDB_OK(
            rocksdb::DB::OpenForReadOnly(db_opts, rdb_dir, cf_dscs, cf_hdls, db),
            "open rocksdb in '{}' failed",
            rdb_dir);
    } else {
        RETURN_FALSE_IF_NON_RDB_OK(rocksdb::DB::Open(db_opts, rdb_dir, cf_dscs, cf_hdls, db),
                                   "open rocksdb in '{}' failed",
                                   rdb_dir);
    }
    CHECK_EQ(2, cf_hdls->size());
    CHECK_EQ(pegasus::server::meta_store::DATA_COLUMN_FAMILY_NAME, (*cf_hdls)[0]->GetName());
    CHECK_EQ(pegasus::server::meta_store::META_COLUMN_FAMILY_NAME, (*cf_hdls)[1]->GetName());

    return true;
}

void release_db(std::vector<rocksdb::ColumnFamilyHandle *> *cf_hdls, rocksdb::DB **db)
{
    CHECK_NOTNULL(cf_hdls, "");
    CHECK_NOTNULL(db, "");
    for (auto cf_hdl : *cf_hdls) {
        delete cf_hdl;
    }
    cf_hdls->clear();
    delete *db;
    *db = nullptr;
}

bool split_partition(const LocalPartitionSplitContext &lpsc,
                     const ToSplitPatition &tsp,
                     const std::string &dst_replicas_dir,
                     const std::string &tmp_split_replicas_dir,
                     PartitionSplitResult &psr)
{
    static const std::string kRdbDirPostfix =
        dsn::utils::filesystem::path_combine(dsn::replication::replication_app_base::kDataDir,
                                             dsn::replication::replication_app_base::kRdbDir);
    const auto rdb_dir = dsn::utils::filesystem::path_combine(tsp.replica_dir, kRdbDirPostfix);
    fmt::print(stdout, " start to split '{}'\n", rdb_dir);

    // 1. Open the original rocksdb in read-only mode.
    rocksdb::DBOptions db_opts;
    // The following options should be set in Pegasus 2.0 and lower versions.
    // db_opts.pegasus_data = true;
    // db_opts.pegasus_data_version = pegasus::PEGASUS_DATA_VERSION_MAX;
    const std::vector<rocksdb::ColumnFamilyDescriptor> cf_dscs(
        {{pegasus::server::meta_store::DATA_COLUMN_FAMILY_NAME, {}},
         {pegasus::server::meta_store::META_COLUMN_FAMILY_NAME, {}}});
    std::vector<rocksdb::ColumnFamilyHandle *> cf_hdls;
    rocksdb::DB *db = nullptr;
    RETURN_FALSE_IF_NOT(open_rocksdb(db_opts, rdb_dir, true, cf_dscs, &cf_hdls, &db), "");

    // 2. Get metadata from rocksdb.
    // - In Pegasus versions lower than 2.0, the metadata is only stored in the MANIFEST
    //   file.
    // - In Pegasus 2.0, the metadata is stored both in the metadata column family and
    //   MANIFEST file.
    // - Since Pegasus 2.1, the metadata is only stored in the metadata column family.
    auto ms = std::make_unique<pegasus::server::meta_store>(rdb_dir.c_str(), db, cf_hdls[1]);
    uint64_t last_committed_decree;
    RETURN_FALSE_IF_NON_OK(ms->get_last_flushed_decree(&last_committed_decree),
                           "get_last_flushed_decree from '{}' failed",
                           rdb_dir);

    uint32_t pegasus_data_version;
    RETURN_FALSE_IF_NON_OK(
        ms->get_data_version(&pegasus_data_version), "get_data_version from '{}' failed", rdb_dir);

    uint64_t last_manual_compact_finish_time;
    RETURN_FALSE_IF_NON_OK(
        ms->get_last_manual_compact_finish_time(&last_manual_compact_finish_time),
        "get_last_manual_compact_finish_time from '{}' failed",
        rdb_dir);

    // 3. Get all live sst files.
    std::vector<rocksdb::LiveFileMetaData> files;
    db->GetLiveFilesMetaData(&files);

    // 4. Close rocksdb.
    release_db(&cf_hdls, &db);

    // 5. Split the sst files.
    auto files_thread_pool = std::unique_ptr<rocksdb::ThreadPool>(
        rocksdb::NewThreadPool(static_cast<int>(lpsc.threads_per_partition)));
    psr.fsrs.reserve(files.size());
    for (const auto &file : files) {
        // Skip metadata column family files, we will write metadata manually later in
        // the new DB.
        if (file.column_family_name == pegasus::server::meta_store::META_COLUMN_FAMILY_NAME) {
            fmt::print(
                stdout, "  skip [{}]: {}: {}\n", file.column_family_name, file.db_path, file.name);
            continue;
        }

        // Statistic the file split result.
        psr.fsrs.emplace_back();
        auto &sfr = psr.fsrs.back();
        sfr.filename = file.name;
        sfr.split_counts.resize(lpsc.split_count);

        files_thread_pool->SubmitJob([=, &sfr]() {
            sfr.success =
                split_file(lpsc, tsp, file, tmp_split_replicas_dir, pegasus_data_version, sfr);
        });
    }
    files_thread_pool->WaitForJobsAndJoinAllThreads();
    files_thread_pool.reset();

    // 6. Create new rocksdb instances for the new partitions.
    // TODO(yingchun): poolize the following operations if necessary.
    for (int i = 0; i < lpsc.split_count; i++) {
        // The new replica is placed in 'dst_replicas_dir'.
        const auto new_replica_dir =
            construct_split_directory(dst_replicas_dir, tsp, lpsc.dst_app_id, i);
        const auto new_rdb_dir =
            dsn::utils::filesystem::path_combine(new_replica_dir, kRdbDirPostfix);

        // i. Create the directory for the split rocksdb.
        // TODO(yingchun): make sure it's not exist!
        RETURN_FALSE_IF_NOT(dsn::utils::filesystem::create_directory(new_rdb_dir),
                            "create directory '{}' failed",
                            new_rdb_dir);

        // ii. Open new rocksdb.
        rocksdb::DBOptions new_db_opts;
        new_db_opts.create_if_missing = true;
        // Create the 'pegasus_meta_cf' column family.
        new_db_opts.create_missing_column_families = true;
        RETURN_FALSE_IF_NOT(open_rocksdb(new_db_opts, new_rdb_dir, false, cf_dscs, &cf_hdls, &db),
                            "");
        const auto count_of_new_replica =
            psr.key_count_by_dst_replica_dirs.insert({new_replica_dir, -1});
        CHECK_TRUE(count_of_new_replica.second);

        // iii. Ingest the split sst files to the new rocksdb.
        do {
            // Skip non-exist directory.
            const auto dst_tmp_rdb_dir =
                construct_split_directory(tmp_split_replicas_dir, tsp, lpsc.dst_app_id, i);
            if (!dsn::utils::filesystem::directory_exists(dst_tmp_rdb_dir)) {
                break;
            }

            // Gather all files.
            rocksdb::IngestExternalFileArg arg;
            arg.column_family = cf_hdls[0];
            RETURN_FALSE_IF_NOT(
                dsn::utils::filesystem::get_subfiles(dst_tmp_rdb_dir, arg.external_files, false),
                "get sub-files from '{}' failed",
                dst_tmp_rdb_dir);

            // Skip empty directory.
            if (arg.external_files.empty()) {
                break;
            }

            // Ingest files.
            RETURN_FALSE_IF_NON_RDB_OK(db->IngestExternalFiles({arg}),
                                       "ingest files from '{}' to '{}' failed",
                                       dst_tmp_rdb_dir,
                                       new_rdb_dir);

            // Optional full compaction.
            if (lpsc.post_full_compact) {
                RETURN_FALSE_IF_NON_RDB_OK(
                    db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr),
                    "full compact rocksdb in '{}' failed",
                    new_rdb_dir);
            }

            // Optional data counting.
            if (lpsc.post_count) {
                std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator({}));
                int new_total_count = 0;
                for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
                    new_total_count++;
                }
                count_of_new_replica.first->second = new_total_count;
            }
        } while (false);

        // iv. Set metadata to rocksdb.
        // - In Pegasus versions lower than 2.0, the metadata is only stored in the MANIFEST
        //   file.
        // - In Pegasus 2.0, the metadata is stored both in the metadata column family and
        //   MANIFEST file.
        // - Since Pegasus 2.1, the metadata is only stored in the metadata column family.

        // TODO(yingchun): these metadata are only written to the metadata column family,
        //  not the manifest file. So this tool is not supporting Pegasus versions lower
        //  than 2.0.
        //  For Pegasus 2.0, it's needed to set [pegasus.server]get_meta_store_type =
        //  "metacf" when restart replica servers after using this tool.
        auto new_ms =
            std::make_unique<pegasus::server::meta_store>(new_rdb_dir.c_str(), db, cf_hdls[1]);
        new_ms->set_data_version(pegasus_data_version);
        new_ms->set_last_flushed_decree(last_committed_decree);
        new_ms->set_last_manual_compact_finish_time(last_manual_compact_finish_time);
        rocksdb::FlushOptions options;
        options.wait = true;
        RETURN_FALSE_IF_NON_RDB_OK(
            db->Flush(options, cf_hdls), "flush rocksdb in '{}' failed", new_rdb_dir);

        // v. Close rocksdb.
        release_db(&cf_hdls, &db);

        // vi. Generate new ".app-info".
        dsn::app_info new_ai(tsp.ai);
        new_ai.app_name = lpsc.dst_app_name;
        new_ai.app_id = static_cast<int32_t>(lpsc.dst_app_id);
        new_ai.partition_count = static_cast<int32_t>(lpsc.dst_partition_count);
        // Note that the online partition split used 'init_partition_count' field will be
        // reset.
        new_ai.init_partition_count = -1;
        dsn::replication::replica_app_info rai(&new_ai);
        const auto rai_path = dsn::utils::filesystem::path_combine(
            new_replica_dir, dsn::replication::replica_app_info::kAppInfo);
        RETURN_FALSE_IF_NON_OK(rai.store(rai_path), "write replica_app_info '{}' failed", rai_path);

        // vii. Generate new ".init-info".
        dsn::replication::replica_init_info new_rii(tsp.rii);
        new_rii.init_offset_in_shared_log = 0;
        new_rii.init_offset_in_private_log = 0;
        const auto rii_path =
            dsn::utils::filesystem::path_combine(new_replica_dir, replica_init_info::kInitInfo);
        RETURN_FALSE_IF_NON_OK(dsn::utils::dump_rjobj_to_file(new_rii, rii_path),
                               "write replica_init_info '{}' failed",
                               rii_path);
    }
    if (std::any_of(psr.fsrs.begin(), psr.fsrs.end(), [](const FileSplitResult &fsr) {
            return !fsr.success;
        })) {
        return false;
    }
    return true;
}

bool split_data_directory(const LocalPartitionSplitContext &lpsc,
                          const std::string &src_data_dir,
                          const std::string &dst_data_dir,
                          DataDirSplitResult &ddsr)
{
    static const std::string kReplicasDir =
        dsn::utils::filesystem::path_combine(dsn::replication::replication_options::kReplicaAppType,
                                             dsn::replication::replication_options::kRepsDir);

    // 1. Collect all replica directories from 'src_data_dir'.
    const auto src_replicas_dir = dsn::utils::filesystem::path_combine(src_data_dir, kReplicasDir);
    std::vector<std::string> replica_dirs;
    RETURN_FALSE_IF_NOT(
        dsn::utils::filesystem::get_subdirectories(src_replicas_dir, replica_dirs, false),
        "get sub-directories from '{}' failed",
        src_replicas_dir);

    // 2. Create temporary split directory on 'dst_data_dir'.
    const auto tmp_split_replicas_dir = dsn::utils::filesystem::path_combine(dst_data_dir, "split");
    RETURN_FALSE_IF_NOT(dsn::utils::filesystem::create_directory(tmp_split_replicas_dir),
                        "create split directory '{}' failed",
                        tmp_split_replicas_dir);

    // 3. Gather partitions to split.
    std::vector<ToSplitPatition> to_split_partitions;
    std::set<uint32_t> exist_app_ids;
    std::set<std::string> exist_app_names;
    std::set<uint32_t> remain_partition_ids(lpsc.src_partition_ids);
    const std::set<std::string> ordered_replica_dirs(replica_dirs.begin(), replica_dirs.end());
    for (const auto &replica_dir : ordered_replica_dirs) {
        // i. Validate the replica directory.
        dsn::app_info ai;
        dsn::gpid pid;
        std::string hint_message;
        if (!replica_stub::validate_replica_dir(replica_dir, ai, pid, hint_message)) {
            fmt::print(stderr, "invalid replica dir '{}': {}\n", replica_dir, hint_message);
            continue;
        }

        // ii. Skip the non-<src_app_id>.
        CHECK_EQ(pid.get_app_id(), ai.app_id);
        if (ai.app_id != lpsc.src_app_id) {
            continue;
        }

        // iii. Skip and warning for the replica with the same app id but not desired partition
        // index.
        const auto cur_pidx = pid.get_partition_index();
        if (lpsc.src_partition_ids.count(cur_pidx) == 0) {
            fmt::print(stdout,
                       "WARNING: the partition index {} of the <src_app_id> {} is skipped\n",
                       cur_pidx,
                       lpsc.src_app_id);
            continue;
        }

        // iv. Continue and warning if the <dst_app_id> exist.
        exist_app_ids.insert(ai.app_id);
        if (exist_app_ids.count(lpsc.dst_app_id) != 0) {
            fmt::print(
                stdout,
                "WARNING: there is already a replica {} with the same <dst_app_id> {} exists\n",
                replica_dir,
                lpsc.dst_app_id);
        }

        // v. Continue and warning if the <dst_app_name> exist.
        exist_app_names.insert(ai.app_name);
        if (exist_app_names.count(lpsc.dst_app_name) != 0) {
            fmt::print(
                stdout,
                "WARNING: there is already a replica {} with the same <dst_app_name> {} exists\n",
                replica_dir,
                lpsc.dst_app_name);
        }

        // vi. Check if <src_partition_count> matches.
        RETURN_FALSE_IF_NOT(ai.partition_count == lpsc.src_partition_count,
                            "unmatched <src_partition_count> ({} vs {})",
                            ai.partition_count,
                            lpsc.src_partition_count);

        // vii. Check the app status.
        RETURN_FALSE_IF_NOT(ai.status == dsn::app_status::AS_AVAILABLE,
                            "not support to split app '{}' in non-AVAILABLE status",
                            ai.app_name);

        // viii. Check if the app is duplicating or bulk loading.
        RETURN_FALSE_IF_NOT(!ai.duplicating && !ai.is_bulk_loading,
                            "not support to split app '{}' which is duplicating or bulk loading",
                            ai.app_name);

        // ix. Load the replica_init_info.
        dsn::replication::replica_init_info rii;
        const auto rii_path =
            dsn::utils::filesystem::path_combine(replica_dir, replica_init_info::kInitInfo);
        RETURN_FALSE_IF_NON_OK(dsn::utils::load_rjobj_from_file(rii_path, &rii),
                               "load replica_init_info from '{}' failed",
                               rii_path);

        // x. Gather the replica.
        to_split_partitions.push_back({replica_dir, ai, rii, pid.get_partition_index()});
        remain_partition_ids.erase(cur_pidx);
    }

    if (!remain_partition_ids.empty()) {
        fmt::print(stdout,
                   "WARNING: the partitions {} are skipped to be split\n",
                   fmt::join(remain_partition_ids, ","));
    }

    // 4. Split the partitions.
    const auto dst_replicas_dir = dsn::utils::filesystem::path_combine(dst_data_dir, kReplicasDir);
    auto partitions_thread_pool = std::unique_ptr<rocksdb::ThreadPool>(
        rocksdb::NewThreadPool(static_cast<int>(lpsc.threads_per_data_dir)));
    ddsr.psrs.reserve(to_split_partitions.size());
    for (const auto &tsp : to_split_partitions) {
        // Statistic the partition split result.
        ddsr.psrs.emplace_back();
        auto &psr = ddsr.psrs.back();
        psr.src_replica_dir = tsp.replica_dir;

        partitions_thread_pool->SubmitJob([=, &psr]() {
            psr.success = split_partition(lpsc, tsp, dst_replicas_dir, tmp_split_replicas_dir, psr);
        });
    }
    partitions_thread_pool->WaitForJobsAndJoinAllThreads();
    if (std::any_of(ddsr.psrs.begin(), ddsr.psrs.end(), [](const PartitionSplitResult &psr) {
            return !psr.success;
        })) {
        return false;
    }
    return true;
}

bool local_partition_split(command_executor *e, shell_context *sc, arguments args)
{
    // 1. Parse parameters.
    argh::parser cmd(args.argc, args.argv);
    RETURN_FALSE_IF_NOT(cmd.pos_args().size() >= 8,
                        "invalid command, should be in the form of '{}'",
                        local_partition_split_help);
    int param_index = 1;
    LocalPartitionSplitContext lpsc;
    PARSE_STRS(lpsc.src_data_dirs);
    PARSE_STRS(lpsc.dst_data_dirs);
    PARSE_UINT(lpsc.src_app_id);
    PARSE_UINT(lpsc.dst_app_id);
    PARSE_UINTS(lpsc.src_partition_ids);
    PARSE_UINT(lpsc.src_partition_count);
    PARSE_UINT(lpsc.dst_partition_count);
    lpsc.dst_app_name = cmd(param_index++).str();
    PARSE_OPT_UINT(lpsc.threads_per_data_dir, 1, "threads_per_data_dir");
    PARSE_OPT_UINT(lpsc.threads_per_partition, 1, "threads_per_partition");
    lpsc.post_full_compact = cmd["--post_full_compact"];
    lpsc.post_count = cmd["--post_count"];

    // 2. Check parameters.
    if (!validate_parameters(lpsc)) {
        return false;
    }

    // 3. Split each data directory.
    auto data_dirs_thread_pool = std::unique_ptr<rocksdb::ThreadPool>(
        rocksdb::NewThreadPool(static_cast<int>(lpsc.src_data_dirs.size())));
    CHECK_EQ(lpsc.src_data_dirs.size(), lpsc.dst_data_dirs.size());
    std::vector<DataDirSplitResult> ddsrs;
    ddsrs.reserve(lpsc.src_data_dirs.size());
    for (auto i = 0; i < lpsc.src_data_dirs.size(); i++) {
        const auto &src_data_dir = lpsc.src_data_dirs[i];
        const auto &dst_data_dir = lpsc.dst_data_dirs[i];

        // Statistic the data directory split result.
        ddsrs.emplace_back();
        auto &ddsr = ddsrs.back();
        ddsr.src_data_dir = src_data_dir;
        ddsr.dst_data_dir = dst_data_dir;

        data_dirs_thread_pool->SubmitJob([=, &ddsr]() {
            ddsr.success = split_data_directory(lpsc, src_data_dir, dst_data_dir, ddsr);
        });
    }
    data_dirs_thread_pool->WaitForJobsAndJoinAllThreads();

    // 4. Output the result.
    dsn::utils::table_printer tp("partition_split_result");
    tp.add_title("src_replica");
    tp.add_column("dst_replica");
    tp.add_column("success");
    tp.add_column("key_count");
    for (const auto &ddsr : ddsrs) {
        for (const auto &psr : ddsr.psrs) {
            for (const auto &[new_dst_replica_dir, key_count] : psr.key_count_by_dst_replica_dirs) {
                tp.add_row(psr.src_replica_dir);
                tp.append_data(new_dst_replica_dir);
                tp.append_data(psr.success);
                tp.append_data(key_count);
            }
        }
    }
    tp.output(std::cout, tp_output_format::kTabular);

    return true;
}
