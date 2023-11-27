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

#include <gtest/gtest_prod.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "pegasus_value_schema.h"
#include "replica/replica_base.h"
#include "absl/strings/string_view.h"

namespace rocksdb {
class ColumnFamilyHandle;
class DB;
} // namespace rocksdb

namespace dsn {
class perf_counter_wrapper;
} // namespace dsn

namespace pegasus {

namespace server {
class pegasus_server_impl;
struct db_get_context;
struct db_write_context;

class rocksdb_wrapper : public dsn::replication::replica_base
{
public:
    rocksdb_wrapper(pegasus_server_impl *server);

    /// Calls RocksDB Get and store the result into `db_get_context`.
    /// \returns rocksdb::Status::kOk if Get succeeded. On failure, a non-zero rocksdb status code
    /// is returned.
    /// \result ctx.expired=true if record expired. Still rocksdb::Status::kOk is returned.
    /// \result ctx.found=false if record is not found. Still rocksdb::Status::kOk is returned.
    int get(absl::string_view raw_key, /*out*/ db_get_context *ctx);

    int write_batch_put(int64_t decree,
                        absl::string_view raw_key,
                        absl::string_view value,
                        uint32_t expire_sec);
    int write_batch_put_ctx(const db_write_context &ctx,
                            absl::string_view raw_key,
                            absl::string_view value,
                            uint32_t expire_sec);
    int write(int64_t decree);
    int write_batch_delete(int64_t decree, absl::string_view raw_key);
    void clear_up_write_batch();
    int ingest_files(int64_t decree,
                     const std::vector<std::string> &sst_file_list,
                     const bool ingest_behind);

    void set_default_ttl(uint32_t ttl);

private:
    uint32_t db_expire_ts(uint32_t expire_ts);

    rocksdb::DB *_db;
    rocksdb::ReadOptions &_rd_opts;
    std::unique_ptr<pegasus_value_generator> _value_generator;
    std::unique_ptr<rocksdb::WriteBatch> _write_batch;
    std::unique_ptr<rocksdb::WriteOptions> _wt_opts;
    rocksdb::ColumnFamilyHandle *_meta_cf;

    const uint32_t _pegasus_data_version;
    dsn::perf_counter_wrapper &_pfc_recent_expire_count;
    volatile uint32_t _default_ttl;

    friend class rocksdb_wrapper_test;
    friend class pegasus_write_service_test;
    friend class pegasus_server_write_test;
    FRIEND_TEST(rocksdb_wrapper_test, put_verify_timetag);
    FRIEND_TEST(rocksdb_wrapper_test, verify_timetag_compatible_with_version_0);
    FRIEND_TEST(rocksdb_wrapper_test, get);
};
} // namespace server
} // namespace pegasus
