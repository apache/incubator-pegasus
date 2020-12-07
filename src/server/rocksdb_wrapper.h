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

#include <dsn/dist/replication/replica_base.h>

namespace rocksdb {
class DB;
class ReadOptions;
} // namespace rocksdb

namespace dsn {
class perf_counter_wrapper;
} // namespace dsn

namespace pegasus {
namespace server {
struct db_get_context;
class pegasus_server_impl;

class rocksdb_wrapper : public dsn::replication::replica_base
{
public:
    rocksdb_wrapper(pegasus_server_impl *server);

    /// Calls RocksDB Get and store the result into `db_get_context`.
    /// \returns 0 if Get succeeded. On failure, a non-zero rocksdb status code is returned.
    /// \result ctx.expired=true if record expired. Still 0 is returned.
    /// \result ctx.found=false if record is not found. Still 0 is returned.
    int get(dsn::string_view raw_key, /*out*/ db_get_context *ctx);

private:
    rocksdb::DB *_db;
    rocksdb::ReadOptions &_rd_opts;
    const uint32_t _pegasus_data_version;
    dsn::perf_counter_wrapper &_pfc_recent_expire_count;
};
} // namespace server
} // namespace pegasus
