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
#include <stdint.h>
#include <string>

#include "utils/error_code.h"

namespace rocksdb {
class ColumnFamilyHandle;
class DB;
} // namespace rocksdb

namespace pegasus {
namespace server {

// Manage meta data of Pegasus, now support
// - pegasus_data_version
// - pegasus_last_flushed_decree
// - pegasus_last_manual_compact_finish_time
class meta_store
{
public:
    // Column family names.
    static const std::string DATA_COLUMN_FAMILY_NAME;
    static const std::string META_COLUMN_FAMILY_NAME;

    meta_store(const char *log_prefix, rocksdb::DB *db, rocksdb::ColumnFamilyHandle *meta_cf);

    dsn::error_code get_last_flushed_decree(uint64_t *decree) const;
    uint64_t get_decree_from_readonly_db(rocksdb::DB *db,
                                         rocksdb::ColumnFamilyHandle *meta_cf) const;
    dsn::error_code get_data_version(uint32_t *version) const;
    dsn::error_code get_last_manual_compact_finish_time(uint64_t *ts) const;
    std::string get_usage_scenario() const;

    void set_last_flushed_decree(uint64_t decree) const;
    void set_data_version(uint32_t version) const;
    void set_last_manual_compact_finish_time(uint64_t last_manual_compact_finish_time) const;
    void set_usage_scenario(const std::string &usage_scenario) const;

private:
    ::dsn::error_code
    get_value_from_meta_cf(bool read_flushed_data, const std::string &key, uint64_t *value) const;
    ::dsn::error_code get_string_value_from_meta_cf(bool read_flushed_data,
                                                    const std::string &key,
                                                    std::string *value) const;
    ::dsn::error_code set_value_to_meta_cf(const std::string &key, uint64_t value) const;
    ::dsn::error_code set_string_value_to_meta_cf(const std::string &key,
                                                  const std::string &value) const;

    static ::dsn::error_code get_value_from_meta_cf(rocksdb::DB *db,
                                                    rocksdb::ColumnFamilyHandle *cf,
                                                    bool read_flushed_data,
                                                    const std::string &key,
                                                    uint64_t *value);
    static ::dsn::error_code get_string_value_from_meta_cf(rocksdb::DB *db,
                                                           rocksdb::ColumnFamilyHandle *cf,
                                                           bool read_flushed_data,
                                                           const std::string &key,
                                                           std::string *value);

    friend class pegasus_server_impl;
    friend class pegasus_write_service;
    friend class rocksdb_wrapper;
    FRIEND_TEST(pegasus_server_impl_test, test_open_db_with_latest_options);
    FRIEND_TEST(pegasus_server_impl_test, test_open_db_with_app_envs);

    const char *log_prefix() const { return _log_prefix.c_str(); }

    // Keys of meta data wrote into meta column family.
    static const std::string DATA_VERSION;
    static const std::string LAST_FLUSHED_DECREE;
    static const std::string LAST_MANUAL_COMPACT_FINISH_TIME;
    static const std::string ROCKSDB_ENV_USAGE_SCENARIO_NORMAL;
    static const std::string ROCKSDB_ENV_USAGE_SCENARIO_PREFER_WRITE;
    static const std::string ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD;

    const std::string _log_prefix;
    rocksdb::DB *_db;
    rocksdb::ColumnFamilyHandle *_meta_cf;
    rocksdb::WriteOptions _wt_opts;
};

} // namespace server
} // namespace pegasus
