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

#include "meta_store.h"

#include <rocksdb/db.h>
#include <rocksdb/status.h>

#include "common/replica_envs.h"
#include "utils/fmt_logging.h"
#include "utils/string_conv.h"

namespace pegasus {
namespace server {
const std::string meta_store::DATA_COLUMN_FAMILY_NAME = "default";
const std::string meta_store::META_COLUMN_FAMILY_NAME = "pegasus_meta_cf";
const std::string meta_store::DATA_VERSION = "pegasus_data_version";
const std::string meta_store::LAST_FLUSHED_DECREE = "pegasus_last_flushed_decree";
const std::string meta_store::LAST_MANUAL_COMPACT_FINISH_TIME =
    "pegasus_last_manual_compact_finish_time";

meta_store::meta_store(const char *log_prefix,
                       rocksdb::DB *db,
                       rocksdb::ColumnFamilyHandle *meta_cf)
    : _log_prefix(log_prefix), _db(db), _meta_cf(meta_cf)
{
    // disable write ahead logging as replication handles logging instead now
    _wt_opts.disableWAL = true;
}

dsn::error_code meta_store::get_last_flushed_decree(uint64_t *decree) const
{
    LOG_AND_RETURN_NOT_OK(ERROR_PREFIX,
                          get_value_from_meta_cf(true, LAST_FLUSHED_DECREE, decree),
                          "get_value_from_meta_cf failed");
    return dsn::ERR_OK;
}

dsn::error_code meta_store::get_data_version(uint32_t *version) const
{
    uint64_t pegasus_data_version = 0;
    LOG_AND_RETURN_NOT_OK(ERROR_PREFIX,
                          get_value_from_meta_cf(false, DATA_VERSION, &pegasus_data_version),
                          "get_value_from_meta_cf failed");
    *version = static_cast<uint32_t>(pegasus_data_version);
    return dsn::ERR_OK;
}

dsn::error_code meta_store::get_last_manual_compact_finish_time(uint64_t *ts) const
{
    LOG_AND_RETURN_NOT_OK(ERROR_PREFIX,
                          get_value_from_meta_cf(false, LAST_MANUAL_COMPACT_FINISH_TIME, ts),
                          "get_value_from_meta_cf failed");
    return dsn::ERR_OK;
}

uint64_t meta_store::get_decree_from_readonly_db(rocksdb::DB *db,
                                                 rocksdb::ColumnFamilyHandle *meta_cf) const
{
    uint64_t last_flushed_decree = 0;
    auto ec = get_value_from_meta_cf(db, meta_cf, true, LAST_FLUSHED_DECREE, &last_flushed_decree);
    CHECK_EQ_PREFIX(::dsn::ERR_OK, ec);
    return last_flushed_decree;
}

std::string meta_store::get_usage_scenario() const
{
    // If couldn't find rocksdb usage scenario in meta column family, return normal in default.
    std::string usage_scenario = dsn::replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_NORMAL;
    auto ec = get_string_value_from_meta_cf(
        false, dsn::replica_envs::ROCKSDB_USAGE_SCENARIO, &usage_scenario);
    CHECK_PREFIX_MSG(ec == ::dsn::ERR_OK || ec == ::dsn::ERR_OBJECT_NOT_FOUND,
                     "rocksdb {} get {} from meta column family failed: {}",
                     _db->GetName(),
                     dsn::replica_envs::ROCKSDB_USAGE_SCENARIO,
                     ec);
    return usage_scenario;
}

::dsn::error_code meta_store::get_value_from_meta_cf(bool read_flushed_data,
                                                     const std::string &key,
                                                     uint64_t *value) const
{
    return get_value_from_meta_cf(_db, _meta_cf, read_flushed_data, key, value);
}

::dsn::error_code meta_store::get_value_from_meta_cf(rocksdb::DB *db,
                                                     rocksdb::ColumnFamilyHandle *cf,
                                                     bool read_flushed_data,
                                                     const std::string &key,
                                                     uint64_t *value)
{
    std::string data;
    auto ec = get_string_value_from_meta_cf(db, cf, read_flushed_data, key, &data);
    if (ec != ::dsn::ERR_OK) {
        return ec;
    }
    CHECK(dsn::buf2uint64(data, *value),
          "rocksdb {} get \"{}\" from meta column family failed to parse into uint64",
          db->GetName(),
          data);
    return ::dsn::ERR_OK;
}

::dsn::error_code meta_store::get_string_value_from_meta_cf(bool read_flushed_data,
                                                            const std::string &key,
                                                            std::string *value) const
{
    return get_string_value_from_meta_cf(_db, _meta_cf, read_flushed_data, key, value);
}

::dsn::error_code meta_store::get_string_value_from_meta_cf(rocksdb::DB *db,
                                                            rocksdb::ColumnFamilyHandle *cf,
                                                            bool read_flushed_data,
                                                            const std::string &key,
                                                            std::string *value)
{
    rocksdb::ReadOptions rd_opts;
    if (read_flushed_data) {
        // only read 'flushed' data, mainly to read 'last_flushed_decree'
        rd_opts.read_tier = rocksdb::kPersistedTier;
    }
    auto status = db->Get(rd_opts, cf, key, value);
    if (status.ok()) {
        return ::dsn::ERR_OK;
    }

    if (status.IsNotFound()) {
        return ::dsn::ERR_OBJECT_NOT_FOUND;
    }

    // TODO(yingchun): add a rocksdb io error.
    return ::dsn::ERR_LOCAL_APP_FAILURE;
}

::dsn::error_code meta_store::set_value_to_meta_cf(const std::string &key, uint64_t value) const
{
    return set_string_value_to_meta_cf(key, std::to_string(value));
}

::dsn::error_code meta_store::set_string_value_to_meta_cf(const std::string &key,
                                                          const std::string &value) const
{
    auto status = _db->Put(_wt_opts, _meta_cf, key, value);
    if (!status.ok()) {
        LOG_ERROR_PREFIX(
            "Put {}={} to meta column family failed, status {}", key, value, status.ToString());
        // TODO(yingchun): add a rocksdb io error.
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }
    return ::dsn::ERR_OK;
}

void meta_store::set_last_flushed_decree(uint64_t decree) const
{
    CHECK_EQ_PREFIX(::dsn::ERR_OK, set_value_to_meta_cf(LAST_FLUSHED_DECREE, decree));
}

void meta_store::set_data_version(uint32_t version) const
{
    CHECK_EQ_PREFIX(::dsn::ERR_OK, set_value_to_meta_cf(DATA_VERSION, version));
}

void meta_store::set_last_manual_compact_finish_time(uint64_t last_manual_compact_finish_time) const
{
    CHECK_EQ_PREFIX(
        ::dsn::ERR_OK,
        set_value_to_meta_cf(LAST_MANUAL_COMPACT_FINISH_TIME, last_manual_compact_finish_time));
}

void meta_store::set_usage_scenario(const std::string &usage_scenario) const
{
    CHECK_EQ_PREFIX(
        ::dsn::ERR_OK,
        set_string_value_to_meta_cf(dsn::replica_envs::ROCKSDB_USAGE_SCENARIO, usage_scenario));
}

} // namespace server
} // namespace pegasus
