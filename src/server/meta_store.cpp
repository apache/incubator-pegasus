// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "meta_store.h"

#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/flags.h>

namespace pegasus {
namespace server {

const std::string meta_store::DATA_VERSION = "pegasus_data_version";
const std::string meta_store::LAST_FLUSHED_DECREE = "pegasus_last_flushed_decree";
const std::string meta_store::LAST_MANUAL_COMPACT_FINISH_TIME =
    "pegasus_last_manual_compact_finish_time";

meta_store::meta_store(pegasus_server_impl *server,
                       rocksdb::DB *db,
                       rocksdb::ColumnFamilyHandle *meta_cf)
    : replica_base(server), _db(db), _meta_cf(meta_cf)
{
    // disable write ahead logging as replication handles logging instead now
    _wt_opts.disableWAL = true;
}

uint64_t meta_store::get_last_flushed_decree() const
{
    uint64_t last_flushed_decree = 0;
    auto ec = get_value_from_meta_cf(true, LAST_FLUSHED_DECREE, &last_flushed_decree);
    dcheck_eq_replica(::dsn::ERR_OK, ec);
    return last_flushed_decree;
}

uint32_t meta_store::get_data_version() const
{
    uint64_t pegasus_data_version = 0;
    auto ec = get_value_from_meta_cf(false, DATA_VERSION, &pegasus_data_version);
    dcheck_eq_replica(::dsn::ERR_OK, ec);
    return static_cast<uint32_t>(pegasus_data_version);
}

uint64_t meta_store::get_last_manual_compact_finish_time() const
{
    uint64_t last_manual_compact_finish_time = 0;
    auto ec = get_value_from_meta_cf(
        false, LAST_MANUAL_COMPACT_FINISH_TIME, &last_manual_compact_finish_time);
    dcheck_eq_replica(::dsn::ERR_OK, ec);
    return last_manual_compact_finish_time;
}

uint64_t meta_store::get_decree_from_readonly_db(rocksdb::DB *db,
                                                 rocksdb::ColumnFamilyHandle *meta_cf) const
{
    std::string str_last_flushed_decree;
    uint64_t last_flushed_decree = 0;
    auto ec = get_string_value_from_meta_cf(
        db, meta_cf, true, LAST_FLUSHED_DECREE, &str_last_flushed_decree);
    dcheck_eq_replica(::dsn::ERR_OK, ec);
    dassert_f(dsn::buf2uint64(str_last_flushed_decree, last_flushed_decree),
              "rocksdb {} get LAST_FLUSHED_DECREE from meta column family got error value {}",
              db->GetName(),
              str_last_flushed_decree);
    return last_flushed_decree;
}

std::string meta_store::get_usage_scenario() const
{
    std::string usage_scenario;
    auto ec = get_string_value_from_meta_cf(false, ROCKSDB_ENV_USAGE_SCENARIO_KEY, &usage_scenario);
    dcheck_eq_replica(::dsn::ERR_OK, ec);
    return usage_scenario;
}

::dsn::error_code meta_store::get_value_from_meta_cf(bool read_flushed_data,
                                                     const std::string &key,
                                                     uint64_t *value) const
{
    std::string data;
    auto ec = get_string_value_from_meta_cf(_db, _meta_cf, read_flushed_data, key, &data);
    if (ec != ::dsn::ERR_OK) {
        return ec;
    }
    dassert_f(dsn::buf2uint64(data, *value),
              "rocksdb {} get {} from meta column family got error value {}",
              _db->GetName(),
              key,
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
        derror_replica(
            "Put {}={} to meta column family failed, status {}", key, value, status.ToString());
        // TODO(yingchun): add a rocksdb io error.
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }
    return ::dsn::ERR_OK;
}

void meta_store::set_last_flushed_decree(uint64_t decree) const
{
    dcheck_eq_replica(::dsn::ERR_OK, set_value_to_meta_cf(LAST_FLUSHED_DECREE, decree));
}

void meta_store::set_data_version(uint32_t version) const
{
    dcheck_eq_replica(::dsn::ERR_OK, set_value_to_meta_cf(DATA_VERSION, version));
}

void meta_store::set_last_manual_compact_finish_time(uint64_t last_manual_compact_finish_time) const
{
    dcheck_eq_replica(
        ::dsn::ERR_OK,
        set_value_to_meta_cf(LAST_MANUAL_COMPACT_FINISH_TIME, last_manual_compact_finish_time));
}

void meta_store::set_usage_scenario(const std::string &usage_scenario) const
{
    dcheck_eq_replica(::dsn::ERR_OK,
                      set_string_value_to_meta_cf(ROCKSDB_ENV_USAGE_SCENARIO_KEY, usage_scenario));
}

} // namespace server
} // namespace pegasus
