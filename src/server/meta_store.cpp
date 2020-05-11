// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "meta_store.h"

#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/flags.h>

namespace pegasus {
namespace server {

DSN_DEFINE_string("pegasus.server", get_meta_store_type, "manifest",
    "Where to get meta data, now support 'manifest' and 'metacf'");
DSN_DEFINE_validator(get_meta_store_type, [](const char *type) {
    return strcmp(type, "manifest") == 0 || strcmp(type, "metacf") == 0;
});

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
    _get_meta_store_type = (strcmp(FLAGS_get_meta_store_type, "manifest") == 0 ? meta_store_type::kManifestOnly : meta_store_type::kMetaCFOnly);
}

uint64_t meta_store::get_last_flushed_decree() const
{
    switch (_get_meta_store_type) {
    case meta_store_type::kManifestOnly:
        return _db->GetLastFlushedDecree();
    case meta_store_type::kMetaCFOnly: {
        uint64_t last_flushed_decree = 0;
        auto ec = get_value_from_meta_cf(true, LAST_FLUSHED_DECREE, &last_flushed_decree);
        dcheck_eq_replica(::dsn::ERR_OK, ec);
        return last_flushed_decree;
    }
    default:
        __builtin_unreachable();
    }
}

uint32_t meta_store::get_data_version() const
{
    switch (_get_meta_store_type) {
    case meta_store_type::kManifestOnly:
        return _db->GetPegasusDataVersion();
    case meta_store_type::kMetaCFOnly: {
        uint64_t pegasus_data_version = 0;
        auto ec = get_value_from_meta_cf(false, DATA_VERSION, &pegasus_data_version);
        dcheck_eq_replica(::dsn::ERR_OK, ec);
        return static_cast<uint32_t>(pegasus_data_version);
    }
    default:
        __builtin_unreachable();
    }
}

uint64_t meta_store::get_last_manual_compact_finish_time() const
{
    switch (_get_meta_store_type) {
    case meta_store_type::kManifestOnly:
        return _db->GetLastManualCompactFinishTime();
    case meta_store_type::kMetaCFOnly: {
        uint64_t last_manual_compact_finish_time = 0;
        auto ec = get_value_from_meta_cf(
            false, LAST_MANUAL_COMPACT_FINISH_TIME, &last_manual_compact_finish_time);
        dcheck_eq_replica(::dsn::ERR_OK, ec);
        return last_manual_compact_finish_time;
    }
    default:
        __builtin_unreachable();
    }
}

::dsn::error_code meta_store::get_value_from_meta_cf(bool read_flushed_data,
                                                     const std::string &key,
                                                     uint64_t *value) const
{
    std::string data;
    rocksdb::ReadOptions rd_opts;
    if (read_flushed_data) {
        // only read 'flushed' data, mainly to read 'last_flushed_decree'
        rd_opts.read_tier = rocksdb::kPersistedTier;
    }
    auto status = _db->Get(rd_opts, _meta_cf, key, &data);
    if (status.ok()) {
        bool ok = dsn::buf2uint64(data, *value);
        dassert_replica(ok,
                        "rocksdb {} get {} from meta column family got error value {}",
                        _db->GetName(),
                        key,
                        data);
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
    auto status = _db->Put(_wt_opts, _meta_cf, key, std::to_string(value));
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

} // namespace server
} // namespace pegasus
