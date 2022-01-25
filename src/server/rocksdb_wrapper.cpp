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

#include "rocksdb_wrapper.h"

#include <dsn/utility/fail_point.h>
#include <rocksdb/db.h>
#include "pegasus_write_service_impl.h"
#include "base/pegasus_value_schema.h"

namespace pegasus {
namespace server {

rocksdb_wrapper::rocksdb_wrapper(pegasus_server_impl *server)
    : replica_base(server),
      _db(server->_db),
      _rd_opts(server->_data_cf_rd_opts),
      _meta_cf(server->_meta_cf),
      _pegasus_data_version(server->_pegasus_data_version),
      _pfc_recent_expire_count(server->_pfc_recent_expire_count),
      _default_ttl(0)
{
    _write_batch = dsn::make_unique<rocksdb::WriteBatch>();
    _value_generator = dsn::make_unique<pegasus_value_generator>();

    _wt_opts = dsn::make_unique<rocksdb::WriteOptions>();
    // disable write ahead logging as replication handles logging instead now
    _wt_opts->disableWAL = true;
}

int rocksdb_wrapper::get(dsn::string_view raw_key, /*out*/ db_get_context *ctx)
{
    FAIL_POINT_INJECT_F("db_get", [](dsn::string_view) -> int { return FAIL_DB_GET; });

    rocksdb::Status s = _db->Get(_rd_opts, utils::to_rocksdb_slice(raw_key), &(ctx->raw_value));
    if (dsn_likely(s.ok())) {
        // success
        ctx->found = true;
        ctx->expire_ts = pegasus_extract_expire_ts(_pegasus_data_version, ctx->raw_value);
        if (check_if_ts_expired(utils::epoch_now(), ctx->expire_ts)) {
            ctx->expired = true;
            _pfc_recent_expire_count->increment();
        }
        return rocksdb::Status::kOk;
    } else if (s.IsNotFound()) {
        // NotFound is an acceptable error
        ctx->found = false;
        return rocksdb::Status::kOk;
    }

    dsn::blob hash_key, sort_key;
    pegasus_restore_key(dsn::blob(raw_key.data(), 0, raw_key.size()), hash_key, sort_key);
    derror_rocksdb("Get",
                   s.ToString(),
                   "hash_key: {}, sort_key: {}",
                   utils::c_escape_string(hash_key),
                   utils::c_escape_string(sort_key));
    return s.code();
}

int rocksdb_wrapper::write_batch_put(int64_t decree,
                                     dsn::string_view raw_key,
                                     dsn::string_view value,
                                     uint32_t expire_sec)
{
    return write_batch_put_ctx(db_write_context::empty(decree), raw_key, value, expire_sec);
}

int rocksdb_wrapper::write_batch_put_ctx(const db_write_context &ctx,
                                         dsn::string_view raw_key,
                                         dsn::string_view value,
                                         uint32_t expire_sec)
{
    FAIL_POINT_INJECT_F("db_write_batch_put",
                        [](dsn::string_view) -> int { return FAIL_DB_WRITE_BATCH_PUT; });

    uint64_t new_timetag = ctx.remote_timetag;
    if (!ctx.is_duplicated_write()) { // local write
        new_timetag = generate_timetag(ctx.timestamp, get_cluster_id_if_exists(), false);
    }

    if (ctx.verify_timetag &&         // needs read-before-write
        _pegasus_data_version >= 1 && // data version 0 doesn't support timetag.
        !raw_key.empty()) {           // not an empty write

        db_get_context get_ctx;
        int err = get(raw_key, &get_ctx);
        if (dsn_unlikely(err != 0)) {
            return err;
        }
        // if record exists and is not expired.
        if (get_ctx.found && !get_ctx.expired) {
            uint64_t local_timetag =
                pegasus_extract_timetag(_pegasus_data_version, get_ctx.raw_value);

            if (local_timetag >= new_timetag) {
                // ignore this stale update with lower timetag,
                // and write an empty record instead
                raw_key = value = dsn::string_view();
            }
        }
    }

    rocksdb::Slice skey = utils::to_rocksdb_slice(raw_key);
    rocksdb::SliceParts skey_parts(&skey, 1);
    rocksdb::SliceParts svalue = _value_generator->generate_value(
        _pegasus_data_version, value, db_expire_ts(expire_sec), new_timetag);
    rocksdb::Status s = _write_batch->Put(skey_parts, svalue);
    if (dsn_unlikely(!s.ok())) {
        ::dsn::blob hash_key, sort_key;
        pegasus_restore_key(::dsn::blob(raw_key.data(), 0, raw_key.size()), hash_key, sort_key);
        derror_rocksdb("WriteBatchPut",
                       s.ToString(),
                       "decree: {}, hash_key: {}, sort_key: {}, expire_ts: {}",
                       ctx.decree,
                       utils::c_escape_string(hash_key),
                       utils::c_escape_string(sort_key),
                       expire_sec);
    }
    return s.code();
}

int rocksdb_wrapper::write(int64_t decree)
{
    dassert(_write_batch->Count() != 0, "the number of updates in the batch is 0");

    FAIL_POINT_INJECT_F("db_write", [](dsn::string_view) -> int { return FAIL_DB_WRITE; });

    rocksdb::Status status =
        _write_batch->Put(_meta_cf, meta_store::LAST_FLUSHED_DECREE, std::to_string(decree));
    if (dsn_unlikely(!status.ok())) {
        derror_rocksdb("Write",
                       status.ToString(),
                       "put decree of meta cf into batch error, decree: {}",
                       decree);
        return status.code();
    }

    status = _db->Write(*_wt_opts, _write_batch.get());
    if (dsn_unlikely(!status.ok())) {
        derror_rocksdb("Write", status.ToString(), "write rocksdb error, decree: {}", decree);
    }
    return status.code();
}

int rocksdb_wrapper::write_batch_delete(int64_t decree, dsn::string_view raw_key)
{
    FAIL_POINT_INJECT_F("db_write_batch_delete",
                        [](dsn::string_view) -> int { return FAIL_DB_WRITE_BATCH_DELETE; });

    rocksdb::Status s = _write_batch->Delete(utils::to_rocksdb_slice(raw_key));
    if (dsn_unlikely(!s.ok())) {
        dsn::blob hash_key, sort_key;
        pegasus_restore_key(dsn::blob(raw_key.data(), 0, raw_key.size()), hash_key, sort_key);
        derror_rocksdb("write_batch_delete",
                       s.ToString(),
                       "decree: {}, hash_key: {}, sort_key: {}",
                       decree,
                       utils::c_escape_string(hash_key),
                       utils::c_escape_string(sort_key));
    }
    return s.code();
}

void rocksdb_wrapper::clear_up_write_batch() { _write_batch->Clear(); }

int rocksdb_wrapper::ingest_files(int64_t decree,
                                  const std::vector<std::string> &sst_file_list,
                                  const bool ingest_behind)
{
    rocksdb::IngestExternalFileOptions ifo;
    ifo.move_files = true;
    ifo.ingest_behind = ingest_behind;
    rocksdb::Status s = _db->IngestExternalFile(sst_file_list, ifo);
    if (dsn_unlikely(!s.ok())) {
        derror_rocksdb("IngestExternalFile",
                       s.ToString(),
                       "decree = {}, ingest_behind = {}",
                       decree,
                       ingest_behind);
    } else {
        ddebug_rocksdb("IngestExternalFile",
                       "Ingest files succeed, decree = {}, ingest_behind = {}",
                       decree,
                       ingest_behind);
    }
    return s.code();
}

void rocksdb_wrapper::set_default_ttl(uint32_t ttl)
{
    if (_default_ttl != ttl) {
        _default_ttl = ttl;
        ddebug_replica("update _default_ttl to {}", ttl);
    }
}

uint32_t rocksdb_wrapper::db_expire_ts(uint32_t expire_ts)
{
    // use '_default_ttl' when ttl is not set for this write operation.
    if (_default_ttl != 0 && expire_ts == 0) {
        return utils::epoch_now() + _default_ttl;
    }

    return expire_ts;
}
} // namespace server
} // namespace pegasus
