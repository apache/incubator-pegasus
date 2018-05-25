// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "pegasus_write_service.h"
#include "pegasus_server_impl.h"
#include "logging_utils.h"

#include "base/pegasus_key_schema.h"

namespace pegasus {
namespace server {

static inline dsn::blob composite_raw_key(dsn::string_view hash_key, dsn::string_view sort_key)
{
    dsn::blob raw_key;
    pegasus_generate_key(raw_key, hash_key, sort_key);
    return raw_key;
}

class pegasus_write_service::impl : public dsn::replication::replica_base
{
public:
    explicit impl(pegasus_server_impl *server)
        : replica_base(server),
          _primary_address(server->_primary_address),
          _value_schema_version(server->_value_schema_version),
          _verify_timetag(false),
          _db(server->_db),
          _wt_opts(&server->_wt_opts),
          _rd_opts(&server->_rd_opts)
    {
        const_cast<bool &>(_verify_timetag) = dsn_config_get_value_bool(
            "pegasus.server", "verify_timetag", false, "verify_timetag, default false");
    }

    void multi_put(const db_write_context &ctx,
                   const dsn::apps::multi_put_request &update,
                   dsn::apps::update_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = ctx.decree;
        resp.server = _primary_address;

        if (update.kvs.empty()) {
            // invalid argument
            derror_replica("invalid argument for multi_put: decree = {}, error = empty kvs",
                           ctx.decree);

            // an invalid operation shouldn't be added to latency calculation
            resp.error = rocksdb::Status::kInvalidArgument;
            return;
        }

        for (auto &kv : update.kvs) {
            resp.error = db_write_batch_put(ctx,
                                            composite_raw_key(update.hash_key, kv.key),
                                            kv.value,
                                            static_cast<uint32_t>(update.expire_ts_seconds));
            if (resp.error != 0) {
                return;
            }
        }

        resp.error = db_write(ctx.decree);
    }

    void multi_remove(const db_write_context &ctx,
                      const dsn::apps::multi_remove_request &update,
                      dsn::apps::multi_remove_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = ctx.decree;
        resp.server = _primary_address;

        if (update.sort_keys.empty()) {
            // invalid argument
            derror_replica(
                "invalid argument for multi_remove: decree = {}, error = empty sort keys",
                ctx.decree);

            // an invalid operation shouldn't be added to latency calculation
            resp.error = rocksdb::Status::kInvalidArgument;
            resp.count = 0;
            return;
        }

        for (auto &sort_key : update.sort_keys) {
            // TODO(wutao1): check returned error
            db_write_batch_delete(composite_raw_key(update.hash_key, sort_key), ctx);
        }

        resp.error = db_write(ctx.decree);
        if (resp.error != 0) {
            resp.count = 0;
        } else {
            resp.count = update.sort_keys.size();
        }
    }

    inline void batch_put(const db_write_context &ctx,
                          const dsn::apps::update_request &update,
                          dsn::apps::update_response &resp)
    {
        resp.error = db_write_batch_put(
            ctx, update.key, update.value, static_cast<uint32_t>(update.expire_ts_seconds));
        _update_responses.emplace_back(&resp);
    }

    inline void batch_remove(const db_write_context &ctx,
                             const dsn::blob &key,
                             dsn::apps::update_response &resp)
    {
        resp.error = db_write_batch_delete(key, ctx);
        _update_responses.emplace_back(&resp);
    }

    int batch_commit(int64_t decree)
    {
        int err = db_write(decree);

        dsn::apps::update_response resp;
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        for (dsn::apps::update_response *uresp : _update_responses) {
            *uresp = resp;
        }
        _update_responses.clear();
        return err;
    }

    // Each of the duplicated update has a timetag (call it `remote_timetag`) that's used
    // to keep consistency between remote cluster and local cluster.
    // Before applying the update, if the record on storage has a larger timetag than
    // the `remote_timetag`, the request will be ignored, otherwise it will be applied
    // using the *new* timetag generated by local cluster.

    int db_write_batch_put(const db_write_context &ctx,
                           dsn::string_view raw_key,
                           dsn::string_view value,
                           uint32_t expire_sec)
    {
        bool is_remote_update = ctx.remote_timetag > 0;
        uint64_t new_timetag = is_remote_update ? ctx.remote_timetag : ctx.timetag;

        if (_verify_timetag) {
            std::string raw_value;
            rocksdb::Status s = _db->Get(*_rd_opts, to_rocksdb_slice(raw_key), &raw_value);
            if (s.ok()) {
                uint64_t local_timetag = pegasus_extract_timetag(_value_schema_version, raw_value);
                if (local_timetag == new_timetag && is_remote_update) {
                    /// ignore if this is a retry attempt from remote.
                    return 0;
                }
                dassert_replica(local_timetag != new_timetag,
                                "timestamps are generated having the same value: [timetag:{}, "
                                "raw_key:{}, raw_value:{}]",
                                local_timetag,
                                raw_key,
                                raw_value);

                if (local_timetag > new_timetag) {
                    ddebug_replica("ignored a stale update with lower timetag [new: "
                                   "{}, local: {}, from_remote: {}]",
                                   new_timetag,
                                   local_timetag,
                                   is_remote_update);
                    return 0;
                }
            } else if (s.code() != rocksdb::Status::kNotFound) {
                derror_rocksdb("get",
                               s.ToString(),
                               "raw_key(len: {}, data: {})",
                               raw_key.length(),
                               raw_key.data());
                return s.code();
            }
        }

        rocksdb::Slice skey = to_rocksdb_slice(raw_key);
        rocksdb::SliceParts skey_parts(&skey, 1);
        rocksdb::SliceParts svalue =
            _value_generator.generate_value(_value_schema_version, value, expire_sec, new_timetag);
        _batch.Put(skey_parts, svalue);
        return 0;
    }

    int db_write_batch_delete(dsn::string_view raw_key, const db_write_context &ctx)
    {
        _batch.Delete(to_rocksdb_slice(raw_key));
        return 0;
    }

    // Apply the write batch into rocksdb.
    int db_write(int64_t decree)
    {
        if (_batch.Count() == 0) {
            return 0;
        }

        _wt_opts->given_decree = static_cast<uint64_t>(decree);
        auto status = _db->Write(*_wt_opts, &_batch);
        if (!status.ok()) {
            derror_rocksdb("write", status.ToString(), "decree: {}", decree);
        }
        _batch.Clear();
        return status.code();
    }

private:
    const std::string _primary_address;
    const int _value_schema_version;
    const bool _verify_timetag;

    rocksdb::WriteBatch _batch;
    rocksdb::DB *_db;
    rocksdb::WriteOptions *_wt_opts;
    const rocksdb::ReadOptions *_rd_opts;

    pegasus_value_generator _value_generator;

    // for setting update_response.error after committed.
    std::vector<dsn::apps::update_response *> _update_responses;
};

} // namespace server
} // namespace pegasus
