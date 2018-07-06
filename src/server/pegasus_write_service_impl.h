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

class pegasus_write_service::impl : public dsn::replication::replica_base
{
public:
    explicit impl(pegasus_server_impl *server)
        : replica_base(*server),
          _primary_address(server->_primary_address),
          _value_schema_version(server->_value_schema_version),
          _db(server->_db),
          _wt_opts(&server->_wt_opts)
    {
    }

    int multi_put(int64_t decree,
                  const dsn::apps::multi_put_request &update,
                  dsn::apps::update_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        if (update.kvs.empty()) {
            derror_replica("invalid argument for multi_put: decree = {}, error = {}",
                           decree,
                           "request.kvs is empty");
            resp.error = rocksdb::Status::kInvalidArgument;
            return 0;
        }

        for (auto &kv : update.kvs) {
            resp.error = db_write_batch_put(decree,
                                            composite_raw_key(update.hash_key, kv.key),
                                            kv.value,
                                            static_cast<uint32_t>(update.expire_ts_seconds));
            RETURN_NOT_ZERO(resp.error);
        }

        resp.error = db_write(decree);
        RETURN_NOT_ZERO(resp.error);

        return 0;
    }

    int multi_remove(int64_t decree,
                     const dsn::apps::multi_remove_request &update,
                     dsn::apps::multi_remove_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        if (update.sort_keys.empty()) {
            derror_replica("invalid argument for multi_remove: decree = {}, error = {}",
                           decree,
                           "request.sort_keys is empty");
            resp.error = rocksdb::Status::kInvalidArgument;
            return 0;
        }

        for (auto &sort_key : update.sort_keys) {
            resp.error =
                db_write_batch_delete(decree, composite_raw_key(update.hash_key, sort_key));
            RETURN_NOT_ZERO(resp.error);
        }

        resp.error = db_write(decree);
        RETURN_NOT_ZERO(resp.error);

        resp.count = update.sort_keys.size();
        return 0;
    }

    int batch_put(int64_t decree,
                  const dsn::apps::update_request &update,
                  dsn::apps::update_response &resp)
    {
        resp.error = db_write_batch_put(
            decree, update.key, update.value, static_cast<uint32_t>(update.expire_ts_seconds));
        _update_responses.emplace_back(&resp);
        return resp.error;
    }

    int batch_remove(int64_t decree, const dsn::blob &key, dsn::apps::update_response &resp)
    {
        resp.error = db_write_batch_delete(decree, key);
        _update_responses.emplace_back(&resp);
        return resp.error;
    }

    int batch_commit(int64_t decree)
    {
        int err = db_write(decree);
        clear_up_batch_states(decree, err);
        return err;
    }

    void batch_abort(int64_t decree, int err) { clear_up_batch_states(decree, err); }

    int db_write_batch_put(int64_t decree,
                           dsn::string_view raw_key,
                           dsn::string_view value,
                           uint32_t expire_sec)
    {
        rocksdb::Slice skey = utils::to_rocksdb_slice(raw_key);
        rocksdb::SliceParts skey_parts(&skey, 1);
        rocksdb::SliceParts svalue =
            _value_generator.generate_value(_value_schema_version, value, expire_sec);
        rocksdb::Status s = _batch.Put(skey_parts, svalue);
        if (dsn_unlikely(!s.ok())) {
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(::dsn::blob(raw_key.data(), 0, raw_key.size()), hash_key, sort_key);
            derror_rocksdb("WriteBatchPut",
                           s.ToString(),
                           "decree: {}, hash_key: {}, sort_key: {}, expire_ts: {}",
                           decree,
                           utils::c_escape_string(hash_key),
                           utils::c_escape_string(sort_key),
                           expire_sec);
        }
        return s.code();
    }

    int db_write_batch_delete(int64_t decree, dsn::string_view raw_key)
    {
        rocksdb::Status s = _batch.Delete(utils::to_rocksdb_slice(raw_key));
        if (dsn_unlikely(!s.ok())) {
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(::dsn::blob(raw_key.data(), 0, raw_key.size()), hash_key, sort_key);
            derror_rocksdb("WriteBatchDelete",
                           s.ToString(),
                           "decree: {}, hash_key: {}, sort_key: {}",
                           decree,
                           utils::c_escape_string(hash_key),
                           utils::c_escape_string(sort_key));
        }
        return s.code();
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
            derror_rocksdb("Write", status.ToString(), "decree: {}", decree);
        }
        return status.code();
    }

private:
    dsn::blob composite_raw_key(dsn::string_view hash_key, dsn::string_view sort_key)
    {
        dsn::blob raw_key;
        pegasus_generate_key(raw_key, hash_key, sort_key);
        return raw_key;
    }

    void clear_up_batch_states(int64_t decree, int err)
    {
        dsn::apps::update_response resp;
        resp.error = err;
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;
        for (dsn::apps::update_response *uresp : _update_responses) {
            *uresp = resp;
        }
        _update_responses.clear();
        _batch.Clear();
    }

private:
    friend class pegasus_write_service_test;

    const std::string _primary_address;
    const uint32_t _value_schema_version;

    rocksdb::WriteBatch _batch;
    rocksdb::DB *_db;
    rocksdb::WriteOptions *_wt_opts;

    pegasus_value_generator _value_generator;

    // for setting update_response.error after committed.
    std::vector<dsn::apps::update_response *> _update_responses;
};

} // namespace server
} // namespace pegasus
