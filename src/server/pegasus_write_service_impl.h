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

    void multi_put(int64_t decree,
                   const dsn::apps::multi_put_request &update,
                   dsn::apps::update_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        if (update.kvs.empty()) {
            // invalid argument
            derror_replica("invalid argument for multi_put: decree = {}, error = empty kvs",
                           decree);

            // an invalid operation shouldn't be added to latency calculation
            resp.error = rocksdb::Status::kInvalidArgument;
            return;
        }

        for (auto &kv : update.kvs) {
            resp.error = db_write_batch_put(composite_raw_key(update.hash_key, kv.key),
                                            kv.value,
                                            static_cast<uint32_t>(update.expire_ts_seconds));
            if (resp.error != 0) {
                return;
            }
        }

        resp.error = db_write(decree);
    }

    void multi_remove(int64_t decree,
                      const dsn::apps::multi_remove_request &update,
                      dsn::apps::multi_remove_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        if (update.sort_keys.empty()) {
            // invalid argument
            derror_replica(
                "invalid argument for multi_remove: decree = {}, error = empty sort keys", decree);

            // an invalid operation shouldn't be added to latency calculation
            resp.error = rocksdb::Status::kInvalidArgument;
            resp.count = 0;
            return;
        }

        for (auto &sort_key : update.sort_keys) {
            // TODO(wutao1): check returned error
            db_write_batch_delete(composite_raw_key(update.hash_key, sort_key));
        }

        resp.error = db_write(decree);
        if (resp.error != 0) {
            resp.count = 0;
        } else {
            resp.count = update.sort_keys.size();
        }
    }

    void batch_put(const dsn::apps::update_request &update, dsn::apps::update_response &resp)
    {
        resp.error = db_write_batch_put(
            update.key, update.value, static_cast<uint32_t>(update.expire_ts_seconds));
        _update_responses.emplace_back(&resp);
    }

    void batch_remove(const dsn::blob &key, dsn::apps::update_response &resp)
    {
        resp.error = db_write_batch_delete(key);
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

    int db_write_batch_put(dsn::string_view raw_key, dsn::string_view value, uint32_t expire_sec)
    {
        rocksdb::Slice skey = utils::to_rocksdb_slice(raw_key);
        rocksdb::SliceParts skey_parts(&skey, 1);
        rocksdb::SliceParts svalue =
            _value_generator.generate_value(_value_schema_version, value, expire_sec);
        _batch.Put(skey_parts, svalue);
        return 0;
    }

    int db_write_batch_delete(dsn::string_view raw_key)
    {
        _batch.Delete(utils::to_rocksdb_slice(raw_key));
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
    dsn::blob composite_raw_key(dsn::string_view hash_key, dsn::string_view sort_key)
    {
        dsn::blob raw_key;
        pegasus_generate_key(raw_key, hash_key, sort_key);
        return raw_key;
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
