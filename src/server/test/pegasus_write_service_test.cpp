// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "base/pegasus_key_schema.h"
#include "pegasus_server_test_base.h"
#include "server/pegasus_server_write.h"
#include "server/pegasus_write_service_impl.h"

namespace pegasus {
namespace server {

class pegasus_write_service_test : public pegasus_server_test_base
{
protected:
    pegasus_write_service *_write_svc;
    std::unique_ptr<pegasus_server_write> _server_write;

public:
    pegasus_write_service_test() : pegasus_server_test_base()
    {
        start();
        _server_write = dsn::make_unique<pegasus_server_write>(_server.get(), true);
        _write_svc = _server_write->_write_svc.get();
    }

    void set_app_duplicating()
    {
        dsn::replication::replica_app_set_duplicating(_replica, _server.get(), true);
    }

    void test_multi_put()
    {
        dsn::fail::setup();

        dsn::apps::multi_put_request request;
        dsn::apps::update_response response;

        int64_t decree = 10;
        std::string hash_key = "hash_key";

        // alarm for empty request
        request.hash_key = dsn::blob(hash_key.data(), 0, hash_key.size());
        auto ctx = db_write_context::create(decree, 1000);
        int err = _write_svc->multi_put(ctx, request, response);
        ASSERT_EQ(err, 0);
        verify_response(response, rocksdb::Status::kInvalidArgument, decree);

        constexpr int kv_num = 100;
        std::string sort_key[kv_num];
        std::string value[kv_num];

        for (int i = 0; i < 100; i++) {
            sort_key[i] = "sort_key_" + std::to_string(i);
            value[i] = "value_" + std::to_string(i);
        }

        for (int i = 0; i < 100; i++) {
            request.kvs.emplace_back();
            request.kvs.back().key.assign(sort_key[i].data(), 0, sort_key[i].size());
            request.kvs.back().value.assign(value[i].data(), 0, value[i].size());
        }

        {
            dsn::fail::cfg("db_write_batch_put", "100%1*return()");
            err = _write_svc->multi_put(ctx, request, response);
            ASSERT_EQ(err, FAIL_DB_WRITE_BATCH_PUT);
            verify_response(response, err, decree);
        }

        {
            dsn::fail::cfg("db_write", "100%1*return()");
            err = _write_svc->multi_put(ctx, request, response);
            ASSERT_EQ(err, FAIL_DB_WRITE);
            verify_response(response, err, decree);
        }

        { // success
            err = _write_svc->multi_put(ctx, request, response);
            ASSERT_EQ(err, 0);
            verify_response(response, 0, decree);
        }

        dsn::fail::teardown();
    }

    void test_multi_remove()
    {
        dsn::fail::setup();

        dsn::apps::multi_remove_request request;
        dsn::apps::multi_remove_response response;

        int64_t decree = 10;
        std::string hash_key = "hash_key";

        // alarm for empty request
        request.hash_key = dsn::blob(hash_key.data(), 0, hash_key.size());
        int err = _write_svc->multi_remove(decree, request, response);
        ASSERT_EQ(err, 0);
        verify_response(response, rocksdb::Status::kInvalidArgument, decree);

        constexpr int kv_num = 100;
        std::string sort_key[kv_num];

        for (int i = 0; i < kv_num; i++) {
            sort_key[i] = "sort_key_" + std::to_string(i);
        }

        for (int i = 0; i < kv_num; i++) {
            request.sort_keys.emplace_back();
            request.sort_keys.back().assign(sort_key[i].data(), 0, sort_key[i].size());
        }

        {
            dsn::fail::cfg("db_write_batch_delete", "100%1*return()");
            err = _write_svc->multi_remove(decree, request, response);
            ASSERT_EQ(err, FAIL_DB_WRITE_BATCH_DELETE);
            verify_response(response, err, decree);
        }

        {
            dsn::fail::cfg("db_write", "100%1*return()");
            err = _write_svc->multi_remove(decree, request, response);
            ASSERT_EQ(err, FAIL_DB_WRITE);
            verify_response(response, err, decree);
        }

        { // success
            err = _write_svc->multi_remove(decree, request, response);
            ASSERT_EQ(err, 0);
            verify_response(response, 0, decree);
        }

        dsn::fail::teardown();
    }

    void test_batched_writes()
    {
        int64_t decree = 10;
        std::string hash_key = "hash_key";

        auto ctx = db_write_context::create(decree, 1000);

        constexpr int kv_num = 100;
        dsn::blob key[kv_num];
        std::string value[kv_num];

        for (int i = 0; i < kv_num; i++) {
            std::string sort_key = "sort_key_" + std::to_string(i);
            pegasus::pegasus_generate_key(key[i], hash_key, sort_key);

            value[i] = "value_" + std::to_string(i);
        }

        // It's dangerous to use std::vector<> here, since the address
        // of response may be changed due to capacity increase.
        std::array<dsn::apps::update_response, kv_num> responses;
        {
            _write_svc->batch_prepare(decree);
            for (int i = 0; i < kv_num; i++) {
                dsn::apps::update_request req;
                req.key = key[i];
                _write_svc->batch_put(ctx, req, responses[i]);
            }
            for (int i = 0; i < kv_num; i++) {
                _write_svc->batch_remove(decree, key[i], responses[i]);
            }
            _write_svc->batch_commit(decree);
        }

        for (const dsn::apps::update_response &resp : responses) {
            verify_response(resp, 0, decree);
        }
    }

    template <typename TResponse>
    void verify_response(const TResponse &response, int err, int64_t decree)
    {
        ASSERT_EQ(response.error, err);
        ASSERT_EQ(response.app_id, _gpid.get_app_id());
        ASSERT_EQ(response.partition_index, _gpid.get_partition_index());
        ASSERT_EQ(response.decree, decree);
        ASSERT_EQ(response.server, _write_svc->_impl->_primary_address);
        ASSERT_EQ(_write_svc->_impl->_batch.Count(), 0);
        ASSERT_EQ(_write_svc->_impl->_update_responses.size(), 0);
    }

    void test_put_verify_timetag()
    {
        set_app_duplicating();
        auto write_impl = _write_svc->_impl.get();
        const_cast<bool &>(write_impl->_verify_timetag) = true;

        dsn::blob raw_key;
        pegasus::pegasus_generate_key(
            raw_key, dsn::string_view("hash_key"), dsn::string_view("sort_key"));
        std::string value = "value";
        int64_t decree = 10;

        /// insert timestamp 10
        uint64_t timestamp = 10;
        auto ctx = db_write_context::create(decree, timestamp);
        ASSERT_EQ(0, write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
        ASSERT_EQ(0, write_impl->db_write(ctx.decree));
        write_impl->clear_up_batch_states(decree, 0);
        ASSERT_EQ(read_timestamp_from(raw_key), timestamp);

        /// insert timestamp 15, which overwrites the previous record
        timestamp = 15;
        ctx = db_write_context::create(decree, timestamp);
        ASSERT_EQ(0, write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
        ASSERT_EQ(0, write_impl->db_write(ctx.decree));
        write_impl->clear_up_batch_states(decree, 0);
        ASSERT_EQ(read_timestamp_from(raw_key), timestamp);

        /// insert timestamp 11, which will be ignored
        uint64_t old_timestamp = timestamp;
        timestamp = 11;
        ctx = db_write_context::create(decree, timestamp);
        ASSERT_EQ(0, write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
        ASSERT_EQ(0, write_impl->db_write(ctx.decree));
        write_impl->clear_up_batch_states(decree, 0);
        ASSERT_EQ(read_timestamp_from(raw_key), old_timestamp);

        /// insert timestamp 15 from remote, which will overwrite the previous record,
        /// since its cluster id is larger (current cluster_id=1)
        timestamp = 15;
        ctx.remote_timetag = pegasus::generate_timetag(timestamp, 2, false);
        ASSERT_EQ(0, write_impl->db_write_batch_put_ctx(ctx, raw_key, value + "_new", 0));
        ASSERT_EQ(0, write_impl->db_write(ctx.decree));
        write_impl->clear_up_batch_states(decree, 0);
        ASSERT_EQ(read_timestamp_from(raw_key), timestamp);
        std::string raw_value;
        dsn::blob user_value;
        rocksdb::Status s = write_impl->_db->Get(
            write_impl->_rd_opts, utils::to_rocksdb_slice(raw_key), &raw_value);
        pegasus_extract_user_data(
            write_impl->_value_schema_version, std::move(raw_value), user_value);
        ASSERT_EQ(user_value.to_string(), "value_new");

        // write retry
        ASSERT_EQ(0, write_impl->db_write_batch_put_ctx(ctx, raw_key, value + "_new", 0));
        ASSERT_EQ(0, write_impl->db_write(ctx.decree));
        write_impl->clear_up_batch_states(decree, 0);

        /// insert timestamp 16 from local, which will overwrite the remote record,
        /// since its timestamp is larger
        timestamp = 16;
        ctx = db_write_context::create(decree, timestamp);
        ASSERT_EQ(0, write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
        ASSERT_EQ(0, write_impl->db_write(ctx.decree));
        write_impl->clear_up_batch_states(decree, 0);
        ASSERT_EQ(read_timestamp_from(raw_key), timestamp);

        // write retry
        ASSERT_EQ(0, write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
        ASSERT_EQ(0, write_impl->db_write(ctx.decree));
        write_impl->clear_up_batch_states(decree, 0);

        const_cast<bool &>(write_impl->_verify_timetag) = false;
    }

    // ensure empty_writes must skip verify_timetag, since its
    // raw_key can't be normally restored.
    void test_empty_write_verify_timetag()
    {
        set_app_duplicating();
        auto write_impl = _write_svc->_impl.get();
        const_cast<bool &>(write_impl->_verify_timetag) = true;

        int err = write_impl->empty_put(1);
        ASSERT_EQ(err, 0);

        err = write_impl->empty_put(1);
        ASSERT_EQ(err, 0);
    }

    void test_verify_timetag_compatible_with_old_schema()
    {
        set_app_duplicating();
        auto write_impl = _write_svc->_impl.get();
        const_cast<bool &>(write_impl->_verify_timetag) = true;
        const_cast<uint32_t &>(write_impl->_value_schema_version) = 0;

        std::string raw_key = "key";
        std::string value = "value";
        int64_t decree = 10;
        uint64_t timestamp = 10;
        auto ctx = db_write_context::create(decree, timestamp);
        ASSERT_EQ(0, write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
        ASSERT_EQ(0, write_impl->db_write(ctx.decree));
        write_impl->clear_up_batch_states(decree, 0);

        ctx = db_write_context::create(decree, timestamp);
        ASSERT_EQ(0, write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
        ASSERT_EQ(0, write_impl->db_write(ctx.decree));
        write_impl->clear_up_batch_states(decree, 0);
    }

    void test_verify_timetag_on_duplicating_table_only()
    {
        dsn::fail::setup();
        auto write_impl = _write_svc->_impl.get();
        const_cast<bool &>(write_impl->_verify_timetag) = true;

        { // if db_write_batch_put_ctx causes a db_get, it will definitely fail here.
            dsn::fail::cfg("db_get", "100%1*return()");

            std::string raw_key = "key";
            std::string value = "value";
            int64_t decree = 10;
            uint64_t timestamp = 10;
            auto ctx = db_write_context::create(decree, timestamp);
            ASSERT_EQ(0, write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
            ASSERT_EQ(0, write_impl->db_write(ctx.decree));
            write_impl->clear_up_batch_states(decree, 0);
        }

        dsn::fail::teardown();
    }

    uint64_t read_timestamp_from(dsn::string_view raw_key)
    {
        auto write_impl = _write_svc->_impl.get();

        std::string raw_value;
        rocksdb::Status s = write_impl->_db->Get(
            write_impl->_rd_opts, utils::to_rocksdb_slice(raw_key), &raw_value);

        uint64_t local_timetag =
            pegasus_extract_timetag(write_impl->_value_schema_version, raw_value);
        return extract_timestamp_from_timetag(local_timetag);
    }
};

TEST_F(pegasus_write_service_test, multi_put) { test_multi_put(); }

TEST_F(pegasus_write_service_test, multi_remove) { test_multi_remove(); }

TEST_F(pegasus_write_service_test, batched_writes) { test_batched_writes(); }

TEST_F(pegasus_write_service_test, put_verify_timetag) { test_put_verify_timetag(); }

TEST_F(pegasus_write_service_test, empty_write_verify_timetag)
{
    test_empty_write_verify_timetag();
}

TEST_F(pegasus_write_service_test, verify_timetag_compatible_with_old_schema)
{
    test_verify_timetag_compatible_with_old_schema();
}

TEST_F(pegasus_write_service_test, verify_timetag_on_duplicating_table_only)
{
    test_verify_timetag_on_duplicating_table_only();
}

} // namespace server
} // namespace pegasus
