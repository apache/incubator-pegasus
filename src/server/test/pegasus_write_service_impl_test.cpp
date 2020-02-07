// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_server_test_base.h"
#include "server/pegasus_server_write.h"
#include "server/pegasus_write_service_impl.h"
#include "message_utils.h"

#include <dsn/utility/defer.h>

namespace pegasus {
namespace server {

class pegasus_write_service_impl_test : public pegasus_server_test_base
{
protected:
    std::unique_ptr<pegasus_server_write> _server_write;
    pegasus_write_service::impl *_write_impl{nullptr};

public:
    void SetUp() override
    {
        start();
        _server_write = dsn::make_unique<pegasus_server_write>(_server.get(), true);
        _write_impl = _server_write->_write_svc->_impl.get();
    }

    uint64_t read_timestamp_from(dsn::string_view raw_key)
    {
        std::string raw_value;
        rocksdb::Status s = _write_impl->_db->Get(
            _write_impl->_rd_opts, utils::to_rocksdb_slice(raw_key), &raw_value);

        uint64_t local_timetag =
            pegasus_extract_timetag(_write_impl->_pegasus_data_version, raw_value);
        return extract_timestamp_from_timetag(local_timetag);
    }

    // start with duplicating.
    void set_app_duplicating()
    {
        _server->stop(false);
        dsn::replication::destroy_replica(_replica);

        dsn::app_info app_info;
        app_info.app_type = "pegasus";
        app_info.duplicating = true;
        _replica =
            dsn::replication::create_test_replica(_replica_stub, _gpid, app_info, "./", false);
        _server = dsn::make_unique<pegasus_server_impl>(_replica);

        SetUp();
    }
};

TEST_F(pegasus_write_service_impl_test, put_verify_timetag)
{
    set_app_duplicating();

    dsn::blob raw_key;
    pegasus::pegasus_generate_key(
        raw_key, dsn::string_view("hash_key"), dsn::string_view("sort_key"));
    std::string value = "value";
    int64_t decree = 10;

    /// insert timestamp 10
    uint64_t timestamp = 10;
    auto ctx = db_write_context::create(decree, timestamp);
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);
    ASSERT_EQ(read_timestamp_from(raw_key), timestamp);

    /// insert timestamp 15, which overwrites the previous record
    timestamp = 15;
    ctx = db_write_context::create(decree, timestamp);
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);
    ASSERT_EQ(read_timestamp_from(raw_key), timestamp);

    /// insert timestamp 15 from remote, which will overwrite the previous record,
    /// since its cluster id is larger (current cluster_id=1)
    timestamp = 15;
    ctx.remote_timetag = pegasus::generate_timetag(timestamp, 2, false);
    ctx.verify_timetag = true;
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value + "_new", 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);
    ASSERT_EQ(read_timestamp_from(raw_key), timestamp);
    std::string raw_value;
    dsn::blob user_value;
    rocksdb::Status s =
        _write_impl->_db->Get(_write_impl->_rd_opts, utils::to_rocksdb_slice(raw_key), &raw_value);
    pegasus_extract_user_data(_write_impl->_pegasus_data_version, std::move(raw_value), user_value);
    ASSERT_EQ(user_value.to_string(), "value_new");

    // write retry
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value + "_new", 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);

    /// insert timestamp 16 from local, which will overwrite the remote record,
    /// since its timestamp is larger
    timestamp = 16;
    ctx = db_write_context::create(decree, timestamp);
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);
    ASSERT_EQ(read_timestamp_from(raw_key), timestamp);

    // write retry
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);
}

// verify timetag on data version v0
TEST_F(pegasus_write_service_impl_test, verify_timetag_compatible_with_version_0)
{
    dsn::fail::setup();
    dsn::fail::cfg("db_get", "100%1*return()");
    // if db_write_batch_put_ctx invokes db_get, this test must fail.

    const_cast<uint32_t &>(_write_impl->_pegasus_data_version) = 0; // old version
    auto tear_down = dsn::defer([this]() {
        const_cast<uint32_t &>(_write_impl->_pegasus_data_version) = 0; // reset version
    });

    dsn::blob raw_key;
    pegasus::pegasus_generate_key(
        raw_key, dsn::string_view("hash_key"), dsn::string_view("sort_key"));
    std::string value = "value";
    int64_t decree = 10;
    uint64_t timestamp = 10;

    auto ctx = db_write_context::create_duplicate(decree, timestamp, true);
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);

    dsn::fail::teardown();
}

} // namespace server
} // namespace pegasus
