// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "replica_test_base.h"

#include <fstream>

#include <dsn/utility/fail_point.h>
#include <gtest/gtest.h>

namespace dsn {
namespace replication {

class replica_bulk_load_test : public replica_test_base
{
public:
    replica_bulk_load_test()
    {
        _replica = create_mock_replica(stub.get());
        fail::setup();
    }

    ~replica_bulk_load_test() { fail::teardown(); }

    /// bulk load functions

    error_code test_on_bulk_load()
    {
        bulk_load_response resp;
        _replica->on_bulk_load(_req, resp);
        return resp.err;
    }

    error_code test_on_group_bulk_load(bulk_load_status::type status, ballot b)
    {
        create_group_bulk_load_request(status, b);
        group_bulk_load_response resp;
        _replica->on_group_bulk_load(_group_req, resp);
        return resp.err;
    }

    error_code test_start_downloading()
    {
        return _replica->bulk_load_start_download(APP_NAME, CLUSTER, PROVIDER);
    }

    /// mock structure functions

    void
    create_bulk_load_request(bulk_load_status::type status, ballot b, int32_t downloading_count = 0)
    {
        _req.app_name = APP_NAME;
        _req.ballot = b;
        _req.cluster_name = CLUSTER;
        _req.meta_bulk_load_status = status;
        _req.pid = PID;
        _req.remote_provider_name = PROVIDER;
        stub->set_bulk_load_downloading_count(downloading_count);
    }

    void create_bulk_load_request(bulk_load_status::type status, int32_t downloading_count = 0)
    {
        if (status != bulk_load_status::BLS_DOWNLOADING) {
            downloading_count = 0;
        }
        create_bulk_load_request(status, BALLOT, downloading_count);
    }

    void create_group_bulk_load_request(bulk_load_status::type status, ballot b)
    {
        _group_req.app_name = APP_NAME;
        _group_req.meta_bulk_load_status = status;
        _group_req.config.status = partition_status::PS_SECONDARY;
        _group_req.config.ballot = b;
        _group_req.target_address = SECONDARY;
    }

    void mock_replica_config(partition_status::type status)
    {
        replica_configuration rconfig;
        rconfig.ballot = BALLOT;
        rconfig.pid = PID;
        rconfig.primary = PRIMARY;
        rconfig.status = status;
        _replica->_config = rconfig;
    }

    void mock_primary_states()
    {
        mock_replica_config(partition_status::PS_PRIMARY);
        partition_configuration config;
        config.max_replica_count = 3;
        config.pid = PID;
        config.ballot = BALLOT;
        config.primary = PRIMARY;
        config.secondaries.emplace_back(SECONDARY);
        config.secondaries.emplace_back(SECONDARY2);
        _replica->_primary_states.membership = config;
    }

    // helper functions
    bulk_load_status::type get_bulk_load_status() const
    {
        return _replica->_bulk_load_context._status;
    }

public:
    std::unique_ptr<mock_replica> _replica;
    bulk_load_request _req;
    group_bulk_load_request _group_req;

    std::string APP_NAME = "replica";
    std::string CLUSTER = "cluster";
    std::string PROVIDER = "local_service";
    gpid PID = gpid(1, 0);
    ballot BALLOT = 3;
    rpc_address PRIMARY = rpc_address("127.0.0.2", 34801);
    rpc_address SECONDARY = rpc_address("127.0.0.3", 34801);
    rpc_address SECONDARY2 = rpc_address("127.0.0.4", 34801);
    int32_t MAX_DOWNLOADING_COUNT = 5;
};

// on_bulk_load unit tests
TEST_F(replica_bulk_load_test, on_bulk_load_not_primary)
{
    create_bulk_load_request(bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(test_on_bulk_load(), ERR_INVALID_STATE);
}

TEST_F(replica_bulk_load_test, on_bulk_load_ballot_change)
{
    create_bulk_load_request(bulk_load_status::BLS_DOWNLOADING, BALLOT + 1);
    mock_primary_states();
    ASSERT_EQ(test_on_bulk_load(), ERR_INVALID_STATE);
}

// on_group_bulk_load unit tests
TEST_F(replica_bulk_load_test, on_group_bulk_load_test)
{
    struct test_struct
    {
        partition_status::type pstatus;
        bulk_load_status::type bstatus;
        ballot b;
        error_code expected_err;
    } tests[] = {
        {partition_status::PS_SECONDARY,
         bulk_load_status::BLS_DOWNLOADING,
         BALLOT - 1,
         ERR_VERSION_OUTDATED},
        {partition_status::PS_SECONDARY,
         bulk_load_status::BLS_DOWNLOADED,
         BALLOT + 1,
         ERR_INVALID_STATE},
        {partition_status::PS_INACTIVE, bulk_load_status::BLS_INGESTING, BALLOT, ERR_INVALID_STATE},
    };

    for (auto test : tests) {
        mock_replica_config(test.pstatus);
        ASSERT_EQ(test_on_group_bulk_load(test.bstatus, test.b), test.expected_err);
    }
}

// start_downloading unit tests
TEST_F(replica_bulk_load_test, start_downloading_test)
{
    // Test cases:
    // - stub concurrent downloading count excceed
    // - TODO(heyuchen): add 'downloading error' after implemtation do_download
    // - /*{false, 1, ERR_CORRUPTION, bulk_load_status::BLS_DOWNLOADING, 1},*/
    // - downloading succeed
    struct test_struct
    {
        bool mock_function;
        int32_t downloading_count;
        error_code expected_err;
        bulk_load_status::type expected_status;
        int32_t expected_downloading_count;
    } tests[]{{false,
               MAX_DOWNLOADING_COUNT,
               ERR_BUSY,
               bulk_load_status::BLS_INVALID,
               MAX_DOWNLOADING_COUNT},
              {true, 1, ERR_OK, bulk_load_status::BLS_DOWNLOADING, 2}};

    for (auto test : tests) {
        if (test.mock_function) {
            fail::cfg("replica_bulk_load_download_sst_files", "return()");
        }
        create_bulk_load_request(bulk_load_status::BLS_DOWNLOADING, test.downloading_count);

        ASSERT_EQ(test_start_downloading(), test.expected_err);
        ASSERT_EQ(get_bulk_load_status(), test.expected_status);
        ASSERT_EQ(stub->get_bulk_load_downloading_count(), test.expected_downloading_count);
    }
}

} // namespace replication
} // namespace dsn
