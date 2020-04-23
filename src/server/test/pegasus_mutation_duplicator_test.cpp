// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "server/pegasus_mutation_duplicator.h"
#include "base/pegasus_rpc_types.h"
#include "pegasus_server_test_base.h"

#include <gtest/gtest.h>
#include <dsn/cpp/message_utils.h>
#include <dsn/dist/replication/replica_base.h>
#include <condition_variable>

namespace pegasus {
namespace server {

using namespace dsn::replication;

class pegasus_mutation_duplicator_test : public pegasus_server_test_base
{
protected:
    dsn::task_tracker _tracker;
    dsn::pipeline::environment _env;

public:
    pegasus_mutation_duplicator_test()
    {
        _env.thread_pool(LPC_REPLICATION_LOW).task_tracker(&_tracker);
    }

    void test_duplicate()
    {
        replica_base replica(dsn::gpid(1, 1), "fake_replica");
        auto duplicator = new_mutation_duplicator(&replica, "onebox2", "temp");
        duplicator->set_task_environment(&_env);

        mutation_tuple_set muts;
        for (uint64_t i = 0; i < 100; i++) {
            uint64_t ts = 200 + i;
            dsn::task_code code = dsn::apps::RPC_RRDB_RRDB_PUT;

            dsn::apps::update_request request;
            pegasus::pegasus_generate_key(request.key, std::string("hash"), std::string("sort"));
            dsn::message_ptr msg =
                dsn::from_thrift_request_to_received_message(request, dsn::apps::RPC_RRDB_RRDB_PUT);
            auto data = dsn::move_message_to_blob(msg.get());

            muts.insert(std::make_tuple(ts, code, data));
        }

        size_t total_shipped_size = 0;
        auto duplicator_impl = dynamic_cast<pegasus_mutation_duplicator *>(duplicator.get());
        RPC_MOCKING(duplicate_rpc)
        {
            duplicator->duplicate(muts, [](size_t) {});

            size_t total_size = 100;
            while (total_size > 0) {
                // ensure mutations having the same hash are sending sequentially.
                ASSERT_EQ(duplicator_impl->_inflights.size(), 1);
                ASSERT_EQ(duplicate_rpc::mail_box().size(), 1);

                total_size--;
                ASSERT_EQ(duplicator_impl->_inflights.begin()->second.size(), total_size);

                auto rpc = duplicate_rpc::mail_box().back();
                duplicate_rpc::mail_box().pop_back();

                total_shipped_size +=
                    rpc.dsn_request()->body_size() + rpc.dsn_request()->header->hdr_length;
                duplicator_impl->on_duplicate_reply(get_hash(rpc),
                                                    [total_shipped_size](size_t final_size) {
                                                        ASSERT_EQ(total_shipped_size, final_size);
                                                    },
                                                    rpc,
                                                    dsn::ERR_OK);

                // schedule next round
                _tracker.wait_outstanding_tasks();
            }

            ASSERT_EQ(duplicator_impl->_total_shipped_size, total_shipped_size);
            ASSERT_EQ(duplicator_impl->_inflights.size(), 0);
            ASSERT_EQ(duplicate_rpc::mail_box().size(), 0);
        }
    }

    void test_duplicate_failed()
    {
        replica_base replica(dsn::gpid(1, 1), "fake_replica");
        auto duplicator = new_mutation_duplicator(&replica, "onebox2", "temp");
        duplicator->set_task_environment(&_env);

        mutation_tuple_set muts;
        for (uint64_t i = 0; i < 10; i++) {
            uint64_t ts = 200 + i;
            dsn::task_code code = dsn::apps::RPC_RRDB_RRDB_PUT;

            dsn::apps::update_request request;
            pegasus::pegasus_generate_key(request.key, std::string("hash"), std::string("sort"));
            dsn::message_ptr msg =
                dsn::from_thrift_request_to_received_message(request, dsn::apps::RPC_RRDB_RRDB_PUT);
            auto data = dsn::move_message_to_blob(msg.get());

            muts.insert(std::make_tuple(ts, code, data));
        }

        auto duplicator_impl = dynamic_cast<pegasus_mutation_duplicator *>(duplicator.get());
        RPC_MOCKING(duplicate_rpc)
        {
            duplicator->duplicate(muts, [](size_t) {});

            auto rpc = duplicate_rpc::mail_box().back();
            duplicate_rpc::mail_box().pop_back();
            ASSERT_EQ(duplicator_impl->_inflights.begin()->second.size(), 9);

            // failed
            duplicator_impl->on_duplicate_reply(
                get_hash(rpc), [](size_t) {}, rpc, dsn::ERR_TIMEOUT);

            // schedule next round
            _tracker.wait_outstanding_tasks();

            // retry infinitely
            ASSERT_EQ(duplicator_impl->_inflights.size(), 1);
            ASSERT_EQ(duplicate_rpc::mail_box().size(), 1);
            ASSERT_EQ(duplicator_impl->_inflights.begin()->second.size(), 9);
            duplicate_rpc::mail_box().clear();

            // with other error
            rpc.response().error = PERR_INVALID_ARGUMENT;
            duplicator_impl->on_duplicate_reply(get_hash(rpc), [](size_t) {}, rpc, dsn::ERR_OK);
            _tracker.wait_outstanding_tasks();
            ASSERT_EQ(duplicator_impl->_inflights.size(), 1);
            ASSERT_EQ(duplicate_rpc::mail_box().size(), 1);
            ASSERT_EQ(duplicator_impl->_inflights.begin()->second.size(), 9);
            duplicate_rpc::mail_box().clear();

            // with other error
            rpc.response().error = PERR_OK;
            duplicator_impl->on_duplicate_reply(
                get_hash(rpc), [](size_t) {}, rpc, dsn::ERR_IO_PENDING);
            _tracker.wait_outstanding_tasks();
            ASSERT_EQ(duplicator_impl->_inflights.size(), 1);
            ASSERT_EQ(duplicate_rpc::mail_box().size(), 1);
            ASSERT_EQ(duplicator_impl->_inflights.begin()->second.size(), 9);
            duplicate_rpc::mail_box().clear();
        }
    }

    void test_duplicate_isolated_hashkeys()
    {
        replica_base replica(dsn::gpid(1, 1), "fake_replica");
        auto duplicator = new_mutation_duplicator(&replica, "onebox2", "temp");
        duplicator->set_task_environment(&_env);

        size_t total_size = 3000;
        mutation_tuple_set muts;
        for (uint64_t i = 0; i < total_size; i++) {
            uint64_t ts = 200 + i;
            dsn::task_code code = dsn::apps::RPC_RRDB_RRDB_PUT;

            dsn::apps::update_request request;
            pegasus::pegasus_generate_key(
                request.key, std::string("hash") + std::to_string(i), std::string("sort"));
            dsn::message_ptr msg = dsn::from_thrift_request_to_received_message(request, code);
            auto data = dsn::move_message_to_blob(msg.get());

            muts.insert(std::make_tuple(ts, code, data));
        }

        auto duplicator_impl = dynamic_cast<pegasus_mutation_duplicator *>(duplicator.get());
        RPC_MOCKING(duplicate_rpc)
        {
            duplicator->duplicate(muts, [](size_t) {});

            // ensure each bucket has only 1 request and each request is
            // isolated with others.
            ASSERT_EQ(duplicator_impl->_inflights.size(), total_size);
            ASSERT_EQ(duplicate_rpc::mail_box().size(), total_size);
            for (const auto &ents : duplicator_impl->_inflights) {
                ASSERT_EQ(ents.second.size(), 0);
            }

            // reply with success
            auto rpc_list = std::move(duplicate_rpc::mail_box());
            for (const auto &rpc : rpc_list) {
                rpc.response().error = dsn::ERR_OK;
                duplicator_impl->on_duplicate_reply(get_hash(rpc), [](size_t) {}, rpc, dsn::ERR_OK);
            }
            _tracker.wait_outstanding_tasks();
            ASSERT_EQ(duplicate_rpc::mail_box().size(), 0);
            ASSERT_EQ(duplicator_impl->_inflights.size(), 0);
        }
    }

    void test_create_duplicator()
    {
        replica_base replica(dsn::gpid(1, 1), "fake_replica");
        auto duplicator = new_mutation_duplicator(&replica, "onebox2", "temp");
        duplicator->set_task_environment(&_env);
        auto duplicator_impl = dynamic_cast<pegasus_mutation_duplicator *>(duplicator.get());
        ASSERT_EQ(duplicator_impl->_remote_cluster_id, 2);
        ASSERT_EQ(duplicator_impl->_remote_cluster, "onebox2");
        ASSERT_EQ(get_current_cluster_id(), 1);
    }

private:
    static uint64_t get_hash(const duplicate_rpc &rpc)
    {
        return get_hash_from_request(rpc.request().task_code, rpc.request().raw_message);
    }
};

TEST_F(pegasus_mutation_duplicator_test, get_hash_from_request)
{
    std::string hash_key("hash");
    std::string sort_key("sort");
    uint64_t hash =
        pegasus::pegasus_hash_key_hash(dsn::blob(hash_key.data(), 0, hash_key.length()));

    {
        dsn::apps::multi_put_request request;
        request.hash_key.assign(hash_key.data(), 0, hash_key.length());
        dsn::message_ptr msg = dsn::from_thrift_request_to_received_message(
            request, dsn::apps::RPC_RRDB_RRDB_MULTI_PUT);
        auto data = dsn::move_message_to_blob(msg.get());
        ASSERT_EQ(hash, get_hash_from_request(dsn::apps::RPC_RRDB_RRDB_MULTI_PUT, data));
    }

    {
        dsn::apps::multi_remove_request request;
        request.hash_key.assign(hash_key.data(), 0, hash_key.length());
        dsn::message_ptr msg = dsn::from_thrift_request_to_received_message(
            request, dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE);

        auto data = dsn::move_message_to_blob(msg.get());
        ASSERT_EQ(hash, get_hash_from_request(dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE, data));
    }

    {
        dsn::apps::update_request request;
        pegasus::pegasus_generate_key(request.key, hash_key, sort_key);
        dsn::message_ptr msg =
            dsn::from_thrift_request_to_received_message(request, dsn::apps::RPC_RRDB_RRDB_PUT);
        auto data = dsn::move_message_to_blob(msg.get());
        ASSERT_EQ(hash, get_hash_from_request(dsn::apps::RPC_RRDB_RRDB_PUT, data));
    }

    {
        dsn::blob key;
        pegasus::pegasus_generate_key(key, hash_key, sort_key);
        dsn::message_ptr msg =
            dsn::from_thrift_request_to_received_message(key, dsn::apps::RPC_RRDB_RRDB_REMOVE);
        auto data = dsn::move_message_to_blob(msg.get());
        ASSERT_EQ(hash, get_hash_from_request(dsn::apps::RPC_RRDB_RRDB_REMOVE, data));
    }
}

// Verifies that calls on `get_hash_key_from_request` won't make
// message unable to read. (if `get_hash_key_from_request` doesn't
// copy the message internally, it will.)
TEST_F(pegasus_mutation_duplicator_test, read_after_get_hash_key)
{
    std::string hash_key("hash");
    std::string sort_key("sort");
    uint64_t hash =
        pegasus::pegasus_hash_key_hash(dsn::blob(hash_key.data(), 0, hash_key.length()));

    dsn::message_ex *msg;
    {
        dsn::apps::update_request request;
        pegasus::pegasus_generate_key(request.key, hash_key, sort_key);
        msg = dsn::from_thrift_request_to_received_message(request, dsn::apps::RPC_RRDB_RRDB_PUT);
    }
    auto data = dsn::move_message_to_blob(msg);
    ASSERT_EQ(hash, get_hash_from_request(dsn::apps::RPC_RRDB_RRDB_PUT, data));

    pegasus::put_rpc rpc(msg);
    dsn::blob raw_key;
    pegasus::pegasus_generate_key(raw_key, hash_key, sort_key);
    ASSERT_EQ(rpc.request().key.to_string(), raw_key.to_string());
}

TEST_F(pegasus_mutation_duplicator_test, duplicate) { test_duplicate(); }

TEST_F(pegasus_mutation_duplicator_test, duplicate_failed) { test_duplicate_failed(); }

TEST_F(pegasus_mutation_duplicator_test, duplicate_isolated_hashkeys)
{
    test_duplicate_isolated_hashkeys();
}

TEST_F(pegasus_mutation_duplicator_test, create_duplicator) { test_create_duplicator(); }

TEST_F(pegasus_mutation_duplicator_test, duplicate_duplicate)
{
    replica_base replica(dsn::gpid(1, 1), "fake_replica");
    auto duplicator = new_mutation_duplicator(&replica, "onebox2", "temp");
    duplicator->set_task_environment(&_env);

    dsn::apps::update_request request;
    pegasus::pegasus_generate_key(request.key, std::string("hash"), std::string("sort"));
    dsn::message_ptr msg =
        dsn::from_thrift_request_to_received_message(request, dsn::apps::RPC_RRDB_RRDB_PUT);
    auto data = dsn::move_message_to_blob(msg.get());

    // a duplicate from onebox2
    dsn::apps::duplicate_request dup;
    dup.cluster_id = 2;
    dup.raw_message = data;
    dup.timestamp = 200;
    msg = dsn::from_thrift_request_to_received_message(dup, dsn::apps::RPC_RRDB_RRDB_DUPLICATE);
    data = dsn::move_message_to_blob(msg.get());

    mutation_tuple_set muts;
    muts.insert(std::make_tuple(200, dsn::apps::RPC_RRDB_RRDB_DUPLICATE, data));
    RPC_MOCKING(duplicate_rpc)
    {
        duplicator->duplicate(muts, [&](size_t sz) {
            // ensure no DUPLICATE is duplicated
            ASSERT_EQ(sz, 0);
        });
    }
    _tracker.wait_outstanding_tasks();
}

} // namespace server
} // namespace pegasus
