// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "server/pegasus_mutation_duplicator.h"
#include "base/pegasus_rpc_types.h"

#include <gtest/gtest.h>
#include <dsn/cpp/message_utils.h>

using namespace pegasus::server;

TEST(pegasus_mutation_duplicator, get_hash_from_request)
{
    std::string hash_key("hash");
    std::string sort_key("sort");
    uint64_t hash =
        pegasus::pegasus_hash_key_hash(dsn::blob(hash_key.data(), 0, hash_key.length()));

    {
        dsn::apps::multi_put_request request;
        request.hash_key.assign(hash_key.data(), 0, hash_key.length());
        auto msg = dsn::from_thrift_request_to_received_message(request,
                                                                dsn::apps::RPC_RRDB_RRDB_MULTI_PUT);
        auto data = dsn::move_message_to_blob(msg);
        ASSERT_EQ(hash, get_hash_from_request(dsn::apps::RPC_RRDB_RRDB_MULTI_PUT, data));
    }

    {
        dsn::apps::multi_remove_request request;
        request.hash_key.assign(hash_key.data(), 0, hash_key.length());
        auto msg = dsn::from_thrift_request_to_received_message(
            request, dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE);

        auto data = dsn::move_message_to_blob(msg);
        ASSERT_EQ(hash, get_hash_from_request(dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE, data));
    }

    {
        dsn::apps::update_request request;
        pegasus::pegasus_generate_key(request.key, hash_key, sort_key);
        auto msg =
            dsn::from_thrift_request_to_received_message(request, dsn::apps::RPC_RRDB_RRDB_PUT);
        auto data = dsn::move_message_to_blob(msg);
        ASSERT_EQ(hash, get_hash_from_request(dsn::apps::RPC_RRDB_RRDB_PUT, data));
    }

    {
        dsn::blob key;
        pegasus::pegasus_generate_key(key, hash_key, sort_key);
        auto msg =
            dsn::from_thrift_request_to_received_message(key, dsn::apps::RPC_RRDB_RRDB_REMOVE);
        auto data = dsn::move_message_to_blob(msg);
        ASSERT_EQ(hash, get_hash_from_request(dsn::apps::RPC_RRDB_RRDB_REMOVE, data));
    }
}

// Verifies that calls on `get_hash_key_from_request` won't make
// message unable to read. (if `get_hash_key_from_request` doesn't
// use copy the message internally, it will.)
TEST(pegasus_mutation_duplicator, read_after_get_hash_key)
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
