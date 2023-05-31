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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TBinaryProtocol.tcc>
#include <thrift/protocol/TProtocol.h>
#include <string.h>
#include <string>
#include <vector>

#include "runtime/rpc/dns_resolver.h"
#include "runtime/rpc/group_address.h"
#include "runtime/rpc/group_host_port.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_host_port.h"
#include "runtime/task/task_code.h"
#include "utils/binary_writer.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/threadpool_code.h"


namespace dsn {

DEFINE_TASK_CODE_RPC(RPC_TEST_THRIFT_HOST_PORT_PARSER, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

TEST(host_port_test, host_port_to_string)
{
    {
        host_port hp = host_port("localhost", 8080);
        ASSERT_EQ(std::string("localhost:8080"), hp.to_string());
    }

    {
        host_port hp;
        ASSERT_EQ(std::string("invalid address"), hp.to_string());
    }
}

TEST(host_port_test, host_port_build)
{
    host_port hp = host_port("localhost", 8080);
    ASSERT_EQ(HOST_TYPE_IPV4, hp.type());
    ASSERT_EQ(8080, hp.port());
    ASSERT_EQ("localhost", hp.host());

    {
        rpc_address addr = rpc_address("localhost", 8080);
        host_port hp1(addr);
        ASSERT_EQ(hp, hp1);
    }
}

TEST(host_port_test, operators)
{
    host_port hp("localhost", 8080);
    ASSERT_EQ(hp, hp);

    {
        host_port new_hp(hp);
        ASSERT_EQ(hp, new_hp);
    }

    {
        host_port new_hp("localhost", 8081);
        ASSERT_NE(hp, new_hp);
    }

    host_port hp2;
    ASSERT_NE(hp, hp2);
    ASSERT_FALSE(hp.is_invalid());
    ASSERT_TRUE(hp2.is_invalid());
}

TEST(host_port_test, rpc_group_host_port)
{
    host_port hp("localhost", 8080);
    host_port hp2("localhost", 8081);
    host_port invalid_hp;

    host_port hp_grp;
    hp_grp.assign_group("test_group");
    ASSERT_EQ(HOST_TYPE_GROUP, hp_grp.type());
    rpc_group_host_port *g = hp_grp.group_host_port();
    ASSERT_EQ(std::string("test_group"), g->name());

    // invalid_hp
    ASSERT_FALSE(g->remove(hp));
    ASSERT_FALSE(g->contains(hp));
    ASSERT_EQ(0u, g->members().size());
    ASSERT_EQ(invalid_hp, g->random_member());
    ASSERT_EQ(invalid_hp, g->next(hp));
    ASSERT_EQ(invalid_hp, g->leader());
    ASSERT_EQ(invalid_hp, g->possible_leader());

    // hp
    g->set_leader(hp);
    ASSERT_TRUE(g->contains(hp));
    ASSERT_EQ(1u, g->members().size());
    ASSERT_EQ(hp, g->members().at(0));
    ASSERT_EQ(hp, g->leader());
    ASSERT_EQ(hp, g->possible_leader());

    // hp2
    g->set_leader(hp2);
    ASSERT_TRUE(g->contains(hp));
    ASSERT_TRUE(g->contains(hp2));
    ASSERT_EQ(2u, g->members().size());
    ASSERT_EQ(hp, g->members().at(0));
    ASSERT_EQ(hp2, g->members().at(1));
    ASSERT_EQ(hp2, g->leader());
    ASSERT_EQ(hp2, g->possible_leader());
    ASSERT_EQ(hp, g->next(hp2));
    ASSERT_EQ(hp2, g->next(hp));

    // change leader
    g->set_leader(hp);
    ASSERT_TRUE(g->contains(hp));
    ASSERT_TRUE(g->contains(hp2));
    ASSERT_EQ(2u, g->members().size());
    ASSERT_EQ(hp, g->members().at(0));
    ASSERT_EQ(hp2, g->members().at(1));
    ASSERT_EQ(hp, g->leader());
    g->leader_forward();
    ASSERT_EQ(hp2, g->leader());

    // del
    ASSERT_TRUE(g->remove(hp));
    ASSERT_FALSE(g->contains(hp));
    ASSERT_TRUE(g->contains(hp2));
    ASSERT_EQ(1u, g->members().size());
    ASSERT_EQ(hp2, g->members().at(0));
    ASSERT_EQ(invalid_hp, g->leader());

    ASSERT_TRUE(g->remove(hp2));
    ASSERT_FALSE(g->contains(hp2));
    ASSERT_EQ(0u, g->members().size());
    ASSERT_EQ(invalid_hp, g->leader());
}

TEST(host_port_test, transfer_rpc_address)
{
    {
        std::vector<rpc_address> addresses;
        host_port hp("localhost", 8080);
        ASSERT_EQ(hp.resolve_addresses(addresses), error_s::ok());
        ASSERT_TRUE(rpc_address("127.0.0.1", 8080) == addresses[0] ||
                    rpc_address("127.0.1.1", 8080) == addresses[0]);
    }
    {
        std::vector<rpc_address> addresses;
        host_port hp;
        hp.resolve_addresses(addresses);
        ASSERT_EQ(
            hp.resolve_addresses(addresses),
            error_s::make(dsn::ERR_INVALID_STATE, "invalid host_port type: HOST_TYPE_INVALID"));

        hp.assign_group("test_group");
        ASSERT_EQ(hp.resolve_addresses(addresses),
                  error_s::make(dsn::ERR_INVALID_STATE, "invalid host_port type: HOST_TYPE_GROUP"));
    }
}

TEST(host_port_test, dns_resolver)
{
    dns_resolver resolver;
    {
        host_port hp("localhost", 8080);
        auto addr = resolver.resolve_address(hp);
        ASSERT_TRUE(rpc_address("127.0.0.1", 8080) == addr ||
                    rpc_address("127.0.1.1", 8080) == addr);
    }

    {
        host_port hp_grp;
        hp_grp.assign_group("test_group");
        rpc_group_host_port *g = hp_grp.group_host_port();

        host_port hp1("localhost", 8080);
        ASSERT_TRUE(g->add(hp1));
        host_port hp2("localhost", 8081);
        g->set_leader(hp2);

        auto addr_grp = resolver.resolve_address(hp_grp);

        ASSERT_EQ(addr_grp.group_address()->is_update_leader_automatically(),
                  hp_grp.group_host_port()->is_update_leader_automatically());
        ASSERT_EQ(strcmp(addr_grp.group_address()->name(), hp_grp.group_host_port()->name()), 0);
        ASSERT_EQ(addr_grp.group_address()->count(), hp_grp.group_host_port()->count());
        ASSERT_EQ(host_port(addr_grp.group_address()->leader()),
                  hp_grp.group_host_port()->leader());
    }
}

shared_ptr<binary_writer_transport> get_binary_writer_transport()
{
    /// write rpc message
    size_t body_length = 0;
    message_ptr msg = message_ex::create_request(RPC_TEST_THRIFT_HOST_PORT_PARSER, 1000, 64, 5000000000);
    rpc_write_stream stream(msg);
    binary_writer_transport binary_transport(stream);
    shared_ptr<binary_writer_transport> trans_ptr(&binary_transport, [](binary_writer_transport *) {});
    return trans_ptr;
}

TEST(host_port_test, thrift_parser)
{
    ::apache::thrift::protocol::TBinaryProtocol oprot_binary(get_binary_writer_transport());

    host_port hp1 = host_port("localhost", 8080);
    hp.write(&oprot_binary);
    host_port hp2;
    hp2.read(&oprot_binary);
    ASSERT_EQ(hp1, hp2);

    ::apache::thrift::protocol::TJSONProtocol oprot_json(trans_ptr);
    hp.write(&oprot_binary);
    host_port hp3;
    hp3.read(&oprot_binary);
    ASSERT_EQ(hp1, hp2);
}

} // namespace dsn
