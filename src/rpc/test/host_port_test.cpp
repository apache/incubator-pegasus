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

#include <arpa/inet.h> // IWYU pragma: keep
#include <fmt/core.h>
#include <netinet/in.h>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "bulk_load_types.h"
#include "common/serialization_helper/dsn.layer2_types.h"
#include "fd_types.h"
#include "gtest/gtest.h"
#include "meta_admin_types.h"
#include "rpc/dns_resolver.h"
#include "rpc/group_address.h"
#include "rpc/group_host_port.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"
#include "rpc/rpc_message.h"
#include "rpc/serialization.h"
#include "runtime/test_utils.h"
#include "task/async_calls.h"
#include "task/task.h"
#include "task/task_spec.h"
#include "task/task_tracker.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/errors.h"

namespace dsn {

TEST(host_port_test, host_port_to_string)
{
    {
        host_port hp("localhost", 8080);
        ASSERT_EQ("localhost:8080", hp.to_string());
    }

    {
        host_port hp;
        ASSERT_EQ("invalid host_port", hp.to_string());
    }
}

TEST(host_port_test, host_port_build)
{
    host_port hp("localhost", 8080);
    ASSERT_EQ(HOST_TYPE_IPV4, hp.type());
    ASSERT_EQ(8080, hp.port());
    ASSERT_EQ("localhost", hp.host());

    {
        const auto addr = rpc_address::from_host_port("localhost", 8080);
        ASSERT_EQ(hp, host_port::from_address(addr));
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
    ASSERT_TRUE(hp);
    ASSERT_FALSE(hp2);

    std::string hp_str = "localhost:8080";
    host_port hp3;
    ASSERT_FALSE(hp3);
    hp3 = host_port::from_string(hp_str);
    ASSERT_EQ(hp, hp3);
    ASSERT_TRUE(hp3);

    host_port hp4;
    ASSERT_FALSE(hp4);
    std::string hp_str2 = "pegasus:8080";
    hp4 = host_port::from_string(hp_str2);
    ASSERT_FALSE(hp4);

    host_port hp5("localhost", 8081);
    ASSERT_LT(hp, hp5);
}

TEST(host_port_test, rpc_group_host_port)
{
    host_port hp("localhost", 8080);
    host_port hp2("localhost", 8081);
    host_port invalid_hp;

    host_port hp_grp;
    hp_grp.assign_group("test_group");
    ASSERT_EQ(HOST_TYPE_GROUP, hp_grp.type());
    const auto &g = hp_grp.group_host_port();
    ASSERT_STREQ("test_group", g->name());

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

    // operator <
    host_port hp_grp1;
    hp_grp1.assign_group("test_group");
    if (hp_grp.group_host_port().get() < hp_grp1.group_host_port().get()) {
        ASSERT_LT(hp_grp, hp_grp1);
    } else {
        ASSERT_FALSE(hp_grp < hp_grp1);
    }

    // address_group -> host_port_group
    const auto addr = rpc_address::from_ip_port("127.0.0.1", 8080);
    const auto addr2 = rpc_address::from_ip_port("127.0.0.1", 8081);

    rpc_address addr_grp;
    addr_grp.assign_group("test_group");
    ASSERT_EQ(HOST_TYPE_GROUP, addr_grp.type());

    auto g_addr = addr_grp.group_address();
    ASSERT_STREQ("test_group", g_addr->name());

    ASSERT_TRUE(g_addr->add(addr));
    g_addr->set_leader(addr2);
    ASSERT_EQ(addr2, g_addr->leader());
    ASSERT_EQ(2, g_addr->count());

    host_port hp_grp2 = host_port::from_address(addr_grp);
    ASSERT_EQ(HOST_TYPE_GROUP, hp_grp2.type());

    auto g_hp = hp_grp2.group_host_port();
    ASSERT_STREQ("test_group", g_hp->name());
    ASSERT_EQ(hp2, g_hp->leader());
    ASSERT_EQ(2, g_hp->count());
}

TEST(host_port_test, transfer_rpc_address)
{
    {
        std::vector<rpc_address> addresses;
        host_port hp("localhost", 8080);
        ASSERT_EQ(hp.resolve_addresses(addresses), error_s::ok());
        ASSERT_TRUE(rpc_address::from_ip_port("127.0.0.1", 8080) == addresses[0] ||
                    rpc_address::from_ip_port("127.0.1.1", 8080) == addresses[0]);
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

void send_and_check_host_port_by_serialize(const host_port &hp, dsn_msg_serialize_format t)
{
    const auto &hp_str = hp.to_string();
    const auto server = ::dsn::rpc_address::from_host_port("localhost", 20101);

    dsn::message_ptr msg_ptr = dsn::message_ex::create_request(RPC_TEST_THRIFT_HOST_PORT_PARSER);
    msg_ptr->header->context.u.serialize_format = t;

    ::dsn::marshall(msg_ptr.get(), hp);

    dsn::task_tracker tracker;
    rpc::call(server, msg_ptr.get(), &tracker, [hp_str](error_code ec, std::string &&resp) {
        ASSERT_EQ(ERR_OK, ec);
        ASSERT_EQ(resp, hp_str);
    })->wait();
}

TEST(host_port_test, thrift_parser)
{
    host_port hp1("localhost", 8080);
    send_and_check_host_port_by_serialize(hp1, DSF_THRIFT_BINARY);
    send_and_check_host_port_by_serialize(hp1, DSF_THRIFT_JSON);

    host_port hp2("localhost", 1010);
    send_and_check_host_port_by_serialize(hp2, DSF_THRIFT_BINARY);
    send_and_check_host_port_by_serialize(hp2, DSF_THRIFT_JSON);
}

TEST(host_port_test, lookup_hostname)
{
    const std::string valid_ip = "127.0.0.1";
    const std::string expected_hostname = "localhost";

    const auto rpc_example_valid = rpc_address::from_ip_port(valid_ip, 23010);
    std::string hostname;
    auto es = host_port::lookup_hostname(htonl(rpc_example_valid.ip()), &hostname);
    ASSERT_TRUE(es.is_ok()) << es.description();
    ASSERT_EQ(expected_hostname, hostname);

    es = host_port::lookup_hostname(12321, &hostname);
    ASSERT_FALSE(es.is_ok());
}

TEST(host_port_test, test_macros)
{
    static const host_port kHp1("localhost", 8081);
    static const host_port kHp2("localhost", 8082);
    static const host_port kHp3("localhost", 8083);
    static const std::vector<host_port> kHps({kHp1, kHp2, kHp3});
    static const rpc_address kAddr1 = dns_resolver::instance().resolve_address(kHp1);
    static const rpc_address kAddr2 = dns_resolver::instance().resolve_address(kHp2);
    static const rpc_address kAddr3 = dns_resolver::instance().resolve_address(kHp3);
    static const std::vector<rpc_address> kAddres({kAddr1, kAddr2, kAddr3});

    // Test GET_HOST_PORT-1.
    {
        fd::beacon_msg beacon;
        host_port hp_from_node;
        GET_HOST_PORT(beacon, from_node, hp_from_node);
        ASSERT_FALSE(hp_from_node);
    }
    // Test GET_HOST_PORT-2.
    {
        fd::beacon_msg beacon;
        host_port hp_from_node;
        beacon.from_node = kAddr1;
        GET_HOST_PORT(beacon, from_node, hp_from_node);
        ASSERT_TRUE(hp_from_node);
        ASSERT_EQ(kHp1, hp_from_node);
        ASSERT_EQ(kAddr1, dns_resolver::instance().resolve_address(hp_from_node));
    }
    // Test GET_HOST_PORT-3.
    {
        fd::beacon_msg beacon;
        host_port hp_from_node;
        beacon.from_node = kAddr1;
        beacon.__set_hp_from_node(kHp1);
        GET_HOST_PORT(beacon, from_node, hp_from_node);
        ASSERT_TRUE(hp_from_node);
        ASSERT_EQ(kHp1, hp_from_node);
        ASSERT_EQ(kAddr1, dns_resolver::instance().resolve_address(hp_from_node));
    }

    // Test GET_HOST_PORTS-1.
    {
        replication::configuration_recovery_request req;
        std::vector<host_port> recovery_nodes;
        GET_HOST_PORTS(req, recovery_nodes, recovery_nodes);
        ASSERT_TRUE(recovery_nodes.empty());
    }
    // Test GET_HOST_PORTS-2.
    {
        replication::configuration_recovery_request req;
        req.__set_recovery_nodes(kAddres);
        std::vector<host_port> recovery_nodes;
        GET_HOST_PORTS(req, recovery_nodes, recovery_nodes);
        ASSERT_EQ(kHps, recovery_nodes);
    }
    // Test GET_HOST_PORTS-2.
    {
        replication::configuration_recovery_request req;
        req.__set_recovery_nodes(kAddres);
        req.__set_hp_recovery_nodes(kHps);
        std::vector<host_port> recovery_nodes;
        GET_HOST_PORTS(req, recovery_nodes, recovery_nodes);
        ASSERT_EQ(kHps, recovery_nodes);
    }

    // Test SET_IP_AND_HOST_PORT.
    {
        fd::beacon_msg beacon;
        SET_IP_AND_HOST_PORT(beacon, from_node, kAddr1, kHp1);
        ASSERT_EQ(kAddr1, beacon.from_node);
        ASSERT_EQ(kHp1, beacon.hp_from_node);
    }

    // Test SET_IP_AND_HOST_PORT_BY_DNS.
    {
        fd::beacon_msg beacon;
        SET_IP_AND_HOST_PORT_BY_DNS(beacon, from_node, kHp1);
        ASSERT_EQ(kAddr1, beacon.from_node);
        ASSERT_EQ(kHp1, beacon.hp_from_node);
    }

    // Test RESET_IP_AND_HOST_PORT.
    {
        fd::beacon_msg beacon;
        SET_IP_AND_HOST_PORT_BY_DNS(beacon, from_node, kHp1);
        ASSERT_EQ(kAddr1, beacon.from_node);
        ASSERT_EQ(kHp1, beacon.hp_from_node);
        RESET_IP_AND_HOST_PORT(beacon, from_node);
        ASSERT_FALSE(beacon.from_node);
        ASSERT_FALSE(beacon.hp_from_node);
    }

    // Test ADD_IP_AND_HOST_PORT.
    {
        partition_configuration pc;
        ADD_IP_AND_HOST_PORT(pc, secondaries, kAddr1, kHp1);
        ASSERT_EQ(1, pc.secondaries.size());
        ASSERT_EQ(1, pc.hp_secondaries.size());
        ASSERT_EQ(kAddr1, pc.secondaries[0]);
        ASSERT_EQ(kHp1, pc.hp_secondaries[0]);
        ADD_IP_AND_HOST_PORT(pc, secondaries, kAddr2, kHp2);
        ASSERT_EQ(2, pc.secondaries.size());
        ASSERT_EQ(2, pc.hp_secondaries.size());
        ASSERT_EQ(kAddr2, pc.secondaries[1]);
        ASSERT_EQ(kHp2, pc.hp_secondaries[1]);
    }

    // Test ADD_IP_AND_HOST_PORT_BY_DNS.
    {
        partition_configuration pc;
        ADD_IP_AND_HOST_PORT_BY_DNS(pc, secondaries, kHp1);
        ASSERT_EQ(1, pc.secondaries.size());
        ASSERT_EQ(1, pc.hp_secondaries.size());
        ASSERT_EQ(kAddr1, pc.secondaries[0]);
        ASSERT_EQ(kHp1, pc.hp_secondaries[0]);
        ADD_IP_AND_HOST_PORT_BY_DNS(pc, secondaries, kHp2);
        ASSERT_EQ(2, pc.secondaries.size());
        ASSERT_EQ(2, pc.hp_secondaries.size());
        ASSERT_EQ(kAddr2, pc.secondaries[1]);
        ASSERT_EQ(kHp2, pc.hp_secondaries[1]);
    }

    // Test SET_IPS_AND_HOST_PORTS_BY_DNS.
    {
        partition_configuration pc;
        SET_IPS_AND_HOST_PORTS_BY_DNS(pc, secondaries, kHp1);
        ASSERT_EQ(1, pc.secondaries.size());
        ASSERT_EQ(1, pc.hp_secondaries.size());
        ASSERT_EQ(kAddr1, pc.secondaries[0]);
        ASSERT_EQ(kHp1, pc.hp_secondaries[0]);

        SET_IPS_AND_HOST_PORTS_BY_DNS(pc, secondaries, kHp2, kHp3);
        ASSERT_EQ(2, pc.secondaries.size());
        ASSERT_EQ(2, pc.hp_secondaries.size());
        ASSERT_EQ(kAddr2, pc.secondaries[0]);
        ASSERT_EQ(kHp2, pc.hp_secondaries[0]);
        ASSERT_EQ(kAddr3, pc.secondaries[1]);
        ASSERT_EQ(kHp3, pc.hp_secondaries[1]);
    }

    // Test CLEAR_IP_AND_HOST_PORT.
    {
        partition_configuration pc;
        ADD_IP_AND_HOST_PORT(pc, secondaries, kAddr1, kHp1);
        CLEAR_IP_AND_HOST_PORT(pc, secondaries);
        ASSERT_TRUE(pc.secondaries.empty());
        ASSERT_TRUE(pc.hp_secondaries.empty());
    }

    // Test SET_VALUE_FROM_IP_AND_HOST_PORT.
    {
        static const int kProgress = 88;
        replication::bulk_load_response response;
        replication::partition_bulk_load_state primary_state;
        primary_state.__set_download_progress(kProgress);
        SET_VALUE_FROM_IP_AND_HOST_PORT(
            response, group_bulk_load_state, kAddr1, kHp1, primary_state);
        ASSERT_EQ(1, response.group_bulk_load_state.size());
        ASSERT_EQ(1, response.hp_group_bulk_load_state.size());
        ASSERT_EQ(kAddr1, response.group_bulk_load_state.begin()->first);
        ASSERT_EQ(kHp1, response.hp_group_bulk_load_state.begin()->first);
        ASSERT_EQ(kProgress, response.group_bulk_load_state.begin()->second.download_progress);
        ASSERT_EQ(kProgress, response.hp_group_bulk_load_state.begin()->second.download_progress);
    }

    // Test SET_VALUE_FROM_HOST_PORT.
    {
        static const int kProgress = 88;
        replication::bulk_load_response response;
        replication::partition_bulk_load_state primary_state;
        primary_state.__set_download_progress(kProgress);
        SET_VALUE_FROM_HOST_PORT(response, group_bulk_load_state, kHp1, primary_state);
        ASSERT_EQ(1, response.group_bulk_load_state.size());
        ASSERT_EQ(1, response.hp_group_bulk_load_state.size());
        ASSERT_EQ(kAddr1, response.group_bulk_load_state.begin()->first);
        ASSERT_EQ(kHp1, response.hp_group_bulk_load_state.begin()->first);
        ASSERT_EQ(kProgress, response.group_bulk_load_state.begin()->second.download_progress);
        ASSERT_EQ(kProgress, response.hp_group_bulk_load_state.begin()->second.download_progress);
    }

    // Test FMT_HOST_PORT_AND_IP.
    {
        fd::beacon_msg beacon;
        SET_IP_AND_HOST_PORT_BY_DNS(beacon, from_node, kHp1);
        ASSERT_EQ(fmt::format("{}({})", kHp1, kAddr1), FMT_HOST_PORT_AND_IP(beacon, from_node));
    }
}

} // namespace dsn
