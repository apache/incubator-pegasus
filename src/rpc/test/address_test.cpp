/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <fmt/core.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "rpc/group_address.h"
#include "rpc/rpc_address.h"
#include "utils/errors.h"

namespace dsn {

static inline uint32_t host_ipv4(uint8_t sec1, uint8_t sec2, uint8_t sec3, uint8_t sec4)
{
    uint32_t ip = 0;
    ip |= (uint32_t)sec1 << 24;
    ip |= (uint32_t)sec2 << 16;
    ip |= (uint32_t)sec3 << 8;
    ip |= (uint32_t)sec4;
    return ip;
}

TEST(rpc_address_test, rpc_address_ipv4_from_host)
{
    struct resolve_test
    {
        std::string hostname;
        bool valid;
        std::set<uint32_t> expect_ips;

        resolve_test(std::string hn, bool v, std::set<uint32_t> expects)
            : hostname(std::move(hn)), valid(v), expect_ips(std::move(expects))
        {
        }
    };

    const resolve_test tests[] = {
        {"127.0.0.1", true, {host_ipv4(127, 0, 0, 1)}},
        {"0.0.0.0", true, {host_ipv4(0, 0, 0, 0)}},
        // on some systems "localhost" could be "127.0.1.1" (debian)
        {"localhost", true, {host_ipv4(127, 0, 0, 1), host_ipv4(127, 0, 1, 1)}},
        {"whatthefuckyoucanhavesuchahostname", false, {}}};

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    for (const auto &test : tests) {
        // Check ipv4_from_host()
        uint32_t ip;
        ASSERT_EQ(test.valid, rpc_address::ipv4_from_host(test.hostname, &ip).is_ok());

        // Check GetAddrInfo()
        AddrInfo result;
        ASSERT_EQ(test.valid, rpc_address::GetAddrInfo(test.hostname, hints, &result).is_ok())
            << test.hostname;
        if (test.valid) {
            ASSERT_GT(test.expect_ips.count(ip), 0) << test.hostname;

            ASSERT_EQ(result.get()->ai_family, AF_INET);
            auto *ipv4 = reinterpret_cast<struct sockaddr_in *>(result.get()->ai_addr);
            ASSERT_GT(test.expect_ips.count(ntohl(ipv4->sin_addr.s_addr)), 0) << test.hostname;
        }
    }
}

TEST(rpc_address_test, rpc_address_ipv4_from_network_interface)
{
    ASSERT_EQ(host_ipv4(127, 0, 0, 1), rpc_address::ipv4_from_network_interface("lo"));
    ASSERT_EQ(host_ipv4(0, 0, 0, 0),
              rpc_address::ipv4_from_network_interface("not_exist_interface"));
}

TEST(rpc_address_test, is_site_local_address)
{
    ASSERT_FALSE(rpc_address::is_site_local_address(htonl(host_ipv4(1, 2, 3, 4))));
    ASSERT_TRUE(rpc_address::is_site_local_address(htonl(host_ipv4(10, 235, 111, 111))));
    ASSERT_FALSE(rpc_address::is_site_local_address(htonl(host_ipv4(171, 11, 11, 11))));
    ASSERT_TRUE(rpc_address::is_site_local_address(htonl(host_ipv4(172, 16, 2, 2))));
    ASSERT_TRUE(rpc_address::is_site_local_address(htonl(host_ipv4(172, 31, 234, 255))));
    ASSERT_FALSE(rpc_address::is_site_local_address(htonl(host_ipv4(191, 128, 1, 2))));
    ASSERT_TRUE(rpc_address::is_site_local_address(htonl(host_ipv4(192, 168, 3, 45))));
    ASSERT_FALSE(rpc_address::is_site_local_address(htonl(host_ipv4(201, 201, 201, 201))));
}

TEST(rpc_address_test, is_docker_netcard)
{
    ASSERT_TRUE(rpc_address::is_docker_netcard("docker0", htonl(host_ipv4(1, 2, 3, 4))));
    ASSERT_TRUE(rpc_address::is_docker_netcard("10docker5", htonl(host_ipv4(4, 5, 6, 8))));
    ASSERT_FALSE(rpc_address::is_docker_netcard("eth0", htonl(host_ipv4(192, 168, 123, 123))));
    ASSERT_TRUE(rpc_address::is_docker_netcard("eth0", htonl(host_ipv4(172, 17, 42, 1))));
}

TEST(rpc_address_test, rpc_address_to_string)
{
    {
        rpc_address addr(host_ipv4(127, 0, 0, 1), 8080);
        ASSERT_STREQ("127.0.0.1:8080", addr.to_string());
    }

    {
        const char *name = "test_group";
        rpc_address addr;
        addr.assign_group(name);
        ASSERT_STREQ(name, addr.to_string());
    }

    {
        rpc_address addr;
        ASSERT_STREQ("invalid address", addr.to_string());
    }
}

TEST(rpc_address_test, dsn_address_build)
{
    {
        rpc_address addr(host_ipv4(127, 0, 0, 1), 8080);
        ASSERT_EQ(HOST_TYPE_IPV4, addr.type());
        ASSERT_EQ(host_ipv4(127, 0, 0, 1), addr.ip());
        ASSERT_EQ(8080, addr.port());

        ASSERT_TRUE(rpc_address::from_ip_port("127.0.0.1", 8080) ==
                        rpc_address::from_host_port("localhost", 8080) ||
                    rpc_address::from_ip_port("127.0.1.1", 8080) ==
                        rpc_address::from_host_port("localhost", 8080));
        ASSERT_EQ(addr, rpc_address::from_ip_port("127.0.0.1", 8080));
        ASSERT_EQ(addr, rpc_address(host_ipv4(127, 0, 0, 1), 8080));
    }

    {
        const char *name = "test_group";
        rpc_address addr;
        addr.assign_group(name);

        ASSERT_EQ(HOST_TYPE_GROUP, addr.type());
        ASSERT_STREQ(name, addr.group_address()->name());
        ASSERT_EQ(1, addr.group_address()->get_count());
    }
}

TEST(rpc_address_test, operators)
{
    rpc_address addr(1234, 123);
    ASSERT_EQ(addr, addr);

    {
        rpc_address new_addr(addr);
        ASSERT_EQ(addr, new_addr);
    }

    {
        rpc_address new_addr(1234, 321);
        ASSERT_NE(addr, new_addr);
    }

    rpc_address addr_grp;
    ASSERT_EQ(addr_grp, addr_grp);
    ASSERT_NE(addr, addr_grp);

    addr_grp.assign_group("test_group");
    ASSERT_TRUE(addr_grp.group_address()->add(addr));
    ASSERT_NE(addr, addr_grp);

    {
        rpc_address new_addr_grp(addr_grp);
        ASSERT_EQ(addr_grp, new_addr_grp);
    }
}

TEST(rpc_address_test, rpc_group_address)
{
    const auto addr = rpc_address::from_ip_port("127.0.0.1", 8080);
    rpc_address invalid_addr;
    const auto addr2 = rpc_address::from_ip_port("127.0.0.1", 8081);

    rpc_address t;
    t.assign_group("test_group");
    ASSERT_EQ(HOST_TYPE_GROUP, t.type());
    rpc_group_address *g = t.group_address();
    ASSERT_EQ(std::string("test_group"), g->name());
    ASSERT_EQ(1, g->get_count());

    // { }
    ASSERT_FALSE(g->remove(addr));
    ASSERT_FALSE(g->contains(addr));
    ASSERT_EQ(0u, g->members().size());
    ASSERT_EQ(invalid_addr, g->random_member());
    ASSERT_EQ(invalid_addr, g->next(addr));
    ASSERT_EQ(invalid_addr, g->leader());
    ASSERT_EQ(invalid_addr, g->possible_leader());

    // { addr }
    ASSERT_TRUE(g->add(addr));
    ASSERT_FALSE(g->add(addr));
    ASSERT_TRUE(g->contains(addr));
    ASSERT_EQ(1u, g->members().size());
    ASSERT_EQ(addr, g->members().at(0));
    ASSERT_EQ(addr, g->random_member());
    ASSERT_EQ(addr, g->next(addr));
    ASSERT_EQ(addr, g->next(invalid_addr));
    ASSERT_EQ(addr, g->next(addr2));
    ASSERT_EQ(invalid_addr, g->leader());
    ASSERT_EQ(addr, g->possible_leader());

    // { addr* }
    g->set_leader(addr);
    ASSERT_TRUE(g->contains(addr));
    ASSERT_EQ(1u, g->members().size());
    ASSERT_EQ(addr, g->members().at(0));
    ASSERT_EQ(addr, g->leader());
    ASSERT_EQ(addr, g->possible_leader());

    // { addr, addr2* }
    g->set_leader(addr2);
    ASSERT_TRUE(g->contains(addr));
    ASSERT_TRUE(g->contains(addr2));
    ASSERT_EQ(2u, g->members().size());
    ASSERT_EQ(addr, g->members().at(0));
    ASSERT_EQ(addr2, g->members().at(1));
    ASSERT_EQ(addr2, g->leader());
    ASSERT_EQ(addr2, g->possible_leader());
    ASSERT_EQ(addr, g->next(addr2));
    ASSERT_EQ(addr2, g->next(addr));

    // { addr, addr2 }
    g->set_leader(invalid_addr);
    ASSERT_TRUE(g->contains(addr));
    ASSERT_TRUE(g->contains(addr2));
    ASSERT_EQ(2u, g->members().size());
    ASSERT_EQ(addr, g->members().at(0));
    ASSERT_EQ(addr2, g->members().at(1));
    ASSERT_EQ(invalid_addr, g->leader());

    // { addr*, addr2 }
    g->set_leader(addr);
    ASSERT_TRUE(g->contains(addr));
    ASSERT_TRUE(g->contains(addr2));
    ASSERT_EQ(2u, g->members().size());
    ASSERT_EQ(addr, g->members().at(0));
    ASSERT_EQ(addr2, g->members().at(1));
    ASSERT_EQ(addr, g->leader());

    // { uri_addr }
    ASSERT_TRUE(g->remove(addr));
    ASSERT_FALSE(g->contains(addr));
    ASSERT_TRUE(g->contains(addr2));
    ASSERT_EQ(1u, g->members().size());
    ASSERT_EQ(addr2, g->members().at(0));
    ASSERT_EQ(invalid_addr, g->leader());

    // { }
    ASSERT_TRUE(g->remove(addr2));
    ASSERT_FALSE(g->contains(addr2));
    ASSERT_EQ(0u, g->members().size());
    ASSERT_EQ(invalid_addr, g->leader());
}

TEST(rpc_address_test, from_host_port)
{
    struct resolve_test
    {
        std::string host_port;
        std::set<std::string> expect_ip_ports;
        bool valid_ip_port;
        bool valid_host_port;

        resolve_test(std::string test, std::set<std::string> expects, bool v_ip, bool v_hp)
            : host_port(std::move(test)),
              expect_ip_ports(std::move(expects)),
              valid_ip_port(v_ip),
              valid_host_port(v_hp)
        {
        }

        resolve_test(std::string test, bool val)
            : host_port(std::move(test)),
              expect_ip_ports({host_port}),
              valid_ip_port(val),
              valid_host_port(val)
        {
        }
    };

    const resolve_test tests[] = {
        {"127.0.0.1:8080", true},
        {"127.0.0.1:", false},
        {"172.16.254.1:1234", true},
        {"0.0.0.0:1234", {"0.0.0.0:1234"}, true, true},
        {"172.16.254.1:222222", false},
        {"172.16.254.1", false},
        {"2222,123,33,1:8080", false},
        {"123.456.789.1:8080", false},
        // "001.223.110.002" can be resolved by host, but not by IP.
        {"001.223.110.002:8080", {"1.223.110.2:8080"}, false, true},
        {"172.16.254.1.8080", false},
        {"172.16.254.1:8080.", false},
        {"127.0.0.11:123!", false},
        {"127.0.0.11:123", true},
        {"localhost:34601", {"127.0.0.1:34601", "127.0.1.1:34601"}, false, true},
        {"localhost:3460100022212312312213", false},
        {"localhost:-12", false},
        {"localhost", false},
        {"localhost:", false},
        {"whatthefuckyoucanhavesuchahostname", false},
        {"localhost:1@2", false}};

    for (const auto &test : tests) {
        // Check from_host_port().
        const auto host_port_result = dsn::rpc_address::from_host_port(test.host_port);
        ASSERT_EQ(test.valid_host_port, static_cast<bool>(host_port_result)) << test.host_port;
        if (host_port_result) {
            ASSERT_GT(test.expect_ip_ports.count(host_port_result.to_string()), 0);
        }

        // Check from_ip_port().
        const auto ip_port_result = dsn::rpc_address::from_ip_port(test.host_port);
        ASSERT_EQ(test.valid_ip_port, static_cast<bool>(ip_port_result));
        if (ip_port_result) {
            ASSERT_GT(test.expect_ip_ports.count(ip_port_result.to_string()), 0);
        }

        // Check they are equal.
        if (test.valid_host_port && test.valid_ip_port) {
            ASSERT_EQ(host_port_result, ip_port_result);
        }
    }
}

TEST(rpc_address_test, from_host_port2)
{
    struct resolve_test
    {
        std::string host;
        uint16_t port;
        std::set<std::string> expect_ip_ports;
        bool valid_ip_port;
        bool valid_host_port;

        resolve_test(
            std::string test, uint16_t p, std::set<std::string> expects, bool v_ip, bool v_hp)
            : host(std::move(test)),
              port(p),
              expect_ip_ports(std::move(expects)),
              valid_ip_port(v_ip),
              valid_host_port(v_hp)
        {
        }

        resolve_test(std::string test, uint16_t p, bool val)
            : host(std::move(test)),
              port(p),
              expect_ip_ports({fmt::format("{}:{}", host, port)}),
              valid_ip_port(val),
              valid_host_port(val)
        {
        }
    };

    const resolve_test tests[] = {
        {"127.0.0.1", 8080, true},
        {"172.16.254.1", 1234, true},
        {"172.16.254.1:", 1234, false},
        {"2222,123,33,1", 8080, false},
        {"123.456.789.1", 8080, false},
        {"0.0.0.0", 1234, {"0.0.0.0:1234"}, true, true},
        // "001.223.110.002" can be resolved by host, but not by IP.
        {"001.223.110.002", 8080, {"1.223.110.2:8080"}, false, true},
        {"127.0.0.11", 123, true},
        {"localhost", 34601, {"127.0.0.1:34601", "127.0.1.1:34601"}, false, true},
        {"localhost:", 34601, false},
        {"whatthefuckyoucanhavesuchahostname", 34601, false}};

    for (const auto &test : tests) {
        // Check from_host_port().
        const auto host_port_result = dsn::rpc_address::from_host_port(test.host, test.port);
        ASSERT_EQ(test.valid_host_port, static_cast<bool>(host_port_result)) << test.host;
        if (host_port_result) {
            ASSERT_GT(test.expect_ip_ports.count(host_port_result.to_string()), 0)
                << test.host << " " << host_port_result.to_string();
        }

        // Check from_ip_port().
        const auto ip_port_result = dsn::rpc_address::from_ip_port(test.host, test.port);
        ASSERT_EQ(test.valid_ip_port, static_cast<bool>(ip_port_result));
        if (ip_port_result) {
            ASSERT_GT(test.expect_ip_ports.count(ip_port_result.to_string()), 0);
        }

        // Check they are equal.
        if (test.valid_host_port && test.valid_ip_port) {
            ASSERT_EQ(host_port_result, ip_port_result);
        }
    }
}

} // namespace dsn
