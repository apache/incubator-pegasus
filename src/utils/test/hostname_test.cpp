// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arpa/inet.h>
#include <gtest/gtest.h>

#include "runtime/rpc/rpc_address.h"
#include "utils/utils.h"

namespace dsn {
namespace replication {

TEST(ip_to_hostname, ipv4_validate)
{
    rpc_address rpc_test_ipv4;
    struct ip_test
    {
        std::string ip;
        bool result;
    } tests[] = {{"127.0.0.1:8080", true},
                 {"172.16.254.1:1234", true},
                 {"172.16.254.1:222222", false},
                 {"172.16.254.1", false},
                 {"2222,123,33,1:8080", false},
                 {"123.456.789.1:8080", false},
                 {"001.223.110.002:8080", false},
                 {"172.16.254.1.8080", false},
                 {"172.16.254.1:8080.", false},
                 {"127.0.0.11:123!", false},
                 {"127.0.0.11:123", true},
                 {"localhost:34601", true},
                 {"localhost:3460100022212312312213", false},
                 {"localhost:-12", false},
                 {"localhost:1@2", false}};

    for (auto test : tests) {
        ASSERT_EQ(rpc_test_ipv4.from_string_ipv4(test.ip.c_str()), test.result);
    }
}

TEST(ip_to_hostname, localhost)
{
    std::string hostname_result;

    const std::string valid_ip = "127.0.0.1";
    const std::string expected_hostname = "localhost";

    const std::string valid_ip_port = "127.0.0.1:23010";
    const std::string expected_hostname_port = "localhost:23010";

    const std::string valid_ip_list = "127.0.0.1,127.0.0.1,127.0.0.1";
    const std::string expected_hostname_list = "localhost,localhost,localhost";

    const std::string valid_ip_port_list = "127.0.0.1:8080,127.0.0.1:8080,127.0.0.1:8080";
    const std::string expected_hostname_port_list = "localhost:8080,localhost:8080,localhost:8080";

    rpc_address rpc_example_valid;
    rpc_example_valid.assign_ipv4(valid_ip.c_str(), 23010);

    // static bool hostname(const rpc_address &address,std::string *hostname_result);
    ASSERT_TRUE(dsn::utils::hostname(rpc_example_valid, &hostname_result));
    ASSERT_EQ(expected_hostname_port, hostname_result);

    // static bool hostname_from_ip(uint32_t ip, std::string* hostname_result);
    ASSERT_TRUE(dsn::utils::hostname_from_ip(htonl(rpc_example_valid.ip()), &hostname_result));
    ASSERT_EQ(expected_hostname, hostname_result);

    // static bool hostname_from_ip(const char *ip,std::string *hostname_result);
    ASSERT_TRUE(dsn::utils::hostname_from_ip(valid_ip.c_str(), &hostname_result));
    ASSERT_EQ(expected_hostname, hostname_result);

    // static bool hostname_from_ip_port(const char *ip_port,std::string *hostname_result);
    ASSERT_TRUE(dsn::utils::hostname_from_ip_port(valid_ip_port.c_str(), &hostname_result));
    ASSERT_EQ(expected_hostname_port, hostname_result);

    // static bool list_hostname_from_ip(const char *ip_port_list,std::string
    // *hostname_result_list);
    ASSERT_TRUE(dsn::utils::list_hostname_from_ip(valid_ip_list.c_str(), &hostname_result));
    ASSERT_EQ(expected_hostname_list, hostname_result);

    ASSERT_FALSE(dsn::utils::list_hostname_from_ip("127.0.0.1,127.0.0.23323,111127.0.0.3",
                                                   &hostname_result));
    ASSERT_EQ("localhost,127.0.0.23323,111127.0.0.3", hostname_result);

    ASSERT_FALSE(dsn::utils::list_hostname_from_ip("123.456.789.111,127.0.0.1", &hostname_result));
    ASSERT_EQ("123.456.789.111,localhost", hostname_result);

    // static bool list_hostname_from_ip_port(const char *ip_port_list,std::string
    // *hostname_result_list);
    ASSERT_TRUE(
        dsn::utils::list_hostname_from_ip_port(valid_ip_port_list.c_str(), &hostname_result));
    ASSERT_EQ(expected_hostname_port_list, hostname_result);

    ASSERT_FALSE(dsn::utils::list_hostname_from_ip_port(
        "127.0.3333.1:23456,1127.0.0.2:22233,127.0.0.1:8080", &hostname_result));
    ASSERT_EQ("127.0.3333.1:23456,1127.0.0.2:22233,localhost:8080", hostname_result);
}

TEST(ip_to_hostname, invalid_ip)
{

    std::string hostname_result;
    const std::string invalid_ip = "123.456.789.111";
    const std::string invalid_ip_port = "123.456.789.111:23010";

    ASSERT_FALSE(dsn::utils::hostname_from_ip(invalid_ip.c_str(), &hostname_result));
    ASSERT_EQ(invalid_ip, hostname_result);

    ASSERT_FALSE(dsn::utils::hostname_from_ip_port(invalid_ip_port.c_str(), &hostname_result));
    ASSERT_EQ(invalid_ip_port, hostname_result);
}

} // namespace replication
} // namespace dsn
