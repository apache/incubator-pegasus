// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/utility/utils.h>

#include <dsn/tool-api/rpc_address.h>
#include <gtest/gtest.h>

namespace dsn {
namespace replication {

TEST(ip_to_hostname, localhost)
{
    std::string hostname_result;

    const std::string success_ip = "127.0.0.1";
    const std::string expected_hostname = "localhost";

    const std::string success_ip_port = "127.0.0.1:23010";
    const std::string expected_hostname_port = "localhost:23010";

    const std::string failed_ip = "123.456.789.111";
    const std::string failed_ip_port = "123.456.789.111:23010";

    const std::string success_ip_list = "127.0.0.1,127.0.0.1,127.0.0.1";
    const std::string expected_hostname_list = "localhost,localhost,localhost";

    const std::string success_ip_port_list = "127.0.0.1:8080,127.0.0.1:8080,127.0.0.1:8080";
    const std::string expected_hostname_port_list = "localhost:8080,localhost:8080,localhost:8080";

    rpc_address rpc_example_success, rpc_example_failed;
    rpc_example_success.assign_ipv4(success_ip.c_str(), 23010);
    rpc_example_failed.assign_ipv4(failed_ip.c_str(), 23010);

    // static bool hostname(const rpc_address &address,std::string *hostname_result);
    ASSERT_TRUE(dsn::utils::hostname(rpc_example_success, &hostname_result));
    ASSERT_STREQ(expected_hostname_port.c_str(), hostname_result.c_str());

    ASSERT_FALSE(dsn::utils::hostname(rpc_example_failed, &hostname_result));

    // static bool hostname_from_ip(uint32_t ip, std::string* hostname_result);
    ASSERT_TRUE(dsn::utils::hostname_from_ip(htonl(rpc_example_success.ip()), &hostname_result));
    ASSERT_STREQ(expected_hostname.c_str(), hostname_result.c_str());

    ASSERT_FALSE(dsn::utils::hostname_from_ip(htonl(rpc_example_failed.ip()), &hostname_result));

    // static bool hostname_from_ip(const char *ip,std::string *hostname_result);
    ASSERT_TRUE(dsn::utils::hostname_from_ip(success_ip.c_str(), &hostname_result));
    ASSERT_STREQ(expected_hostname.c_str(), hostname_result.c_str());

    ASSERT_FALSE(dsn::utils::hostname_from_ip(failed_ip.c_str(), &hostname_result));
    ASSERT_STREQ(failed_ip.c_str(), hostname_result.c_str());

    // static bool hostname_from_ip_port(const char *ip_port,std::string *hostname_result);
    ASSERT_TRUE(dsn::utils::hostname_from_ip_port(success_ip_port.c_str(), &hostname_result));
    ASSERT_STREQ(expected_hostname_port.c_str(), hostname_result.c_str());

    ASSERT_FALSE(dsn::utils::hostname_from_ip_port(failed_ip_port.c_str(), &hostname_result));
    ASSERT_STREQ(failed_ip_port.c_str(), hostname_result.c_str());

    // static bool list_hostname_from_ip(const char *ip_port_list,std::string
    // *hostname_result_list);
    ASSERT_TRUE(dsn::utils::list_hostname_from_ip(success_ip_list.c_str(), &hostname_result));
    ASSERT_STREQ(expected_hostname_list.c_str(), hostname_result.c_str());

    ASSERT_FALSE(dsn::utils::list_hostname_from_ip("127.0.0.1,127.0.0.23323,111127.0.0.3",
                                                   &hostname_result));
    ASSERT_STREQ("localhost,127.0.0.23323,111127.0.0.3", hostname_result.c_str());

    ASSERT_FALSE(dsn::utils::list_hostname_from_ip("123.456.789.111,127.0.0.1", &hostname_result));
    ASSERT_STREQ("123.456.789.111,localhost", hostname_result.c_str());

    // static bool list_hostname_from_ip_port(const char *ip_port_list,std::string
    // *hostname_result_list);
    ASSERT_TRUE(
        dsn::utils::list_hostname_from_ip_port(success_ip_port_list.c_str(), &hostname_result));
    ASSERT_STREQ(expected_hostname_port_list.c_str(), hostname_result.c_str());

    ASSERT_FALSE(dsn::utils::list_hostname_from_ip_port(
        "127.0.3333.1:23456,1127.0.0.2:22233,127.0.0.1:8080", &hostname_result));
    ASSERT_STREQ("127.0.3333.1:23456,1127.0.0.2:22233,localhost:8080", hostname_result.c_str());
}

} // namespace replication
} // namespace dsn
