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

#include <netinet/in.h>
#include <string>

#include "gtest/gtest.h"
#include "runtime/rpc/rpc_address.h"
#include "utils/utils.h"

namespace dsn {
namespace replication {

TEST(ip_to_hostname, localhost)
{
    const std::string valid_ip = "127.0.0.1";
    const std::string expected_hostname = "localhost";

    const auto rpc_example_valid = rpc_address::from_ip_port(valid_ip, 23010);

    // bool hostname_from_ip(uint32_t ip, std::string *hostname_result)
    std::string hostname_result;
    ASSERT_TRUE(dsn::utils::hostname_from_ip(htonl(rpc_example_valid.ip()), &hostname_result));
    ASSERT_EQ(expected_hostname, hostname_result);
}

} // namespace replication
} // namespace dsn
