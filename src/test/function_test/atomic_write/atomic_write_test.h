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

#pragma once

#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "client/replication_ddl_client.h"
#include "rpc/rpc_host_port.h"
#include "utils/ports.h"

namespace pegasus {

class pegasus_client;

struct atomic_write_case
{
    bool atomic_idempotent;
};

// The base fixture for each type of atomic write.
class AtomicWriteTest : public testing::TestWithParam<atomic_write_case>
{
public:
    ~AtomicWriteTest() override = default;

protected:
    explicit AtomicWriteTest(std::string table_name_prefix);

    static void SetUpTestSuite();

    void SetUp() override;
    void TearDown() override;

    static const std::string kClusterName;
    static const int32_t kPartitionCount;

    const std::string _table_name_prefix;

    std::vector<dsn::host_port> _meta_list;
    std::unique_ptr<dsn::replication::replication_ddl_client> _ddl_client;

    std::string _table_name;

    pegasus_client *_client{nullptr};

    std::string _hash_key;

private:
    DISALLOW_COPY_AND_ASSIGN(AtomicWriteTest);
    DISALLOW_MOVE_AND_ASSIGN(AtomicWriteTest);
};

// Generate both idempotent and non-idempotent cases for atomic writes.
std::vector<atomic_write_case> generate_atomic_write_cases();

} // namespace pegasus
