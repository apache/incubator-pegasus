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

#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "backup_types.h"
#include "client/replication_ddl_client.h"
#include "gtest/gtest.h"
#include "test/function_test/utils/test_util.h"
#include "test_util/test_util.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/test_macros.h"
#include "utils/utils.h"

using namespace dsn;
using namespace dsn::replication;
using namespace pegasus;

class backup_restore_test : public test_util
{
public:
    void TearDown() override
    {
        ASSERT_EQ(ERR_OK, ddl_client_->drop_app(table_name_, 0));
        ASSERT_EQ(ERR_OK, ddl_client_->drop_app(s_new_app_name, 0));
    }

    void wait_backup_complete(int64_t backup_id)
    {
        ASSERT_IN_TIME(
            [&] {
                auto resp = ddl_client_->query_backup(table_id_, backup_id).get_value();
                ASSERT_EQ(dsn::ERR_OK, resp.err);
                ASSERT_FALSE(resp.backup_items.empty());
                // we got only one backup_item for a certain app_id and backup_id.
                ASSERT_GT(resp.backup_items[0].end_time_ms, 0);
            },
            180);
    }

    void test_backup_and_restore(const std::string &user_specified_path = "")
    {
        NO_FATALS(wait_table_healthy(table_name_));
        NO_FATALS(write_data(s_num_of_rows));
        NO_FATALS(verify_data(s_num_of_rows));

        auto resp =
            ddl_client_->backup_app(table_id_, s_provider_type, user_specified_path).get_value();
        ASSERT_EQ(ERR_OK, resp.err);
        int64_t backup_id = resp.backup_id;
        NO_FATALS(wait_backup_complete(backup_id));
        ASSERT_EQ(ERR_OK,
                  ddl_client_->do_restore(s_provider_type,
                                          kClusterName,
                                          /* policy_name */ "",
                                          backup_id,
                                          table_name_,
                                          table_id_,
                                          s_new_app_name,
                                          /* skip_bad_partition */ false,
                                          user_specified_path));
        NO_FATALS(wait_table_healthy(s_new_app_name));
        NO_FATALS(verify_data(s_new_app_name, s_num_of_rows));
    }

private:
    static const uint32_t s_num_of_rows = 1000;
    static const std::string s_new_app_name;
    static const std::string s_provider_type;
};

const std::string backup_restore_test::s_new_app_name = "new_app";
const std::string backup_restore_test::s_provider_type = "local_service";

TEST_F(backup_restore_test, test_backup_and_restore) { test_backup_and_restore(); }

TEST_F(backup_restore_test, test_backup_and_restore_with_user_specified_path)
{
    test_backup_and_restore("test/path");
}
