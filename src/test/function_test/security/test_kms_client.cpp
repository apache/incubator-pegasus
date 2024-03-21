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

#include <absl/strings/str_split.h>
#include <string.h>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "replica/kms_key_provider.h"
#include "replica/replication_app_base.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/flags.h"

DSN_DECLARE_bool(enable_acl);
DSN_DECLARE_string(cluster_name);
DSN_DECLARE_string(hadoop_kms_url);

class kms_client_test : public testing::Test
{
};

TEST_F(kms_client_test, test_generate_and_decrypt_encryption_key)
{
    if (strlen(FLAGS_hadoop_kms_url) == 0) {
        GTEST_SKIP() << "Set a proper 'hadoop_kms_url' in config.ini to enable this test.";
    }

    auto _key_provider = std::make_unique<dsn::security::kms_key_provider>(
        ::absl::StrSplit(FLAGS_hadoop_kms_url, ",", ::absl::SkipEmpty()), FLAGS_cluster_name);
    dsn::replication::kms_info info;

    // 1. generate encryption key.
    ASSERT_EQ(dsn::ERR_OK, _key_provider->GenerateEncryptionKey(&info).code());

    // 2. decrypt encryption key.
    std::string server_key;
    ASSERT_EQ(dsn::ERR_OK, _key_provider->DecryptEncryptionKey(info, &server_key).code());
    ASSERT_EQ(server_key.size(), info.encrypted_key.length());
    ASSERT_NE(server_key, info.encrypted_key);
}
