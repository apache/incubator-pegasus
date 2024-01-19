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
#include <stdio.h>
#include <string.h>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "replica/kms_key_provider.h"
#include "replica/replication_app_base.h"
#include "test_util/test_util.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/flags.h"

namespace dsn {
DSN_DECLARE_string(cluster_name);

namespace security {
DSN_DECLARE_string(enable_acl);
DSN_DECLARE_string(super_users);
} // namespace security

namespace replication {
DSN_DECLARE_string(hadoop_kms_url);
} // namespace replication
} // namespace dsn

class KmsClientTest : public pegasus::encrypt_data_test_base
{
protected:
    KmsClientTest() : pegasus::encrypt_data_test_base()
    {
        dsn::FLAGS_cluster_name = "kudu_cluster_key";
        dsn::security::FLAGS_enable_acl = "true";
        dsn::security::FLAGS_super_users = "pegasus";
        dsn::replication::FLAGS_hadoop_kms_url = "";
    }
};

INSTANTIATE_TEST_CASE_P(, KmsClientTest, ::testing::Values(false, true));

TEST_P(KmsClientTest, test_generate_and_decrypt_encryption_key)
{
    if (strlen(dsn::replication::FLAGS_hadoop_kms_url) == 0 ||
        FLAGS_encrypt_data_at_rest == false) {
        GTEST_SKIP()
            << "Set kms_client_test.* configs in config-test.ini to enable kms_client_test.";
    }

    std::unique_ptr<dsn::security::KMSKeyProvider> key_provider;
    key_provider.reset(new dsn::security::KMSKeyProvider(
        ::absl::StrSplit(dsn::replication::FLAGS_hadoop_kms_url, ",", ::absl::SkipEmpty()),
        dsn::FLAGS_cluster_name));
    dsn::replication::kms_info kms_info;

    // 1. generate encryption key.
    printf("generate encryption key.\n");
    ASSERT_EQ(dsn::ERR_OK, key_provider->GenerateEncryptionKey(&kms_info).code());

    // 2. decrypt encryption key.
    printf("decrypt encryption key.\n");
    std::string server_key;
    ASSERT_EQ(dsn::ERR_OK, key_provider->DecryptEncryptionKey(kms_info, &server_key).code());
    ASSERT_EQ(server_key.size(), kms_info.eek.length());
    ASSERT_NE(server_key, kms_info.eek);
}
