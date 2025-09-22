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

#include <zookeeper/zookeeper.h>
#include <cstdio>
#include <string>

#include "gtest/gtest.h"
#include "utils/flags.h"
#include "zookeeper/test/zookeeper_session_test_base.h"
#include "zookeeper_session_test.h"

DSN_DECLARE_string(sasl_mechanisms_type);
DSN_DECLARE_string(sasl_user_name);
DSN_DECLARE_string(sasl_password_file);
DSN_DECLARE_string(sasl_password_encryption_scheme);

namespace dsn::dist {

namespace {

// The correct password plaintext and its encrypted strings.
const std::string kPlaintextPassword("mypassword");
const std::string kBase64Password("bXlwYXNzd29yZA==");

// Write the given `password` into the file. The `password` can be either unencrypted
// or encrypted.
void write_password(const std::string &password)
{
    FILE *f = fopen("sasl_auth.password", "w");
    ASSERT_NE(nullptr, f);
    ASSERT_EQ(password.size(), fwrite(password.c_str(), 1, password.size(), f));
    ASSERT_EQ(0, fclose(f));
}

// Configure SASL auth with specified `encryption_scheme`.
void config_sasl(const char *encryption_scheme)
{
    FLAGS_sasl_mechanisms_type = "DIGEST-MD5";
    FLAGS_sasl_user_name = "myuser";
    FLAGS_sasl_password_file = "sasl_auth.password";
    FLAGS_sasl_password_encryption_scheme = encryption_scheme;
}

} // anonymous namespace

class ZookeeperSessionSASLConnectTest : public ZookeeperSessionConnector
{
};

TEST_F(ZookeeperSessionSASLConnectTest, CorrectPlaintextPasswordByEmptyScheme)
{
    write_password(kPlaintextPassword);
    config_sasl("");
    test_connect(ZOO_CONNECTED_STATE);
}

TEST_F(ZookeeperSessionSASLConnectTest, CorrectPlaintextPasswordBySpecifiedScheme)
{
    write_password(kPlaintextPassword);
    config_sasl("plaintext");
    test_connect(ZOO_CONNECTED_STATE);
}

TEST_F(ZookeeperSessionSASLConnectTest, CorrectBase64Password)
{
    write_password(kBase64Password);
    config_sasl("base64");
    test_connect(ZOO_CONNECTED_STATE);
}

TEST_F(ZookeeperSessionSASLConnectTest, WrongPlaintextPasswordByEmptyScheme)
{
    write_password("password");
    config_sasl("");
    test_connect(ZOO_AUTH_FAILED_STATE);
}

TEST_F(ZookeeperSessionSASLConnectTest, WrongPlaintextPasswordBySpecifiedScheme)
{
    write_password("password");
    config_sasl("plaintext");
    test_connect(ZOO_AUTH_FAILED_STATE);
}

TEST_F(ZookeeperSessionSASLConnectTest, WrongBase64Password)
{
    write_password("cGFzc3dvcmQ="); // base64 encoding for "password".
    config_sasl("base64");
    test_connect(ZOO_AUTH_FAILED_STATE);
}

class ZookeeperSessionSASLAuthTest : public ZookeeperSessionTestBase
{
protected:
    ZookeeperSessionSASLAuthTest();
};

ZookeeperSessionSASLAuthTest::ZookeeperSessionSASLAuthTest()
{
    write_password(kBase64Password);
    config_sasl("base64");
    // test_connect() will be called in ZookeeperSessionTestBase::SetUp().
}

using ZookeeperSessionSASLAuthTestImpl = ::testing::Types<ZookeeperSessionSASLAuthTest>;
INSTANTIATE_TYPED_TEST_SUITE_P(ZookeeperSASLAuthTest,
                               ZookeeperSessionTest,
                               ZookeeperSessionSASLAuthTestImpl);

} // namespace dsn::dist
