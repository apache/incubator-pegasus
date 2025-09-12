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

#include <memory>

#include "gtest/gtest.h"
#include "utils/flags.h"
#include "zookeeper_session_sasl_auth_test.h"
#include "zookeeper_session_test.h"

DSN_DECLARE_string(sasl_mechanisms_type);
DSN_DECLARE_string(sasl_user_name);
DSN_DECLARE_string(sasl_password_file);
DSN_DECLARE_string(sasl_password_encryption_scheme);

namespace dsn::dist {

ZookeeperSessionSASLAuthTest::ZookeeperSessionSASLAuthTest()
{
    FLAGS_sasl_mechanisms_type = "DIGEST-MD5";
    FLAGS_sasl_user_name = "myuser";
    FLAGS_sasl_password_file = "sasl_auth.password";
    FLAGS_sasl_password_encryption_scheme = "base64";
}

using ZookeeperSessionSASLAuthTestImpl = ::testing::Types<ZookeeperSessionSASLAuthTest>;
INSTANTIATE_TYPED_TEST_SUITE_P(ZookeeperSASLAuthTest,
                               ZookeeperSessionTest,
                               ZookeeperSessionSASLAuthTestImpl);

} // namespace dsn::dist
