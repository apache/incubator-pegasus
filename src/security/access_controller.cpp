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

#include "access_controller.h"

#include "meta_access_controller.h"
#include "replica_access_controller.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"

DSN_DEFINE_bool(security, enable_acl, false, "whether enable access controller or not");
DSN_DEFINE_bool(security,
                enable_ranger_acl,
                false,
                "whether enable access controller integrate to Apache Ranger or not");
DSN_DEFINE_string(security,
                  super_users,
                  "",
                  "super users for access controller, comma-separated list of user names");
DSN_DEFINE_string(security,
                  encryption_cluster_key_name,
                  "pegasus_cluster_key",
                  "Name of the cluster key that is used to encrypt server encryption keys as"
                  "stored in Ranger KMS.");

namespace dsn {
namespace security {

access_controller::access_controller()
{
    // when FLAGS_enable_ranger_acl is true, FLAGS_enable_acl must be true.
    // TODO(wanghao): check with DSN_DEFINE_group_validator().
    CHECK(!FLAGS_enable_ranger_acl || FLAGS_enable_acl,
          "when FLAGS_enable_ranger_acl is true, FLAGS_enable_acl must be true too");
    utils::split_args(FLAGS_super_users, _super_users, ',');
}

access_controller::~access_controller() {}

bool access_controller::is_enable_ranger_acl() const { return FLAGS_enable_ranger_acl; }

bool access_controller::is_super_user(const std::string &user_name) const
{
    return _super_users.find(user_name) != _super_users.end();
}

std::shared_ptr<access_controller> create_meta_access_controller(
    const std::shared_ptr<ranger::ranger_resource_policy_manager> &policy_manager)
{
    return std::make_shared<meta_access_controller>(policy_manager);
}

std::unique_ptr<access_controller> create_replica_access_controller(const std::string &replica_name)
{
    return std::make_unique<replica_access_controller>(replica_name);
}
} // namespace security
} // namespace dsn
