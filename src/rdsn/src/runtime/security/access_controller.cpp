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

#include "utils/flags.h"
#include "utils/strings.h"
#include "utils/smart_pointers.h"
#include "meta_access_controller.h"
#include "replica_access_controller.h"

namespace dsn {
namespace security {
DSN_DEFINE_bool("security", enable_acl, false, "whether enable access controller or not");
DSN_TAG_VARIABLE(enable_acl, FT_MUTABLE);

DSN_DEFINE_string("security", super_users, "", "super user for access controller");

access_controller::access_controller() { utils::split_args(FLAGS_super_users, _super_users, ','); }

access_controller::~access_controller() {}

bool access_controller::pre_check(const std::string &user_name)
{
    if (!FLAGS_enable_acl || _super_users.find(user_name) != _super_users.end()) {
        return true;
    }
    return false;
}

std::unique_ptr<access_controller> create_meta_access_controller()
{
    return make_unique<meta_access_controller>();
}

std::unique_ptr<access_controller> create_replica_access_controller(const std::string &name)
{
    return make_unique<replica_access_controller>(name);
}
} // namespace security
} // namespace dsn
