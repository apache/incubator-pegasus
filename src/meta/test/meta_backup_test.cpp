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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "backup_types.h"
#include "common/backup_common.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "gtest/gtest.h"
#include "meta/backup_engine.h"
#include "meta/meta_backup_service.h"
#include "meta/meta_data.h"
#include "meta/meta_rpc_types.h"
#include "meta/meta_service.h"
#include "meta/server_state.h"
#include "meta_test_base.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/zlocks.h"

namespace dsn {
namespace replication {


// TODO(heyuchen): implement it


} // namespace replication
} // namespace dsn
