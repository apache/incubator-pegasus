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

#include <string>

#include "utils_types.h"
#include "utils/enum_helper.h"
#include "utils/errors.h"

namespace dsn {

ENUM_BEGIN2(utils::checksum_type::type, checksum_type, utils::checksum_type::CST_INVALID)
ENUM_REG_WITH_CUSTOM_NAME(utils::checksum_type::CST_INVALID, invalid)
ENUM_REG_WITH_CUSTOM_NAME(utils::checksum_type::CST_NONE, none)
ENUM_REG_WITH_CUSTOM_NAME(utils::checksum_type::CST_MD5, md5)
ENUM_END2(utils::checksum_type::type, checksum_type)

// Calculate the checksum for a file.
//
// Parameters:
// - `file_path`: the path of the file.
// - `type`: decides which algorithm is used to calculate the checksum for the file.
// CST_NONE means do not calculate the checksum.
// - `result`: the output parameter that holds the resulting checksum.
//
// Return ok if succeed in calculating, otherwise return corresponding error.
error_s
calc_checksum(const std::string &file_path, utils::checksum_type::type type, std::string &result);

} // namespace dsn
