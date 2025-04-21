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

#include "utils/checksum.h"

#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/ports.h"

namespace dsn {

error_s calc_checksum(const std::string &file_path, utils::checksum_type::type type, std::string &result)
{
    switch (type) {
        case utils::checksum_type::CST_MD5:
        const auto err = utils::filesystem::md5sum(file_path, result);
        if (dsn_unlikely(err != ERR_OK)) {
            return FMT_ERR(err, "md5sum failed: err={}", err);
        }

        break;

        case utils::checksum_type::CST_NONE:
        break;

    default:
        return FMT_ERR(ERR_NOT_IMPLEMENTED,
                       "checksum_type is not supported: val={}, str={}",
                       static_cast<int>(type),
                       enum_to_string(type));
    }

    return error_s::ok();
}

} // namespace dsn

