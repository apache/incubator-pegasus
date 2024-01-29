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

#include <cstdint>
#include <type_traits>

#include "utils/enum_helper.h"

namespace dsn {
namespace ranger {

// ACL type defined in Range service for RPC matching policy
enum class access_type : uint8_t
{
    kInvalid = 0,
    kRead = 1,
    kWrite = 1 << 1,
    kCreate = 1 << 2,
    kDrop = 1 << 3,
    kList = 1 << 4,
    kMetadata = 1 << 5,
    kControl = 1 << 6
};
ENUM_BEGIN(access_type, access_type::kInvalid)
ENUM_REG(access_type::kRead)
ENUM_REG(access_type::kWrite)
ENUM_REG(access_type::kCreate)
ENUM_REG(access_type::kDrop)
ENUM_REG(access_type::kList)
ENUM_REG(access_type::kMetadata)
ENUM_REG(access_type::kControl)
ENUM_END(access_type)

using act = std::underlying_type<access_type>::type;

inline access_type operator|(access_type lhs, access_type rhs)
{
    return access_type(static_cast<act>(lhs) | static_cast<act>(rhs));
}

inline access_type operator&(access_type lhs, access_type rhs)
{
    return access_type(static_cast<act>(lhs) & static_cast<act>(rhs));
}

inline access_type &operator|=(access_type &lhs, access_type rhs) { return lhs = lhs | rhs; }

const access_type kAccessTypeNone = access_type::kInvalid;
const access_type kAccessTypeAll = access_type::kRead | access_type::kWrite | access_type::kCreate |
                                   access_type::kDrop | access_type::kList |
                                   access_type::kMetadata | access_type::kControl;

} // namespace ranger
} // namespace dsn
