/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <string>

#include "rrdb/rrdb_types.h"
#include "utils/fmt_utils.h"
#include "gutil/map_util.h"

namespace pegasus {

inline std::string cas_check_type_to_string(dsn::apps::cas_check_type::type type)
{
    const auto *name = gutil::FindOrNull(dsn::apps::_cas_check_type_VALUES_TO_NAMES, type);
    if (dsn_unlikely(name == nullptr)) {
        return fmt::format("INVALID={}", type);
    }
    return *name;
}

inline bool cas_is_check_operand_needed(dsn::apps::cas_check_type::type type)
{
    return type >= dsn::apps::cas_check_type::CT_VALUE_MATCH_ANYWHERE;
}

} // namespace pegasus

namespace dsn {
namespace apps {
USER_DEFINED_ENUM_FORMATTER(cas_check_type::type)
USER_DEFINED_ENUM_FORMATTER(filter_type::type)
USER_DEFINED_ENUM_FORMATTER(mutate_operation::type)
} // namespace apps
} // namespace dsn
