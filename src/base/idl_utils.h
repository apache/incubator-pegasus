// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

namespace pegasus {

inline std::string cas_check_type_to_string(dsn::apps::cas_check_type::type type)
{
    using namespace dsn::apps;
    auto it = _cas_check_type_VALUES_TO_NAMES.find(type);
    if (it == _cas_check_type_VALUES_TO_NAMES.end()) {
        return std::string("INVALID=") + std::to_string(int(type));
    }
    return it->second;
}

inline bool cas_is_check_operand_needed(dsn::apps::cas_check_type::type type)
{
    return type >= dsn::apps::cas_check_type::CT_VALUE_MATCH_ANYWHERE;
}

} // namespace pegasus
