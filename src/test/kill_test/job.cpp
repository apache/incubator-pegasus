// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "job.h"

namespace pegasus {
namespace test {

void job::append_addrs(const std::string &addr)
{
    addrs.emplace_back(addr);
    status.insert(make_pair(addr, true));
}
std::string job::get_addr_by_index(int index)
{
    if (index < addrs.size())
        return addrs[index];
    return std::string();
}

void job::set_name(const std::string &_name) { name = _name; }
}
} // end namespace
