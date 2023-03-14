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

#include "job.h"

#include <utility>

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
