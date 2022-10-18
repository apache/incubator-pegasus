/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <vector>
#include <map>
#include "utils/singleton.h"
#include "utils/zlocks.h"
#include "utils/rpc_address.h"
#include "client/partition_resolver.h"

namespace dsn {
namespace replication {

class partition_resolver_manager : public dsn::utils::singleton<partition_resolver_manager>
{
public:
    partition_resolver_ptr find_or_create(const char *cluster_name,
                                          const std::vector<rpc_address> &meta_list,
                                          const char *app_name);

private:
    dsn::zlock _lock;
    // cluster_name -> <app_name, resolver>
    std::map<std::string, std::map<std::string, partition_resolver_ptr>> _resolvers;
};

} // namespace replication
} // namespace dsn
