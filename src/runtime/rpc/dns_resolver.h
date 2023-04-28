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
#include <unordered_map>

#include "runtime/rpc/group_address.h"
#include "runtime/rpc/group_host_port.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_host_port.h"
#include "utils/errors.h"
#include "utils/synchronize.h"

namespace dsn {

class dns_resolver
{
public:
    explicit dns_resolver() = default;

    void add_item(const host_port &hp, const rpc_address &addr);

    rpc_address resolve_address(const host_port &hp);

private:
    bool get_cached_addresses(const host_port &hp, std::vector<rpc_address> &addresses);

    error_s resolve_addresses(const host_port &hp, std::vector<rpc_address> &addresses);

    error_s do_resolution(const host_port &hp, std::vector<rpc_address> &addresses);

    mutable utils::rw_lock_nr _lock;
    std::unordered_map<host_port, rpc_address> _dsn_cache;
};

} // namespace dsn
