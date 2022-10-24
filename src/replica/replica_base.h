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

#include "common/gpid.h"
#include "utils/string_view.h"

namespace dsn {
namespace replication {

/// Base class for types that are one-instance-per-replica.
struct replica_base
{
    replica_base(gpid id, string_view name, string_view app_name)
        : _gpid(id), _name(name), _app_name(app_name)
    {
    }

    explicit replica_base(replica_base *rhs)
        : replica_base(rhs->get_gpid(), rhs->replica_name(), rhs->_app_name)
    {
    }

    gpid get_gpid() const { return _gpid; }

    const char *replica_name() const { return _name.c_str(); }

    const char *app_name() const { return _app_name.c_str(); }

    const char *log_prefix() const { return _name.c_str(); }

private:
    const gpid _gpid;
    const std::string _name;
    const std::string _app_name;
};

} // namespace replication
} // namespace dsn
