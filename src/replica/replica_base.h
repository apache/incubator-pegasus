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

#include <string>

#include "common/gpid.h"
#include "absl/strings/string_view.h"
#include "utils/fmt_logging.h"
#include "utils/metrics.h"

namespace dsn {
namespace replication {

/// Base class for types that are one-instance-per-replica.
struct replica_base
{
    replica_base(gpid id, absl::string_view name, absl::string_view app_name);

    explicit replica_base(replica_base *rhs)
        : replica_base(rhs->get_gpid(), rhs->replica_name(), rhs->_app_name)
    {
    }

    gpid get_gpid() const { return _gpid; }

    const char *replica_name() const { return _name.c_str(); }

    const char *app_name() const { return _app_name.c_str(); }

    const char *log_prefix() const { return _name.c_str(); }

    const metric_entity_ptr &replica_metric_entity() const
    {
        CHECK_NOTNULL(_replica_metric_entity,
                      "replica metric entity (table_id={}, partition_id={}) should has been "
                      "instantiated: uninitialized entity cannot be used to instantiate metric",
                      _gpid.get_app_id(),
                      _gpid.get_partition_index());
        return _replica_metric_entity;
    }

private:
    const gpid _gpid;
    const std::string _name;
    // TODO(wangdan): drop `_app_name` or make it changeable, since a table could be renamed.
    const std::string _app_name;
    const metric_entity_ptr _replica_metric_entity;
};

} // namespace replication
} // namespace dsn
