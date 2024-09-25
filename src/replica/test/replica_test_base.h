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

#include <gtest/gtest.h>

#include "mock_utils.h"
#include "replica/replication_app_base.h"
#include "replica/replica_stub.h"
#include "test_util/test_util.h"
#include "utils/errors.h"
#include "utils/filesystem.h"

namespace dsn {
namespace replication {

class replica_stub_test_base : public pegasus::encrypt_data_test_base
{
public:
    replica_stub_test_base() { stub = std::make_unique<mock_replica_stub>(); }

    ~replica_stub_test_base() { stub.reset(); }

    std::unique_ptr<mock_replica_stub> stub;
};

class replica_test_base : public replica_stub_test_base
{
public:
    std::unique_ptr<mock_replica> _replica;
    // TODO(yingchun): rename to _replica_dir, and consider to remove it totally.
    std::string _log_dir;

    replica_test_base()
    {
        _replica = create_mock_replica(stub.get(), 1, 1);
        _log_dir = _replica->dir();
    }

    virtual mutation_ptr
    create_test_mutation(int64_t decree, int64_t last_committed_decree, const char *data)
    {
        mutation_ptr mu(new mutation());
        mu->data.header.ballot = 1;
        mu->data.header.decree = decree;
        mu->data.header.pid = _replica->get_gpid();
        mu->data.header.last_committed_decree = last_committed_decree;
        mu->data.header.log_offset = 0;
        mu->data.header.timestamp = decree;

        mu->data.updates.emplace_back(mutation_update());
        mu->data.updates.back().code =
            RPC_COLD_BACKUP; // whatever code it is, but never be WRITE_EMPTY
        if (data != nullptr) {
            mu->data.updates.back().data = blob::create_from_bytes(data);
        }
        mu->client_requests.push_back(nullptr);

        // replica_duplicator always loads from hard disk,
        // so it must be logged.
        mu->set_logged();

        return mu;
    }

    virtual mutation_ptr create_test_mutation(int64_t decree, const char *data)
    {
        return replica_test_base::create_test_mutation(decree, decree - 1, data);
    }

    gpid get_gpid() const { return _replica->get_gpid(); }

    void set_last_applied_decree(decree d) { _replica->set_app_last_committed_decree(d); }
};

} // namespace replication
} // namespace dsn
