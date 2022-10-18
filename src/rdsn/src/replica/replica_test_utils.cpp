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

#include "replica/replica_test_utils.h"

#include "replica.h"
#include "replica_stub.h"

namespace dsn {
namespace replication {

class mock_replica : public replica
{
public:
    mock_replica(replica_stub *stub,
                 const gpid &gpid,
                 const app_info &app,
                 const char *dir,
                 bool restore_if_necessary,
                 bool is_duplication_follower)
        : replica(stub, gpid, app, dir, restore_if_necessary, is_duplication_follower)
    {
    }
};

replica *create_test_replica(replica_stub *stub,
                             gpid gpid,
                             const app_info &app,
                             const char *dir,
                             bool restore_if_necessary,
                             bool is_duplication_follower)
{
    return new mock_replica(stub, gpid, app, dir, restore_if_necessary, is_duplication_follower);
}

replica_stub *create_test_replica_stub() { return new replica_stub(); }

void destroy_replica(replica *r) { delete r; }

void destroy_replica_stub(replica_stub *rs) { delete rs; }

} // namespace replication
} // namespace dsn
