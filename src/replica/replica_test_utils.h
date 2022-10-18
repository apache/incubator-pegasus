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

/// This file contains utilities for upper level applications (pegasus) which
/// needs the hidden abstraction of rDSN in order to make unit test.

#include "common/serialization_helper/dsn.layer2_types.h"

namespace dsn {
namespace replication {

class replica;
class replica_stub;

extern replica *create_test_replica(replica_stub *stub,
                                    gpid gpid,
                                    const app_info &app,
                                    const char *dir,
                                    bool restore_if_necessary,
                                    bool is_duplication_follower);

extern replica_stub *create_test_replica_stub();

extern void destroy_replica(replica *r);

extern void destroy_replica_stub(replica_stub *rs);

} // namespace replication
} // namespace dsn
