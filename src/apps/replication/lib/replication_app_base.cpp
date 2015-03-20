/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "replica.h"
#include "mutation.h"
#include "replication_app_base.h"

#define __TITLE__ "TwoPhaseCommit"

namespace dsn { namespace replication {

replication_app_base::replication_app_base(replica* replica, const replication_app_config* config)
{
    _dir = replica->dir();
    _replica = replica;
}

int replication_app_base::WriteInternal(mutation_ptr& mu, bool ackClient)
{
    dassert (mu->data.header.decree == last_committed_decree() + 1, "");

    int err = write(mu->client_requests, mu->data.header.decree, ackClient);

    //dassert (mu->data.header.decree == last_committed_decree(), "");

    return err;
}

void replication_app_base::WriteReplicationResponse(message_ptr& response)
{
    int err = ERR_SUCCESS;
    response->writer().write(err);
}

}} // end namespace
