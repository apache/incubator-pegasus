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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */


# include "replication_common.h"
# include "replica_stub.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.service_app"

namespace dsn { namespace replication {

replication_service_app::replication_service_app()
    : layer2_handler()
{
    _stub = new replica_stub();
}

replication_service_app::~replication_service_app(void)
{
}

error_code replication_service_app::start(int argc, char** argv)
{
    replication_options opts;
    opts.initialize();

    _stub->initialize(opts);
    _stub->open_service();

    return ERR_OK;
}

error_code replication_service_app::stop(bool cleanup)
{
    if (_stub != nullptr)
    {
        _stub->close();
        _stub = nullptr;
    }

    return ERR_OK;
}

void replication_service_app::on_request(dsn_gpid dpid, bool is_write, dsn_message_t msg, int delay_ms)
{
    global_partition_id gpid;
    gpid.app_id = dpid.u.app_id;
    gpid.pidx = dpid.u.partition_index;

    if (is_write)
    {
        tasking::enqueue(
            LPC_REPLICATION_CLIENT_WRITE,
            nullptr,
            [=]() {_stub->on_client_write(gpid, msg); },
            gpid_to_hash(gpid)
            );
    }
    else
    {
        tasking::enqueue(
            LPC_REPLICATION_CLIENT_READ,
            nullptr,
            [=]() {_stub->on_client_read(gpid, msg); },
            gpid_to_hash(gpid)
            );
    }
}

}}
