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

#include "nfs_node_simple.h"
#include "nfs_client_impl.h"
#include "nfs_server_impl.h"

namespace dsn {
namespace service {

nfs_node_simple::nfs_node_simple() : nfs_node()
{
    _server = nullptr;
    _client = nullptr;
}

nfs_node_simple::~nfs_node_simple() { stop(); }

void nfs_node_simple::call(std::shared_ptr<remote_copy_request> rci, aio_task *callback)
{
    _client->begin_remote_copy(rci, callback); // copy file request entry
}

error_code nfs_node_simple::start()
{
    _server = new nfs_service_impl();
    _server->open_service();

    _client = new nfs_client_impl();
    return ERR_OK;
}

error_code nfs_node_simple::stop()
{
    delete _server;
    _server = nullptr;

    delete _client;
    _client = nullptr;

    return ERR_OK;
}
} // namespace service
} // namespace dsn
