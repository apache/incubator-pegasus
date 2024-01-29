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

#include <memory>

#include "nfs/nfs_node.h"
#include "nfs_client_impl.h"
#include "nfs_node_simple.h"
#include "nfs_server_impl.h"
#include "utils/error_code.h"

namespace dsn {
class aio_task;
template <typename TResponse>
class rpc_replier;

namespace service {
class copy_request;
class copy_response;
class get_file_size_request;
class get_file_size_response;

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

    _client = new nfs_client_impl();
    return ERR_OK;
}

void nfs_node_simple::register_async_rpc_handler_for_test()
{
    _server->open_nfs_service_for_test();
}

error_code nfs_node_simple::stop()
{
    if (_server != nullptr) {
        _server->close_service();

        delete _server;
        _server = nullptr;
    }

    if (_client != nullptr) {
        delete _client;
        _client = nullptr;
    }
    return ERR_OK;
}

void nfs_node_simple::on_copy(const copy_request &request, ::dsn::rpc_replier<copy_response> &reply)
{
    _server->on_copy(request, reply);
}

void nfs_node_simple::on_get_file_size(
    const ::dsn::service::get_file_size_request &request,
    ::dsn::rpc_replier<::dsn::service::get_file_size_response> &reply)
{
    _server->on_get_file_size(request, reply);
}

} // namespace service
} // namespace dsn
