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

#include <memory>

#include "nfs/nfs_node.h"
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
class nfs_client_impl;
class nfs_service_impl;

class nfs_node_simple : public nfs_node
{
public:
    nfs_node_simple();

    virtual ~nfs_node_simple();

    void call(std::shared_ptr<remote_copy_request> rci, aio_task *callback) override;

    error_code start() override;

    error_code stop() override;

    void on_copy(const ::dsn::service::copy_request &request,
                 ::dsn::rpc_replier<::dsn::service::copy_response> &reply) override;

    void
    on_get_file_size(const ::dsn::service::get_file_size_request &request,
                     ::dsn::rpc_replier<::dsn::service::get_file_size_response> &reply) override;

    void register_async_rpc_handler_for_test() override;

private:
    nfs_service_impl *_server;
    nfs_client_impl *_client;
};
} // namespace service
} // namespace dsn
