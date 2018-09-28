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
#include <iostream>
#include <dsn/cpp/serverlet.h>
#include "nfs_code_definition.h"
#include "nfs_types.h"

namespace dsn {
namespace service {
class nfs_service : public ::dsn::serverlet<nfs_service>
{
public:
    nfs_service() : ::dsn::serverlet<nfs_service>("nfs") {}
    virtual ~nfs_service() {}

protected:
    // all service handlers to be implemented further
    // RPC_NFS_NFS_COPY
    virtual void on_copy(const copy_request &request, ::dsn::rpc_replier<copy_response> &reply)
    {
        std::cout << "... exec RPC_NFS_NFS_COPY ... (not implemented) " << std::endl;
        copy_response resp;
        reply(resp);
    }
    // RPC_NFS_NFS_GET_FILE_SIZE
    virtual void on_get_file_size(const get_file_size_request &request,
                                  ::dsn::rpc_replier<get_file_size_response> &reply)
    {
        std::cout << "... exec RPC_NFS_NFS_GET_FILE_SIZE ... (not implemented) " << std::endl;
        get_file_size_response resp;
        reply(resp);
    }

public:
    void open_service()
    {
        this->register_async_rpc_handler(RPC_NFS_COPY, "copy", &nfs_service::on_copy);
        this->register_async_rpc_handler(
            RPC_NFS_GET_FILE_SIZE, "get_file_size", &nfs_service::on_get_file_size);
    }

    void close_service()
    {
        this->unregister_rpc_handler(RPC_NFS_COPY);
        this->unregister_rpc_handler(RPC_NFS_GET_FILE_SIZE);
    }
};
}
}
