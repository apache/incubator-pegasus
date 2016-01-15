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
# pragma once
# include "nfs_code_definition.h"
# include <iostream>


namespace dsn { namespace service { 
class nfs_client 
    : public virtual ::dsn::clientlet
{
public:
    nfs_client(::dsn::rpc_address server) { _server = server; }
    nfs_client() {  }
    virtual ~nfs_client() {}


    // ---------- call RPC_NFS_COPY ------------
    // - synchronous 
    ::dsn::error_code copy(
        const copy_request& request, 
        /*out*/ copy_response& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        ::dsn::rpc_read_stream response;
        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            RPC_NFS_COPY, request, hash, timeout_milliseconds);
        if (err == ::dsn::ERR_OK)
        {
            unmarshall(response, resp);
        }
        return err;
    }
    
    // - asynchronous with on-stack copy_request and copy_response 
    ::dsn::task_ptr begin_copy(
        const copy_request& request, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_NFS_COPY, 
                    request, 
                    this,
                    [=](error_code err, copy_response&& resp)
                    {
                        end_copy(err, std::move(resp), context);
                    },
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_copy(
        ::dsn::error_code err, 
        const copy_response& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_NFS_COPY err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_NFS_COPY ok" << std::endl;
        }
    }

    // ---------- call RPC_NFS_GET_FILE_SIZE ------------
    // - synchronous 
    ::dsn::error_code get_file_size(
        const get_file_size_request& request, 
        /*out*/ get_file_size_response& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        ::dsn::rpc_read_stream response;
        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            RPC_NFS_GET_FILE_SIZE, request, hash, timeout_milliseconds);
        if (err == ::dsn::ERR_OK)
        {
            unmarshall(response, resp);
        }
        return err;
    }
    
    // - asynchronous with on-stack get_file_size_request and get_file_size_response 
    ::dsn::task_ptr begin_get_file_size(
        const get_file_size_request& request, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_NFS_GET_FILE_SIZE, 
                    request, 
                    this, 
                    [=](error_code err, get_file_size_response&& resp)
                    {
                        end_get_file_size(err, std::move(resp), context);
                    },
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_get_file_size(
        ::dsn::error_code err, 
        const get_file_size_response& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_NFS_GET_FILE_SIZE err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_NFS_GET_FILE_SIZE ok" << std::endl;
        }
    }
    
private:
    ::dsn::rpc_address _server;
};

} } 