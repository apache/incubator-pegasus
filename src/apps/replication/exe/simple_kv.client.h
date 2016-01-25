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
# include <dsn/dist/replication.h>
# include "simple_kv.code.definition.h"
# include <iostream>

namespace dsn { namespace replication { namespace application { 
class simple_kv_client 
    : public ::dsn::replication::replication_app_client_base
{
public:
    using replication_app_client_base::replication_app_client_base;
    virtual ~simple_kv_client() {}
    
    // from requests to partition index
    // PLEASE DO RE-DEFINE THEM IN A SUB CLASS!!!
    virtual uint64_t get_key_hash(const std::string& key) = 0;
    virtual uint64_t get_key_hash(const kv_pair& key) = 0;
    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_READ ------------
    // - synchronous 
    std::pair<::dsn::error_code, std::string> read_sync(
        const std::string& key, 
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0), 
        ::dsn::replication::read_semantic_t read_semantic = ::dsn::replication::read_semantic_t::ReadLastUpdate 
        )
    {
        return dsn::rpc::wait_and_unwrap<std::string>(
            ::dsn::replication::replication_app_client_base::read(
                get_key_hash(key),
                RPC_SIMPLE_KV_SIMPLE_KV_READ,
                key,
                this,
                empty_callback,
                timeout,
                0, 
                read_semantic 
                )
            );
    }
    
    // - asynchronous with on-stack std::string and std::string 
    template<typename TCallback>
    ::dsn::task_ptr read(
        const std::string& key,
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0,  
        ::dsn::replication::read_semantic_t read_semantic = ::dsn::replication::read_semantic_t::ReadLastUpdate 
        )
    {
        return ::dsn::replication::replication_app_client_base::read(
            get_key_hash(key),
            RPC_SIMPLE_KV_SIMPLE_KV_READ, 
            key,
            this,
            std::forward<TCallback>(callback),
            timeout,
            reply_hash, 
            read_semantic 
            );
    }    
    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_WRITE ------------
    // - synchronous 
    std::pair<::dsn::error_code, int32_t> write_sync(
        const kv_pair& pr, 
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0) 
        )
    {
        return dsn::rpc::wait_and_unwrap<int32_t>(
            ::dsn::replication::replication_app_client_base::write(
                get_key_hash(pr),
                RPC_SIMPLE_KV_SIMPLE_KV_WRITE,
                pr,
                this,
                empty_callback,
                timeout,
                0 
                )
            );
    }
    
    // - asynchronous with on-stack kv_pair and int32_t 
    template<typename TCallback>
    ::dsn::task_ptr write(
        const kv_pair& pr,
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0 
        )
    {
        return ::dsn::replication::replication_app_client_base::write(
            get_key_hash(pr),
            RPC_SIMPLE_KV_SIMPLE_KV_WRITE, 
            pr,
            this,
            std::forward<TCallback>(callback),
            timeout,
            reply_hash 
            );
    }    
    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_APPEND ------------
    // - synchronous 
    std::pair<::dsn::error_code, int32_t> append_sync(
        const kv_pair& pr, 
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0) 
        )
    {
        return dsn::rpc::wait_and_unwrap<int32_t>(
            ::dsn::replication::replication_app_client_base::write(
                get_key_hash(pr),
                RPC_SIMPLE_KV_SIMPLE_KV_APPEND,
                pr,
                this,
                empty_callback,
                timeout,
                0 
                )
            );
    }
    
    // - asynchronous with on-stack kv_pair and int32_t 
    template<typename TCallback>
    ::dsn::task_ptr append(
        const kv_pair& pr,
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0 
        )
    {
        return ::dsn::replication::replication_app_client_base::write(
            get_key_hash(pr),
            RPC_SIMPLE_KV_SIMPLE_KV_APPEND, 
            pr,
            this,
            std::forward<TCallback>(callback),
            timeout,
            reply_hash 
            );
    }    
};

} } } 