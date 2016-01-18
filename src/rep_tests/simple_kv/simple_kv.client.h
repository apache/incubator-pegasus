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
# pragma once
# include <dsn/dist/replication.h>
# include "simple_kv.code.definition.h"
# include "common.h"
# include <iostream>

namespace dsn { namespace replication { namespace test { 

class simple_kv_client 
    : public ::dsn::replication::replication_app_client_base
{
public:
    simple_kv_client(
        const std::vector<rpc_address>& meta_servers,
        const char* app_name)
        : ::dsn::replication::replication_app_client_base(meta_servers, app_name) 
    {
    }
    
    virtual ~simple_kv_client() {}
    
    // from requests to partition index
    // PLEASE DO RE-DEFINE THEM IN A SUB CLASS!!!
    virtual uint64_t get_key_hash(const std::string& key)
    {
        return dsn_crc64_compute(key.c_str(), key.size(), 0);
    }
    virtual uint64_t get_key_hash(const ::dsn::replication::test::kv_pair& key)
    {
        return dsn_crc64_compute(key.key.c_str(), key.key.size(), 0);
    }

    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_READ ------------
    // - synchronous 
    ::dsn::error_code read(
        const std::string& key, 
        /*out*/ std::string& resp, 
        int timeout_milliseconds = 0
        )
    {
        auto resp_task = ::dsn::replication::replication_app_client_base::read(
            get_key_hash(key),
            RPC_SIMPLE_KV_SIMPLE_KV_READ,
            key,
            nullptr,
            timeout_milliseconds,
            0,
            read_semantic_t::ReadLastUpdate
            );
        resp_task->wait();
        if (resp_task->error() == ::dsn::ERR_OK)
        {
            ::unmarshall(resp_task->response(), resp);
        }
        return resp_task->error();
    }
    
    // - asynchronous with on-stack std::string and std::string 
    ::dsn::task_ptr begin_read(
        const std::string& key,         
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0
        )
    {
        return ::dsn::replication::replication_app_client_base::read(
            get_key_hash(key),
            RPC_SIMPLE_KV_SIMPLE_KV_READ, 
            key,
            this,
            [=](error_code err, std::string&& resp)
            {
                end_read(err, resp, context);
            },
            timeout_milliseconds,
            reply_hash,
            read_semantic_t::ReadLastUpdate
            );
    }

    virtual void end_read(
        ::dsn::error_code err, 
        const std::string& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_SIMPLE_KV_SIMPLE_KV_READ err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_SIMPLE_KV_SIMPLE_KV_READ ok" << std::endl;
        }
    }
    

    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_WRITE ------------
    // - synchronous 
    ::dsn::error_code write(
        const ::dsn::replication::test::kv_pair& pr, 
        /*out*/ int32_t& resp, 
        int timeout_milliseconds = 0
        )
    {
        auto resp_task = ::dsn::replication::replication_app_client_base::write(
            get_key_hash(pr),
            RPC_SIMPLE_KV_SIMPLE_KV_WRITE,
            pr,
            nullptr,
            timeout_milliseconds
            );
        resp_task->wait();
        if (resp_task->error() == ::dsn::ERR_OK)
        {
            ::unmarshall(resp_task->response(), resp);
        }
        return resp_task->error();
    }
    
    // - asynchronous with on-stack ::dsn::replication::test::kv_pair and int32_t 
    ::dsn::task_ptr begin_write(
        const ::dsn::replication::test::kv_pair& pr,     
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0
        )
    {
        return ::dsn::replication::replication_app_client_base::write(
            get_key_hash(pr),
            RPC_SIMPLE_KV_SIMPLE_KV_WRITE, 
            pr,
            this,
            [=](error_code err, int32_t resp)
            {
                end_write(err, resp, context);
            },
            timeout_milliseconds,
            reply_hash
            );
    }

    virtual void end_write(
        ::dsn::error_code err, 
        const int32_t& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_SIMPLE_KV_SIMPLE_KV_WRITE err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_SIMPLE_KV_SIMPLE_KV_WRITE ok" << std::endl;
        }
    }
    
    

    // ---------- call RPC_SIMPLE_KV_SIMPLE_KV_APPEND ------------
    // - synchronous 
    ::dsn::error_code append(
        const ::dsn::replication::test::kv_pair& pr, 
        /*out*/ int32_t& resp, 
        int timeout_milliseconds = 0
        )
    {
        auto resp_task = ::dsn::replication::replication_app_client_base::write(
            get_key_hash(pr),
            RPC_SIMPLE_KV_SIMPLE_KV_APPEND,
            pr,
            nullptr,
            timeout_milliseconds
            );
        resp_task->wait();
        if (resp_task->error() == ::dsn::ERR_OK)
        {
            ::unmarshall(resp_task->response(), resp);
        }
        return resp_task->error();
    }
    
    // - asynchronous with on-stack ::dsn::replication::test::kv_pair and int32_t 
    ::dsn::task_ptr begin_append(
        const ::dsn::replication::test::kv_pair& pr,         
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0
        )
    {
        return ::dsn::replication::replication_app_client_base::write(
            get_key_hash(pr),
            RPC_SIMPLE_KV_SIMPLE_KV_APPEND, 
            pr,
            this,
            [=](error_code err, int32_t resp)
            {
                end_append(err, resp, context);
            },
            timeout_milliseconds,
            reply_hash
            );
    }

    virtual void end_append(
        ::dsn::error_code err, 
        const int32_t& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_SIMPLE_KV_SIMPLE_KV_APPEND err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_SIMPLE_KV_SIMPLE_KV_APPEND ok" << std::endl;
        }
    }
    
    void send_config_to_meta(const rpc_address& receiver, dsn::replication::config_type type, const rpc_address& node)
    {
        dsn::rpc_address meta_servers = replication_app_client_base::get_meta_servers();
        dsn_message_t request = dsn_msg_create_request(RPC_CM_MODIFY_REPLICA_CONFIG_COMMAND, 30000);

        ::marshall(request, g_default_gpid);
        ::marshall(request, receiver);
        ::marshall(request, type);
        ::marshall(request, node);

        dsn_rpc_call_one_way(meta_servers.c_addr(), request);
    }
};

}}} 

