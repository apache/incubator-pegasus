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
# include <dsn/cpp/service.api.oo.h>
# include <dsn/dist/failure_detector/fd.code.definition.h>
# include <iostream>


namespace dsn { namespace fd { 
class failure_detector_client 
    : public virtual ::dsn::servicelet
{
public:
    failure_detector_client(const dsn_address_t& server) { _server = server; }
    failure_detector_client() { _server = dsn_address_invalid; }
    virtual ~failure_detector_client() {}


    // ---------- call RPC_FD_FAILURE_DETECTOR_PING ------------
    // - synchronous 
    ::dsn::error_code ping(
        const ::dsn::fd::beacon_msg& beacon, 
        __out_param ::dsn::fd::beacon_ack& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        ::dsn::message_ptr resp_msg;
        auto err = ::dsn::rpc::call_typed_wait(
            &resp_msg, p_server_addr ? *p_server_addr : _server,
            RPC_FD_FAILURE_DETECTOR_PING, beacon,
            hash, timeout_milliseconds
            );
        if (err == ::dsn::ERR_OK)
        {
            ::unmarshall(resp_msg.get(), resp);
        }
        return err;
    }
    
    // - asynchronous with on-stack ::dsn::fd::beacon_msg and ::dsn::fd::beacon_ack 
    ::dsn::task_ptr begin_ping(
        const ::dsn::fd::beacon_msg& beacon, 
        void* context,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_FD_FAILURE_DETECTOR_PING, 
                    beacon, 
                    this, 
                    &failure_detector_client::end_ping, 
                    context,
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_ping(
        ::dsn::error_code err, 
        const ::dsn::fd::beacon_ack& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_FD_FAILURE_DETECTOR_PING err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_FD_FAILURE_DETECTOR_PING ok" << std::endl;
        }
    }
    
    // - asynchronous with on-heap std::shared_ptr<::dsn::fd::beacon_msg> and std::shared_ptr<::dsn::fd::beacon_ack> 
    ::dsn::task_ptr begin_ping2(
        std::shared_ptr<::dsn::fd::beacon_msg>& beacon,         
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_FD_FAILURE_DETECTOR_PING, 
                    beacon, 
                    this, 
                    &failure_detector_client::end_ping2, 
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_ping2(
        ::dsn::error_code err, 
        std::shared_ptr<::dsn::fd::beacon_msg>& beacon, 
        std::shared_ptr<::dsn::fd::beacon_ack>& resp)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_FD_FAILURE_DETECTOR_PING err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_FD_FAILURE_DETECTOR_PING ok" << std::endl;
        }
    }
    

private:
    dsn_address_t _server;
};

} } 