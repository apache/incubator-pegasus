// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/service_api_cpp.h>
#include <dsn/tool-api/zlocks.h>
#include <unordered_map>
#include <functional>

namespace pegasus {
namespace proxy {

DEFINE_TASK_CODE_RPC(RPC_CALL_RAW_SESSION_DISCONNECT,
                     TASK_PRIORITY_COMMON,
                     ::dsn::THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE_RPC(RPC_CALL_RAW_MESSAGE, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

class proxy_stub;
class proxy_session : public std::enable_shared_from_this<proxy_session>
{
public:
    typedef std::function<std::shared_ptr<proxy_session>(proxy_stub *p, dsn::message_ex *first_msg)>
        factory;
    proxy_session(proxy_stub *p, dsn::message_ex *first_msg);
    virtual ~proxy_session();

    // on_recv_request & on_remove_session are called by proxy_stub when messages are got from
    // underlying rpc engine.
    //
    // then rpc engine ensures that on_recv_request for one proxy_session
    // won't be called concurrently. that is to say: another on_recv_requst
    // may happen only after the first one returns
    //
    // however, during the running of on_recv_request, an "on_remove_session" may be called,
    // the proxy_session and its derived class may need to do some synchronization on this.
    void on_recv_request(dsn::message_ex *msg);
    void on_remove_session();

protected:
    // return if parse ok
    virtual bool parse(dsn::message_ex *msg) = 0;
    dsn::message_ex *create_response();

protected:
    proxy_stub *stub;
    std::atomic_bool is_session_reset;

    // when get message from raw parser, request & response of "dsn::message_ex*" are not in couple.
    // we need to backup one request to create a response struct.
    dsn::message_ex *backup_one_request;
    // the client address for which this session served
    dsn::rpc_address remote_address;
};

class proxy_stub : public ::dsn::serverlet<proxy_stub>
{
public:
    proxy_stub(const proxy_session::factory &f,
               const char *cluster,
               const char *app,
               const char *geo_app = "");
    const char *get_cluster() const { return _cluster.c_str(); }
    const char *get_app() const { return _app.c_str(); }
    const char *get_geo_app() const { return _geo_app.c_str(); }
    void open_service()
    {
        this->register_rpc_handler(
            RPC_CALL_RAW_MESSAGE, "raw_message", &proxy_stub::on_rpc_request);
        this->register_rpc_handler(RPC_CALL_RAW_SESSION_DISCONNECT,
                                   "raw_session_disconnect",
                                   &proxy_stub::on_recv_remove_session_request);
    }
    void close_service()
    {
        this->unregister_rpc_handler(RPC_CALL_RAW_MESSAGE);
        this->unregister_rpc_handler(RPC_CALL_RAW_SESSION_DISCONNECT);
    }
    std::shared_ptr<proxy_session> remove_session(dsn::rpc_address remote_address);

private:
    void on_rpc_request(dsn::message_ex *request);
    void on_recv_remove_session_request(dsn::message_ex *);

    ::dsn::zrwlock_nr _lock;
    std::unordered_map<::dsn::rpc_address, std::shared_ptr<proxy_session>> _sessions;
    proxy_session::factory _factory;
    ::dsn::rpc_address _uri_address;
    std::string _cluster;
    std::string _app;
    std::string _geo_app;
};
} // namespace proxy
} // namespace pegasus
