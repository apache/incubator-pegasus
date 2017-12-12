// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/tool-api/uri_address.h>
#include "proxy_layer.h"

namespace pegasus {
namespace proxy {

proxy_stub::proxy_stub(const proxy_session::factory &factory, const char *uri)
    : serverlet<proxy_stub>("proxy_stub"), _factory(factory)
{
    _uri_address.assign_uri(uri);
    open_service();
}

proxy_stub::~proxy_stub() {}

void proxy_stub::on_rpc_request(dsn_message_t request)
{
    ::dsn::rpc_address source = dsn_msg_from_address(request);
    std::shared_ptr<proxy_session> ps;
    {
        ::dsn::service::zauto_read_lock l(_lock);
        auto it = _sessions.find(source);
        if (it != _sessions.end()) {
            ps = it->second;
        }
    }
    if (nullptr == ps) {
        ::dsn::service::zauto_write_lock l(_lock);
        auto it = _sessions.find(source);
        if (it != _sessions.end()) {
            ps = it->second;
        } else {
            ps = _factory(this, source);
            _sessions.emplace(source, ps);
        }
    }

    // release in proxy_session
    dsn_msg_add_ref(request);
    /*
    tasking::enqueue(LPC_RPC_CALL_RAW_SCATTER, nullptr,
                     std::bind(&proxy_session::on_recv_request, ps.get(), ps, request),
                     ps->hash());
    */
    ps->on_recv_request(ps, request);
}

void proxy_stub::on_recv_remove_session_request(dsn_message_t request)
{
    ::dsn::rpc_address source = dsn_msg_from_address(request);
    std::shared_ptr<proxy_session> ps;
    {
        ::dsn::service::zauto_write_lock l(_lock);
        auto iter = _sessions.find(source);
        if (iter == _sessions.end())
            return;
        ps = iter->second;
        _sessions.erase(iter);
    }
    ddebug("proxy session %s removed", source.to_string());
    ::dsn::tasking::enqueue(LPC_RPC_CALL_RAW_SCATTER,
                            nullptr,
                            std::bind(&proxy_session::on_remove_session, ps.get(), ps),
                            ps->hash());
}

proxy_session::proxy_session(proxy_stub *op, ::dsn::rpc_address raddr)
    : stub(op),
      backup_one_request(nullptr),
      remote_address(raddr),
      hash_code(std::hash<::dsn::rpc_address>()(remote_address))
{
    dassert(
        raddr.type() == HOST_TYPE_IPV4, "invalid rpc_address type, type = %d", (int)raddr.type());
}

proxy_session::~proxy_session()
{
    if (backup_one_request) {
        dsn_msg_release_ref(backup_one_request);
    }
    dinfo("proxy session %s destroyed", remote_address.to_string());
}

void proxy_session::on_recv_request(std::shared_ptr<proxy_session> _this, dsn_message_t msg)
{
    ::dsn::service::zauto_lock l(_lock);
    if (backup_one_request == nullptr) {
        backup_one_request = msg;
        dsn_msg_add_ref(msg); // release in service session's dtor
    }

    if (!parse(msg)) {
        ::dsn::rpc_address from = dsn_msg_from_address(msg);
        derror("got invalid message from remote address %s", from.to_string());

        // TODO: notify rpc engine to reset the socket, currently let's take the easiest way :)
        dassert(false, "got invalid message");
    }
}

dsn_message_t proxy_session::create_response()
{
    if (backup_one_request == nullptr)
        return nullptr;
    return dsn_msg_create_response(backup_one_request);
}
}
} // namespace
