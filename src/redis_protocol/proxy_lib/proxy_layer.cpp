/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "runtime/task/task_spec.h"

#include <rrdb/rrdb.code.definition.h>
#include "proxy_layer.h"
#include "utils/fmt_logging.h"

namespace pegasus {
namespace proxy {

proxy_stub::proxy_stub(const proxy_session::factory &f,
                       const char *cluster,
                       const char *app,
                       const char *geo_app)
    : serverlet<proxy_stub>("proxy_stub"),
      _factory(f),
      _cluster(cluster),
      _app(app),
      _geo_app(geo_app)
{
    dsn::task_spec::get(RPC_CALL_RAW_MESSAGE)->allow_inline = true;
    dsn::task_spec::get(RPC_CALL_RAW_SESSION_DISCONNECT)->allow_inline = true;

    dsn::task_spec::get(dsn::apps::RPC_RRDB_RRDB_PUT_ACK)->allow_inline = true;
    dsn::task_spec::get(dsn::apps::RPC_RRDB_RRDB_MULTI_PUT_ACK)->allow_inline = true;
    dsn::task_spec::get(dsn::apps::RPC_RRDB_RRDB_REMOVE_ACK)->allow_inline = true;
    dsn::task_spec::get(dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE_ACK)->allow_inline = true;
    dsn::task_spec::get(dsn::apps::RPC_RRDB_RRDB_GET_ACK)->allow_inline = true;
    dsn::task_spec::get(dsn::apps::RPC_RRDB_RRDB_MULTI_GET_ACK)->allow_inline = true;
    dsn::task_spec::get(dsn::apps::RPC_RRDB_RRDB_SORTKEY_COUNT_ACK)->allow_inline = true;
    dsn::task_spec::get(dsn::apps::RPC_RRDB_RRDB_TTL_ACK)->allow_inline = true;
    dsn::task_spec::get(dsn::apps::RPC_RRDB_RRDB_GET_SCANNER_ACK)->allow_inline = true;
    dsn::task_spec::get(dsn::apps::RPC_RRDB_RRDB_SCAN_ACK)->allow_inline = true;
    dsn::task_spec::get(dsn::apps::RPC_RRDB_RRDB_CLEAR_SCANNER_ACK)->allow_inline = true;
    dsn::task_spec::get(dsn::apps::RPC_RRDB_RRDB_INCR_ACK)->allow_inline = true;

    open_service();
}

void proxy_stub::on_rpc_request(dsn::message_ex *request)
{
    ::dsn::rpc_address source = request->header->from_address;
    std::shared_ptr<proxy_session> session;
    {
        ::dsn::zauto_read_lock l(_lock);
        auto it = _sessions.find(source);
        if (it != _sessions.end()) {
            session = it->second;
        }
    }
    if (nullptr == session) {
        ::dsn::zauto_write_lock l(_lock);
        auto it = _sessions.find(source);
        if (it != _sessions.end()) {
            session = it->second;
        } else {
            session = _factory(this, request);
            _sessions.emplace(source, session);
        }
    }

    session->on_recv_request(request);
}

void proxy_stub::on_recv_remove_session_request(dsn::message_ex *request)
{
    ::dsn::rpc_address source = request->header->from_address;
    remove_session(source);
}

void proxy_stub::remove_session(dsn::rpc_address remote_address)
{
    std::shared_ptr<proxy_session> session;
    {
        ::dsn::zauto_write_lock l(_lock);
        auto iter = _sessions.find(remote_address);
        if (iter == _sessions.end()) {
            LOG_WARNING("{} has been removed from proxy stub", remote_address);
            return;
        }
        LOG_INFO("remove {} from proxy stub", remote_address);
        session = std::move(iter->second);
        _sessions.erase(iter);
    }
    session->on_remove_session();
}

proxy_session::proxy_session(proxy_stub *op, dsn::message_ex *first_msg)
    : _stub(op), _is_session_reset(false), _backup_one_request(first_msg)
{
    CHECK_NOTNULL(first_msg, "null msg when create session");
    _backup_one_request->add_ref();

    _remote_address = _backup_one_request->header->from_address;
    CHECK_EQ_MSG(_remote_address.type(), HOST_TYPE_IPV4, "invalid rpc_address type");
}

proxy_session::~proxy_session()
{
    _backup_one_request->release_ref();
    LOG_INFO("proxy session {} destroyed", _remote_address);
}

void proxy_session::on_recv_request(dsn::message_ex *msg)
{
    // NOTICE:
    // 1. in the implementation of "parse", the msg may add_ref & release_ref.
    //    so if the ref_count of msg is 0 before call "parse", the msg may be released already
    //    after "parse" returns. so please take care when you want to
    //    use "msg" after call "parse"
    //
    // 2. as "on_recv_request" won't be called concurrently, it's not necessary to call
    //    "parse" with a lock. a subclass may implement a lock inside parse if necessary
    if (!parse(msg)) {
        LOG_ERROR("{}: got invalid message, try to remove proxy session from proxy stub",
                  _remote_address);
        _stub->remove_session(_remote_address);

        LOG_ERROR("close the rpc session {}", _remote_address);
        ((dsn::message_ex *)_backup_one_request)->io_session->close();
    }
}

void proxy_session::on_remove_session() { _is_session_reset.store(true); }

dsn::message_ex *proxy_session::create_response() { return _backup_one_request->create_response(); }
} // namespace proxy
} // namespace pegasus
