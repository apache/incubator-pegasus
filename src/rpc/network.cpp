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

#include "network.h"

#include <errno.h>
#include <unistd.h>
#include <chrono>
#include <list>
#include <string_view>
#include <utility>

#include "message_parser_manager.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_engine.h"
#include "runtime/api_task.h"
#include "task/task.h"
#include "task/task_code.h"
#include "utils/blob.h"
#include "utils/customizable_id.h"
#include "utils/errors.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/safe_strerror_posix.h"
#include "utils/strings.h"
#include "utils/threadpool_code.h"

METRIC_DEFINE_gauge_int64(server,
                          network_client_sessions,
                          dsn::metric_unit::kSessions,
                          "The number of sessions from client side");

METRIC_DEFINE_gauge_int64(server,
                          network_server_sessions,
                          dsn::metric_unit::kSessions,
                          "The number of sessions from server side");

DSN_DEFINE_uint32(network,
                  conn_threshold_per_ip,
                  0,
                  "The maximum connection count to each server per IP address, 0 means no limit");
DSN_DEFINE_string(network, unknown_message_header_format, "", "format for unknown message headers");
DSN_DEFINE_string(network,
                  explicit_host_address,
                  "",
                  "explicit host name or ip (v4) assigned to this node (e.g., "
                  "service ip for pods in kubernets)");
DSN_DEFINE_string(network,
                  primary_interface,
                  "",
                  "network interface name used to init primary ipv4 address, "
                  "if empty, means using a site local address");

namespace dsn {
/*static*/ join_point<void, rpc_session *>
    rpc_session::on_rpc_session_connected("rpc.session.connected");
/*static*/ join_point<void, rpc_session *>
    rpc_session::on_rpc_session_disconnected("rpc.session.disconnected");
/*static*/ join_point<bool, message_ex *>
    rpc_session::on_rpc_recv_message("rpc.session.recv.message");
/*static*/ join_point<bool, message_ex *>
    rpc_session::on_rpc_send_message("rpc.session.send.message");

rpc_session::~rpc_session()
{
    clear_pending_messages();
    clear_send_queue(false);

    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        CHECK_EQ_MSG(0, _sending_msgs.size(), "sending queue is not cleared yet");
        CHECK_EQ_MSG(0, _message_count, "sending queue is not cleared yet");
    }
}

bool rpc_session::set_connecting()
{
    CHECK(is_client(), "must be client session");

    utils::auto_lock<utils::ex_lock_nr> l(_lock);
    if (_connect_state == SS_DISCONNECTED) {
        _connect_state = SS_CONNECTING;
        return true;
    } else {
        return false;
    }
}

void rpc_session::set_connected()
{
    CHECK(is_client(), "must be client session");

    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        CHECK_EQ(_connect_state, SS_CONNECTING);
        _connect_state = SS_CONNECTED;
    }

    rpc_session_ptr sp = this;
    _net.on_client_session_connected(sp);

    on_rpc_session_connected.execute(this);
}

bool rpc_session::set_disconnected()
{
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        if (_connect_state != SS_DISCONNECTED) {
            _connect_state = SS_DISCONNECTED;
        } else {
            return false;
        }
    }

    on_rpc_session_disconnected.execute(this);
    return true;
}

void rpc_session::clear_send_queue(bool resend_msgs)
{
    //
    // - in concurrent case, resending _sending_msgs and _messages
    //   may not maintain the original sending order
    // - can optimize by batch sending instead of sending one by one
    //
    // however, our threading model cannot ensure in-order processing
    // of incoming messages neither, so this guarantee is not necesssary
    // and the upper applications should not always rely on this (but can
    // rely on this with a high probability).
    //

    std::vector<message_ex *> swapped_sending_msgs;
    {
        // protect _sending_msgs and _sending_buffers in lock
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        _sending_msgs.swap(swapped_sending_msgs);
        _sending_buffers.clear();
    }

    // resend pending messages if need
    for (auto &msg : swapped_sending_msgs) {
        if (resend_msgs) {
            _net.send_message(msg);
        }

        // if not resend, the message's callback will not be invoked until timeout,
        // it's too slow - let's try to mimic the failure by recving an empty reply
        else if (msg->header->context.u.is_request && !msg->header->context.u.is_forwarded) {
            _net.on_recv_reply(msg->header->id, nullptr, 0);
        }

        // added in rpc_engine::reply (for server) or rpc_session::send_message (for client)
        msg->release_ref();
    }

    while (true) {
        dlink *msg;
        {
            utils::auto_lock<utils::ex_lock_nr> l(_lock);
            msg = _messages.next();
            if (msg == &_messages)
                break;

            msg->remove();
            --_message_count;
        }

        auto rmsg = CONTAINING_RECORD(msg, message_ex, dl);
        rmsg->io_session = nullptr;

        if (resend_msgs) {
            _net.send_message(rmsg);
        }

        // if not resend, the message's callback will not be invoked until timeout,
        // it's too slow - let's try to mimic the failure by recving an empty reply
        else if (rmsg->header->context.u.is_request && !rmsg->header->context.u.is_forwarded) {
            _net.on_recv_reply(rmsg->header->id, nullptr, 0);
        }

        // added in rpc_engine::reply (for server) or rpc_session::send_message (for client)
        rmsg->release_ref();
    }
}

inline bool rpc_session::unlink_message_for_send()
{
    auto n = _messages.next();
    int bcount = 0;

    DCHECK_EQ(0, _sending_buffers.size());
    DCHECK_EQ(0, _sending_msgs.size());

    while (n != &_messages) {
        auto lmsg = CONTAINING_RECORD(n, message_ex, dl);
        auto lcount = _parser->get_buffer_count_on_send(lmsg);
        if (bcount > 0 && bcount + lcount > _max_buffer_block_count_per_send) {
            break;
        }

        _sending_buffers.resize(bcount + lcount);
        auto rcount = _parser->get_buffers_on_send(lmsg, &_sending_buffers[bcount]);
        CHECK_GE(lcount, rcount);
        if (lcount != rcount)
            _sending_buffers.resize(bcount + rcount);
        bcount += rcount;
        _sending_msgs.push_back(lmsg);

        n = n->next();
        lmsg->dl.remove();
    }

    // added in send_message
    _message_count -= (int)_sending_msgs.size();
    return _sending_msgs.size() > 0;
}

DEFINE_TASK_CODE(LPC_DELAY_RPC_REQUEST_RATE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

void rpc_session::start_read_next(int read_next)
{
    // server only
    if (!is_client()) {
        int delay_ms = _delay_server_receive_ms.exchange(0);

        // delayed read
        if (delay_ms > 0) {
            this->add_ref();
            dsn::task_ptr delay_task(new raw_task(LPC_DELAY_RPC_REQUEST_RATE, [this]() {
                start_read_next();
                this->release_ref();
            }));
            delay_task->enqueue(std::chrono::milliseconds(delay_ms));
        } else {
            do_read(read_next);
        }
    } else {
        do_read(read_next);
    }
}

int rpc_session::prepare_parser()
{
    if (_reader._buffer_occupied < sizeof(uint32_t))
        return sizeof(uint32_t) - _reader._buffer_occupied;

    auto hdr_format = message_parser::get_header_type(_reader._buffer.data());
    if (hdr_format == NET_HDR_INVALID) {
        hdr_format = _net.unknown_msg_hdr_format();

        if (hdr_format == NET_HDR_INVALID) {
            LOG_ERROR("invalid header type, remote_client = {}, header_type = '{}'",
                      _remote_addr,
                      message_parser::get_debug_string(_reader._buffer.data()));
            return -1;
        }
    }
    _parser = _net.new_message_parser(hdr_format);
    LOG_DEBUG(
        "message parser created, remote_client = {}, header_format = {}", _remote_addr, hdr_format);

    return 0;
}

void rpc_session::send_message(message_ex *msg)
{
    msg->add_ref(); // released in on_send_completed
    msg->io_session = this;

    // ignore msg if join point return false
    if (dsn_unlikely(!on_rpc_send_message.execute(msg, true))) {
        msg->release_ref();
        return;
    }

    CHECK_NOTNULL(_parser, "parser should not be null when send");
    _parser->prepare_on_send(msg);

    uint64_t sig;
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        msg->dl.insert_before(&_messages);
        ++_message_count;

        if ((SS_CONNECTED == _connect_state) && !_is_sending_next) {
            _is_sending_next = true;
            sig = _message_sent + 1;
            unlink_message_for_send();
        } else {
            return;
        }
    }

    this->send(sig);
}

bool rpc_session::cancel(message_ex *request)
{
    if (request->io_session.get() != this)
        return false;

    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        if (request->dl.is_alone())
            return false;

        request->dl.remove();
        --_message_count;
    }

    // added in rpc_engine::reply (for server) or rpc_session::send_message (for client)
    request->release_ref();
    request->io_session = nullptr;
    return true;
}

void rpc_session::on_send_completed(uint64_t signature)
{
    uint64_t sig = 0;
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        if (signature != 0) {
            CHECK(_is_sending_next && signature == _message_sent + 1, "sent msg must be sending");
            _is_sending_next = false;

            // the _sending_msgs may have been cleared when reading of the rpc_session is failed.
            if (_sending_msgs.size() == 0) {
                CHECK_EQ_MSG(_connect_state,
                             SS_DISCONNECTED,
                             "assume sending queue is cleared due to session closed");
                return;
            }

            for (auto &msg : _sending_msgs) {
                // added in rpc_engine::reply (for server) or rpc_session::send_message (for client)
                msg->release_ref();
                _message_sent++;
            }
            _sending_msgs.clear();
            _sending_buffers.clear();
        }

        if (!_is_sending_next) {
            if (unlink_message_for_send()) {
                sig = _message_sent + 1;
                _is_sending_next = true;
            }
        }
    }

    // for next send messages
    if (sig != 0)
        this->send(sig);
}

rpc_session::rpc_session(connection_oriented_network &net,
                         ::dsn::rpc_address remote_addr,
                         message_parser_ptr &parser,
                         bool is_client)
    : _connect_state(is_client ? SS_DISCONNECTED : SS_CONNECTED),
      _message_count(0),
      _is_sending_next(false),
      _message_sent(0),
      _net(net),
      _remote_addr(remote_addr),
      // TODO(yingchun): '_remote_host_port' is possible to be invalid after this!
      // TODO(yingchun): It's too cost to reverse resolve host in constructor.
      _remote_host_port(host_port::from_address(_remote_addr)),
      _max_buffer_block_count_per_send(net.max_buffer_block_count_per_send()),
      _reader(net.message_buffer_block_size()),
      _parser(parser),
      _is_client(is_client),
      _matcher(_net.engine()->matcher()),
      _delay_server_receive_ms(0)
{
    LOG_WARNING_IF(!_remote_host_port, "'{}' can not be reverse resolved", _remote_addr);
    if (!is_client) {
        on_rpc_session_connected.execute(this);
    }
}

bool rpc_session::on_disconnected(bool is_write)
{
    bool ret;
    if (set_disconnected()) {
        rpc_session_ptr sp = this;
        if (is_client()) {
            _net.on_client_session_disconnected(sp);
        } else {
            _net.on_server_session_disconnected(sp);
        }

        ret = true;
    } else {
        ret = false;
    }

    if (is_write) {
        clear_send_queue(false);
    }

    return ret;
}

void rpc_session::on_failure(bool is_write)
{
    // Just update the state machine here.
    if (on_disconnected(is_write)) {
        // The under layer socket may be used by async_* interfaces concurrently, it's not thread
        // safe to invalidate the '_socket', it should be invalidated when the session is
        // destroyed.
        LOG_WARNING("disconnect to remote {}, the socket will be lazily closed when the session "
                    "destroyed",
                    _remote_addr);
    }
}

bool rpc_session::on_recv_message(message_ex *msg, int delay_ms)
{
    if (!msg->header->from_address) {
        msg->header->from_address = _remote_addr;
    }

    msg->to_address = _net.address();
    msg->to_host_port = _net.host_port();
    msg->io_session = this;

    // ignore msg if join point return false
    if (dsn_unlikely(!on_rpc_recv_message.execute(msg, true))) {
        delete msg;
        return false;
    }

    if (msg->header->context.u.is_request) {
        // ATTENTION: need to check if self connection occurred.
        //
        // When we try to connect some socket in the same host, if we don't bind the client to a
        // specific port,
        // operating system will provide ephemeral port for us. If it's happened to be the one we
        // want to connect to,
        // it causes self connection.
        //
        // The case is:
        // - this session is a client session
        // - the remote address is in the same host
        // - the remote address is not listened, which means the remote port is not occupied
        // - operating system chooses the remote port as client's ephemeral port
        if (is_client() && msg->header->from_address == _net.engine()->primary_address()) {
            LOG_ERROR("self connection detected, address = {}", msg->header->from_address);
            CHECK_EQ_MSG(msg->get_count(), 0, "message should not be referenced by anybody so far");
            delete msg;
            return false;
        }

        DCHECK(!is_client(), "only rpc server session can recv rpc requests");
        _net.on_recv_request(msg, delay_ms);
    }

    // both rpc server session and rpc client session can receive rpc reply
    // specially, rpc client session can receive general rpc reply,
    // and rpc server session can receive forwarded rpc reply
    else {
        _matcher->on_recv_reply(&_net, msg->header->id, msg, delay_ms);
    }

    return true;
}

bool rpc_session::try_pend_message(message_ex *msg)
{
    // if negotiation is not succeed, we should pend msg,
    // in order to resend it when the negotiation is succeed
    if (dsn_unlikely(!negotiation_succeed)) {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        if (!negotiation_succeed) {
            msg->add_ref();
            _pending_messages.push_back(msg);
            return true;
        }
    }
    return false;
}

void rpc_session::clear_pending_messages()
{
    utils::auto_lock<utils::ex_lock_nr> l(_lock);
    for (auto msg : _pending_messages) {
        msg->release_ref();
    }
    _pending_messages.clear();
}

void rpc_session::set_negotiation_succeed()
{
    std::vector<message_ex *> swapped_pending_msgs;
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        negotiation_succeed = true;

        _pending_messages.swap(swapped_pending_msgs);
    }

    // resend the pending messages
    for (auto msg : swapped_pending_msgs) {
        send_message(msg);
        msg->release_ref();
    }
}

bool rpc_session::is_negotiation_succeed() const
{
    // double check. the first one don't lock the _lock.
    // Because negotiation_succeed only transfered from false to true.
    // So if it is true now, it will not change in the later.
    // But if it is false now, maybe it will change soon. So we should use lock to protect it.
    if (dsn_likely(negotiation_succeed)) {
        return negotiation_succeed;
    } else {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        return negotiation_succeed;
    }
}

void rpc_session::set_client_username(const std::string &user_name)
{
    _client_username = user_name;
}

const std::string &rpc_session::get_client_username() const { return _client_username; }

////////////////////////////////////////////////////////////////////////////////////////////////
network::network(rpc_engine *srv, network *inner_provider)
    : _engine(srv), _client_hdr_format(NET_HDR_DSN), _unknown_msg_header_format(NET_HDR_INVALID)
{
    _message_buffer_block_size = 1024 * 64;
    _max_buffer_block_count_per_send = 64; // TODO: windows, how about the other platforms?
    _unknown_msg_header_format =
        network_header_format::from_string(FLAGS_unknown_message_header_format, NET_HDR_INVALID);
}

void network::reset_parser_attr(network_header_format client_hdr_format,
                                int message_buffer_block_size)
{
    _client_hdr_format = client_hdr_format;
    _message_buffer_block_size = message_buffer_block_size;
}

service_node *network::node() const { return _engine->node(); }

void network::on_recv_request(message_ex *msg, int delay_ms)
{
    return _engine->on_recv_request(this, msg, delay_ms);
}

void network::on_recv_reply(uint64_t id, message_ex *msg, int delay_ms)
{
    _engine->matcher()->on_recv_reply(this, id, msg, delay_ms);
}

message_parser *network::new_message_parser(network_header_format hdr_format)
{
    message_parser *parser = message_parser_manager::instance().create_parser(hdr_format);
    CHECK_NOTNULL(parser, "message parser '{}' not registerd or invalid!", hdr_format);
    return parser;
}

uint32_t network::get_local_ipv4()
{
    uint32_t ip = 0;
    error_s s;
    if (!utils::is_empty(FLAGS_explicit_host_address)) {
        s = rpc_address::ipv4_from_host(FLAGS_explicit_host_address, &ip);
    }

    if (!s || 0 == ip) {
        ip = rpc_address::ipv4_from_network_interface(FLAGS_primary_interface);
    }

    if (0 == ip) {
        char name[128] = {0};
        CHECK_EQ_MSG(::gethostname(name, sizeof(name)),
                     0,
                     "gethostname failed, err = {}",
                     utils::safe_strerror(errno));
        CHECK_OK(rpc_address::ipv4_from_host(name, &ip), "ipv4_from_host for '{}' failed", name);
    }

    return ip;
}

connection_oriented_network::connection_oriented_network(rpc_engine *srv, network *inner_provider)
    : network(srv, inner_provider),
      METRIC_VAR_INIT_server(network_client_sessions),
      METRIC_VAR_INIT_server(network_server_sessions)
{
}

void connection_oriented_network::inject_drop_message(message_ex *msg, bool is_send)
{
    rpc_session_ptr s = msg->io_session;
    if (s == nullptr) {
        // - if io_session == nulltr, there must be is_send == true;
        // - but if is_send == true, there may be is_session != nullptr, when it is a
        //   normal (not forwarding) reply message from server to client, in which case
        //   the io_session has also been set.
        CHECK(is_send, "received message should always has io_session set");
        utils::auto_read_lock l(_clients_lock);
        auto it = _clients.find(msg->to_address);
        if (it != _clients.end()) {
            s = it->second;
        }
    }

    if (s != nullptr) {
        s->close();
    }
}

void connection_oriented_network::send_message(message_ex *request)
{
    rpc_session_ptr client = nullptr;
    auto &to = request->to_address;

    // TODO: thread-local client ptr cache
    {
        utils::auto_read_lock l(_clients_lock);
        auto it = _clients.find(to);
        if (it != _clients.end()) {
            client = it->second;
        }
    }

    int ip_count = 0;
    bool new_client = false;
    if (nullptr == client.get()) {
        utils::auto_write_lock l(_clients_lock);
        auto it = _clients.find(to);
        if (it != _clients.end()) {
            client = it->second;
        } else {
            client = create_client_session(to);
            _clients.insert(client_sessions::value_type(to, client));
            new_client = true;
        }
        ip_count = (int)_clients.size();
    }

    // init connection if necessary
    if (new_client) {
        LOG_INFO("client session created, remote_server = {}, current_count = {}",
                 client->remote_address(),
                 ip_count);
        METRIC_VAR_SET(network_client_sessions, ip_count);
        client->connect();
    }

    // rpc call
    client->send_message(request);
}

rpc_session_ptr connection_oriented_network::get_server_session(::dsn::rpc_address ep)
{
    utils::auto_read_lock l(_servers_lock);
    auto it = _servers.find(ep);
    return it != _servers.end() ? it->second : nullptr;
}

void connection_oriented_network::on_server_session_accepted(rpc_session_ptr &s)
{
    int ip_count = 0;
    int ip_conn_count = 1;
    {
        utils::auto_write_lock l(_servers_lock);

        auto pr = _servers.insert(server_sessions::value_type(s->remote_address(), s));
        if (pr.second) {
            // nothing to do
        } else {
            pr.first->second = s;
            LOG_WARNING("server session already exists, remote_client = {}, preempted",
                        s->remote_address());
        }
        ip_count = (int)_servers.size();

        auto pr2 =
            _ip_conn_count.insert(ip_connection_count::value_type(s->remote_address().ip(), 1));
        if (!pr2.second) {
            ip_conn_count = ++pr2.first->second;
        }
    }

    LOG_INFO("server session accepted, remote_client = {}, current_count = {}",
             s->remote_address(),
             ip_count);

    LOG_INFO("ip session {}, remote_client = {}, current_count = {}",
             ip_conn_count == 1 ? "inserted" : "increased",
             s->remote_address(),
             ip_conn_count);

    METRIC_VAR_SET(network_server_sessions, ip_count);
}

void connection_oriented_network::on_server_session_disconnected(rpc_session_ptr &s)
{
    // how many unique client(the same ip:port is considered to be a unique client)
    int ip_count = 0;
    // one unique client may remain more than one connection on the server, which
    // is an unexpected behavior of client, we should record it in logs.
    int ip_conn_count = 0;

    bool session_removed = false;
    {
        utils::auto_write_lock l(_servers_lock);
        auto it = _servers.find(s->remote_address());
        if (it != _servers.end() && it->second.get() == s.get()) {
            _servers.erase(it);
            session_removed = true;
        }
        ip_count = (int)_servers.size();

        auto it2 = _ip_conn_count.find(s->remote_address().ip());
        if (it2 != _ip_conn_count.end()) {
            if (it2->second > 1) {
                it2->second -= 1;
                ip_conn_count = it2->second;
            } else {
                _ip_conn_count.erase(it2);
            }
        }
    }

    if (session_removed) {
        LOG_INFO("session {} disconnected, the total client sessions count remains {}",
                 s->remote_address(),
                 ip_count);
        METRIC_VAR_SET(network_server_sessions, ip_count);
    }

    if (ip_conn_count == 0) {
        // TODO(wutao1): print ip only
        LOG_INFO("client ip {} has no more session to this server", s->remote_address());
    } else {
        LOG_INFO("client ip {} has still {} of sessions to this server",
                 s->remote_address(),
                 ip_conn_count);
    }
}

bool connection_oriented_network::check_if_conn_threshold_exceeded(::dsn::rpc_address ep)
{
    if (FLAGS_conn_threshold_per_ip <= 0) {
        LOG_DEBUG("new client from {} is connecting to server {}, no connection threshold",
                  ep.ipv4_str(),
                  address());
        return false;
    }

    bool exceeded = false;
    int ip_conn_count = 0; // the amount of connections from this ip address.
    {
        utils::auto_read_lock l(_servers_lock);
        auto it = _ip_conn_count.find(ep.ip());
        if (it != _ip_conn_count.end()) {
            ip_conn_count = it->second;
        }
    }
    if (ip_conn_count >= FLAGS_conn_threshold_per_ip) {
        exceeded = true;
    }

    LOG_DEBUG("new client from {} is connecting to server {}, existing connection count = {}, "
              "threshold = {}",
              ep.ipv4_str(),
              address(),
              ip_conn_count,
              FLAGS_conn_threshold_per_ip);

    return exceeded;
}

void connection_oriented_network::on_client_session_connected(rpc_session_ptr &s)
{
    int ip_count = 0;
    bool r = false;
    {
        utils::auto_read_lock l(_clients_lock);
        auto it = _clients.find(s->remote_address());
        if (it != _clients.end() && it->second.get() == s.get()) {
            r = true;
        }
        ip_count = (int)_clients.size();
    }

    if (r) {
        LOG_INFO("client session connected, remote_server = {}, current_count = {}",
                 s->remote_address(),
                 ip_count);
        METRIC_VAR_SET(network_client_sessions, ip_count);
    }
}

void connection_oriented_network::on_client_session_disconnected(rpc_session_ptr &s)
{
    int ip_count = 0;
    bool r = false;
    {
        utils::auto_write_lock l(_clients_lock);
        auto it = _clients.find(s->remote_address());
        if (it != _clients.end() && it->second.get() == s.get()) {
            _clients.erase(it);
            r = true;
        }
        ip_count = (int)_clients.size();
    }

    if (r) {
        LOG_INFO("client session disconnected, remote_server = {}, current_count = {}",
                 s->remote_address(),
                 ip_count);
        METRIC_VAR_SET(network_client_sessions, ip_count);
    }
}

} // namespace dsn
