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

#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <list>
#include <string_view>
#include <type_traits>
#include <utility>

#include "gutil/map_util.h"
#include "fmt/core.h"
#include "message_parser_manager.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_engine.h"
#include "runtime/api_task.h"
#include "task/task.h"
#include "task/task_code.h"
#include "utils/blob.h"
#include "utils/customizable_id.h"
#include "utils/defer.h"
#include "utils/errors.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/safe_strerror_posix.h"
#include "utils/strings.h"
#include "utils/threadpool_code.h"

METRIC_DEFINE_entity(connection);

METRIC_DEFINE_gauge_int64(connection,
                          network_client_sessions,
                          dsn::metric_unit::kSessions,
                          "The number of sessions from client side for each remote server "
                          "address (i.e. <ip>:<port>)");

METRIC_DEFINE_gauge_int64(server,
                          network_server_sessions,
                          dsn::metric_unit::kSessions,
                          "The number of sessions from server side");

DSN_DEFINE_uint32(network,
                  conn_threshold_per_ip,
                  0,
                  "The maximum connection count to each server per IP address, 0 means no limit");

DSN_DEFINE_uint32(network,
                  conn_pool_max_size,
                  4,
                  "The maximum number of client connections allowed for a pool to the same "
                  "remote address (i.e. <ip>:<port>)");
DSN_DEFINE_validator(conn_pool_max_size, [](uint32_t value) -> bool { return value > 0; });

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

namespace {

metric_entity_ptr instantiate_connection_metric_entity(const std::string &remote_addr)
{
    auto entity_id = fmt::format("connection@{}", remote_addr);

    return METRIC_ENTITY_connection.instantiate(entity_id, {{"remote_addr", remote_addr}});
}

} // anonymous namespace

connection_metrics::connection_metrics(const std::string &remote_addr)
    : _remote_addr(remote_addr),
      _connection_metric_entity(instantiate_connection_metric_entity(remote_addr)),
      METRIC_VAR_INIT_connection(network_client_sessions)
{
}

const metric_entity_ptr &connection_metrics::connection_metric_entity() const
{
    CHECK_NOTNULL(_connection_metric_entity,
                  "connection metric entity (remote_addr={}) should has been instantiated: "
                  "uninitialized entity cannot be used to instantiate metric",
                  _remote_addr);
    return _connection_metric_entity;
}

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
        CHECK_EQ_PREFIX_MSG(0, _sending_msgs.size(), "sending messages have not been cleared yet");
        CHECK_EQ_PREFIX_MSG(0, _batched_msg_count, "batched messages have not been cleared yet");
        CHECK_EQ_PREFIX_MSG(
            0, _queued_msg_count.load(), "there should not be any queued message now");
    }
}

bool rpc_session::mark_connecting()
{
    CHECK_PREFIX_MSG(is_client(), "must be client session");

    utils::auto_lock<utils::ex_lock_nr> l(_lock);

    if (_connect_state != SS_DISCONNECTED) {
        return false;
    }

    _connect_state = SS_CONNECTING;
    return true;
}

void rpc_session::mark_connected()
{
    CHECK_PREFIX_MSG(is_client(), "must be client session");

    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        CHECK_EQ_PREFIX(_connect_state, SS_CONNECTING);
        _connect_state = SS_CONNECTED;
    }

    rpc_session_ptr sp = this;
    _net.on_client_session_connected(sp);

    on_rpc_session_connected.execute(this);
}

bool rpc_session::mark_disconnected()
{
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        if (_connect_state == SS_DISCONNECTED) {
            return false;
        }

        _connect_state = SS_DISCONNECTED;
    }

    on_rpc_session_disconnected.execute(this);
    return true;
}

void rpc_session::clear_send_queue(bool resend_msgs)
{
    //
    // - in concurrent case, resending _sending_msgs and _batched_msgs
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
        _queued_msg_count -= static_cast<int>(_sending_msgs.size());
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
        dlink *msg{nullptr};
        {
            utils::auto_lock<utils::ex_lock_nr> l(_lock);
            msg = _batched_msgs.next();
            if (msg == &_batched_msgs) {
                break;
            }

            msg->remove();
            --_batched_msg_count;
            --_queued_msg_count;
        }

        auto *rmsg = CONTAINING_RECORD(msg, message_ex, dl); // NOLINT
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
    auto *n = _batched_msgs.next();
    int bcount = 0;

    DCHECK_EQ_PREFIX(0, _sending_buffers.size());
    DCHECK_EQ_PREFIX(0, _sending_msgs.size());

    while (n != &_batched_msgs) {
        auto *lmsg = CONTAINING_RECORD(n, message_ex, dl); // NOLINT
        const auto lcount = _parser->get_buffer_count_on_send(lmsg);
        if (bcount > 0 && bcount + lcount > _max_buffer_block_count_per_send) {
            break;
        }

        _sending_buffers.resize(bcount + lcount);
        const auto rcount = _parser->get_buffers_on_send(lmsg, &_sending_buffers[bcount]);
        CHECK_GE_PREFIX(lcount, rcount);

        if (lcount != rcount) {
            _sending_buffers.resize(bcount + rcount);
        }

        bcount += rcount;
        _sending_msgs.push_back(lmsg);

        n = n->next();
        lmsg->dl.remove();
    }

    // added in send_message
    _batched_msg_count -= static_cast<int>(_sending_msgs.size());
    return !_sending_msgs.empty();
}

DEFINE_TASK_CODE(LPC_DELAY_RPC_REQUEST_RATE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

void rpc_session::start_read_next(int read_next)
{
    if (is_client()) {
        do_read(read_next);
        return;
    }

    const int delay_ms = _delay_server_receive_ms.exchange(0);
    if (delay_ms <= 0) {
        do_read(read_next);
        return;
    }

    // delayed read
    this->add_ref();
    dsn::task_ptr delay_task(new raw_task(LPC_DELAY_RPC_REQUEST_RATE, [this]() {
        start_read_next();
        this->release_ref();
    }));
    delay_task->enqueue(std::chrono::milliseconds(delay_ms));
}

int rpc_session::prepare_parser()
{
    if (_reader._buffer_occupied < sizeof(uint32_t)) {
        return static_cast<int>(sizeof(uint32_t) - _reader._buffer_occupied);
    }

    auto hdr_format = message_parser::get_header_type(_reader._buffer.data());
    if (hdr_format == NET_HDR_INVALID) {
        hdr_format = _net.unknown_msg_hdr_format();

        if (hdr_format == NET_HDR_INVALID) {
            LOG_ERROR_PREFIX("invalid header type, remote_client = {}, header_type = '{}'",
                             _remote_addr,
                             message_parser::get_debug_string(_reader._buffer.data()));
            return -1;
        }
    }

    _parser = _net.new_message_parser(hdr_format);
    LOG_DEBUG_PREFIX(
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

    CHECK_NOTNULL_PREFIX_MSG(_parser, "parser should not be null when send");
    _parser->prepare_on_send(msg);

    uint64_t sig{0};
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);

        // Cache the message firstly, to be sent in batch later.
        msg->dl.insert_before(&_batched_msgs);
        ++_batched_msg_count;
        ++_queued_msg_count;

        // Messages cannot be sent until following conditions are met:
        // 1. the connection should be established successfully, since connecting to remote
        // is asynchronous, and
        // 2. there is not another batch being sent.
        if ((SS_CONNECTED != _connect_state) || _is_sending_next) {
            return;
        }

        _is_sending_next = true;
        sig = _message_sent + 1;
        unlink_message_for_send();
    }

    this->send(sig);
}

bool rpc_session::cancel(message_ex *request)
{
    if (request->io_session.get() != this)
        return false;

    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);

        if (request->dl.is_alone()) {
            return false;
        }

        request->dl.remove();
        --_batched_msg_count;
        --_queued_msg_count;
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
            CHECK_PREFIX_MSG(_is_sending_next && signature == _message_sent + 1,
                             "sent msg must be sending");
            _is_sending_next = false;

            // the _sending_msgs may have been cleared when reading of the rpc_session is failed.
            if (_sending_msgs.empty()) {
                CHECK_EQ_PREFIX_MSG(_connect_state,
                                    SS_DISCONNECTED,
                                    "assume sending queue is cleared due to session closed");
                return;
            }

            for (auto &msg : _sending_msgs) {
                // added in rpc_engine::reply (for server) or rpc_session::send_message (for client)
                msg->release_ref();
                _message_sent++;
            }

            _queued_msg_count -= static_cast<int>(_sending_msgs.size());
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
    if (sig != 0) {
        this->send(sig);
    }
}

rpc_session::rpc_session(connection_oriented_network &net,
                         rpc_address remote_addr,
                         message_parser_ptr &parser,
                         bool is_client)
    : _connect_state(is_client ? SS_DISCONNECTED : SS_CONNECTED),
      _batched_msg_count(0),
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
      _log_prefix(fmt::format(
          "[{}][remote@{}]", is_client ? "client session" : "server session", remote_addr)),
      _matcher(_net.engine()->matcher()),
      _delay_server_receive_ms(0)
{
    LOG_WARNING_IF_PREFIX(!_remote_host_port, "'{}' can not be reverse resolved", _remote_addr);
    if (!is_client) {
        on_rpc_session_connected.execute(this);
    }
}

bool rpc_session::on_disconnected(bool is_write)
{
    const auto cleanup = defer([is_write, this]() {
        if (is_write) {
            clear_send_queue(false);
        }
    });

    if (!mark_disconnected()) {
        return false;
    }

    const rpc_session_ptr session(this);
    if (is_client()) {
        _net.on_client_session_disconnected(session);
    } else {
        _net.on_server_session_disconnected(session);
    }

    return true;
}

void rpc_session::on_failure(bool is_write)
{
    // Just update the state machine here.
    if (on_disconnected(is_write)) {
        // The under layer socket may be used by async_* interfaces concurrently, it's not thread
        // safe to invalidate the '_socket', it should be invalidated when the session is
        // destroyed.
        LOG_WARNING_PREFIX("disconnect to remote {}, the socket will be lazily closed when "
                           "the session destroyed",
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
            LOG_ERROR_PREFIX("self connection detected, address = {}", msg->header->from_address);
            CHECK_EQ_PREFIX_MSG(
                msg->get_count(), 0, "message should not be referenced by anybody so far");
            delete msg;
            return false;
        }

        DCHECK_PREFIX_MSG(!is_client(), "only rpc server session can recv rpc requests");
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
            _pending_msgs.push_back(msg);
            return true;
        }
    }
    return false;
}

void rpc_session::clear_pending_messages()
{
    utils::auto_lock<utils::ex_lock_nr> l(_lock);
    for (auto *msg : _pending_msgs) {
        msg->release_ref();
    }
    _pending_msgs.clear();
}

void rpc_session::set_negotiation_succeed()
{
    std::vector<message_ex *> swapped_pending_msgs;
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        negotiation_succeed = true;

        _pending_msgs.swap(swapped_pending_msgs);
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
    : network(srv, inner_provider), METRIC_VAR_INIT_server(network_server_sessions)
{
}

void connection_oriented_network::inject_drop_message(message_ex *msg, bool is_send)
{
    const rpc_session_ptr session(msg->io_session);
    if (session != nullptr) {
        session->close();
        return;
    }

    // - if io_session == nulltr, there must be is_send == true;
    // - but if is_send == true, there may be is_session != nullptr, when it is a
    //   normal (not forwarding) reply message from server to client, in which case
    //   the io_session has also been set.
    CHECK(is_send, "received message should always has io_session set");

    utils::auto_read_lock l(_clients_lock);

    const auto iter = std::as_const(_clients).find(msg->to_address);
    if (iter == _clients.end()) {
        return;
    }

    CHECK(iter->second, "connection pool must be non-null");
    iter->second->close();
}

void connection_oriented_network::send_message(message_ex *request)
{
    rpc_session_pool_ptr client_pool;
    const auto server_addr = request->to_address;

    // TODO(wangdan): thread-local client ptr cache
    {
        utils::auto_read_lock l(_clients_lock);
        const auto iter = std::as_const(_clients).find(server_addr);
        if (iter != _clients.end()) {
            client_pool = iter->second;
        }
    }

    if (!client_pool) {
        utils::auto_write_lock l(_clients_lock);
        const auto iter = std::as_const(_clients).find(server_addr);
        if (iter != _clients.end()) {
            client_pool = iter->second;
        } else {
            client_pool = rpc_session_pool::create(this, server_addr);
            _clients.try_emplace(server_addr, client_pool);
        }
    }

    client_pool->get_rpc_session()->send_message(request);
}

rpc_session_ptr connection_oriented_network::get_server_session(rpc_address client_addr)
{
    utils::auto_read_lock l(_servers_lock);
    return gutil::FindWithDefault(_servers, client_addr);
}

void connection_oriented_network::add_server_session(const rpc_session_ptr &session)
{
    const auto [iter, inserted] = _servers.try_emplace(session->remote_address(), session);
    if (inserted) {
        return;
    }

    iter->second = session;
    LOG_WARNING("server session already exists, remote_client = {}, preempted",
                session->remote_address());
}

uint32_t connection_oriented_network::add_server_conn_count(const rpc_session_ptr &session)
{
    const auto [iter, inserted] = _ip_conn_counts.try_emplace(session->remote_address().ip(), 1);
    if (inserted) {
        return 1;
    }

    return ++iter->second;
}

void connection_oriented_network::on_server_session_accepted(const rpc_session_ptr &session)
{
    uint32_t ip_count{0};
    uint32_t ip_conn_count{0};

    {
        utils::auto_write_lock l(_servers_lock);

        add_server_session(session);
        ip_count = static_cast<uint32_t>(_servers.size());

        ip_conn_count = add_server_conn_count(session);
    }

    LOG_INFO("server session accepted, remote_client = {}, current_count = {}",
             session->remote_address(),
             ip_count);

    LOG_INFO("ip session {}, remote_client = {}, current_count = {}",
             ip_conn_count == 1 ? "inserted" : "increased",
             session->remote_address(),
             ip_conn_count);

    METRIC_VAR_SET(network_server_sessions, ip_count);
}

bool connection_oriented_network::remove_server_session(const rpc_session_ptr &session)
{
    const auto iter = std::as_const(_servers).find(session->remote_address());
    if (iter == _servers.end() || iter->second.get() != session.get()) {
        return false;
    }

    _servers.erase(iter);
    return true;
}

uint32_t connection_oriented_network::remove_server_conn_count(const rpc_session_ptr &session)
{
    const auto iter = _ip_conn_counts.find(session->remote_address().ip());
    if (iter == _ip_conn_counts.end()) {
        return 0;
    }

    if (iter->second <= 1) {
        _ip_conn_counts.erase(iter);
        return 0;
    }

    return --iter->second;
}

void connection_oriented_network::on_server_session_disconnected(const rpc_session_ptr &session)
{
    bool session_removed{false};

    // how many unique client(the same ip:port is considered to be a unique client)
    uint32_t ip_count{0};

    // one unique client may remain more than one connection on the server, which
    // is an unexpected behavior of client, we should record it in logs.
    uint32_t ip_conn_count{0};

    {
        utils::auto_write_lock l(_servers_lock);

        session_removed = remove_server_session(session);
        ip_count = static_cast<uint32_t>(_servers.size());

        ip_conn_count = remove_server_conn_count(session);
    }

    if (session_removed) {
        LOG_INFO("session {} disconnected, the total client sessions count remains {}",
                 session->remote_address(),
                 ip_count);
        METRIC_VAR_SET(network_server_sessions, ip_count);
    }

    if (ip_conn_count == 0) {
        // TODO(wutao1): print ip only
        LOG_INFO("client ip {} has no more session to this server", session->remote_address());
    } else {
        LOG_INFO("client ip {} has still {} of sessions to this server",
                 session->remote_address(),
                 ip_conn_count);
    }
}

bool connection_oriented_network::check_if_conn_threshold_exceeded(rpc_address client_addr)
{
    if (FLAGS_conn_threshold_per_ip <= 0) {
        LOG_DEBUG("new client from {} is connecting to server {}, no connection threshold",
                  client_addr.ipv4_str(),
                  address());
        return false;
    }

    uint32_t ip_conn_count{0}; // the amount of connections from this ip address.
    {
        utils::auto_read_lock l(_servers_lock);
        ip_conn_count = gutil::FindWithDefault(_ip_conn_counts, client_addr.ip());
    }

    LOG_DEBUG("new client from {} is connecting to server {}, existing connection count = {}, "
              "threshold = {}",
              client_addr.ipv4_str(),
              address(),
              ip_conn_count,
              FLAGS_conn_threshold_per_ip);

    return ip_conn_count >= FLAGS_conn_threshold_per_ip;
}

void connection_oriented_network::on_client_session_connected(const rpc_session_ptr &session)
{
    utils::auto_read_lock l(_clients_lock);
    const auto iter = std::as_const(_clients).find(session->remote_address());
    if (iter == _clients.end()) {
        return;
    }

    iter->second->on_connected(session);
}

void connection_oriented_network::on_client_session_disconnected(const rpc_session_ptr &session)
{
    utils::auto_write_lock l(_clients_lock);

    const auto iter = _clients.find(session->remote_address());
    if (iter == _clients.end()) {
        return;
    }

    const rpc_session_pool_ptr pool(iter->second);
    pool->on_disconnected(session, [iter, this]() { _clients.erase(iter); });
}

rpc_session_pool::rpc_session_pool(connection_oriented_network *net, rpc_address server_addr)
    : _net(net), _server_addr(server_addr), _conn_metrics(server_addr.to_string())
{
}

rpc_session_ptr rpc_session_pool::select_rpc_session() const
{
    CHECK_GT(_sessions.size(), 0);

    return std::min_element(_sessions.begin(),
                            _sessions.end(),
                            [](const auto &lhs, const auto &rhs) {
                                return lhs.first->queued_message_count() <
                                       rhs.first->queued_message_count();
                            })
        ->second;
}

rpc_session_ptr rpc_session_pool::create_rpc_session()
{
    const auto session = _net->create_client_session(_server_addr);

    _sessions.try_emplace(session.get(), session);

    LOG_INFO("[pool] client session created: remote = {}, count = {}",
             session->remote_address(),
             _sessions.size());
    METRIC_SET(_conn_metrics, network_client_sessions, _sessions.size());

    return session;
}

rpc_session_ptr rpc_session_pool::get_rpc_session()
{
    {
        utils::auto_read_lock l(_lock);
        if (_sessions.size() >= FLAGS_conn_pool_max_size) {
            return select_rpc_session();
        }
    }

    rpc_session_ptr session;
    {
        utils::auto_write_lock l(_lock);
        if (_sessions.size() >= FLAGS_conn_pool_max_size) {
            return select_rpc_session();
        }

        session = create_rpc_session();
    }

    // Don't place session->connect() under the protection of `_lock`. This is because some
    // implementations of rpc_session (such as sim_client_session) are not asynchronous - after
    // the connection is established, they may immediately invoke mark_connected() within the
    // same thread (which eventually calls rpc_session_pool::on_connected()), leading to deadlock.
    session->connect();

    return session;
}

void rpc_session_pool::on_connected(const rpc_session_ptr &session) const
{
    utils::auto_read_lock l(_lock);

    const auto iter = std::as_const(_sessions).find(session.get());
    if (iter == _sessions.end()) {
        return;
    }

    // No need to update metric since no new session was added.
    LOG_INFO("[pool] client session connected: local = {}, remote = {}, count = {}",
             session->local_address(),
             _server_addr,
             _sessions.size());
}

void rpc_session_pool::on_disconnected(const rpc_session_ptr &session,
                                       std::function<void()> &&on_empty)
{
    utils::auto_write_lock l(_lock);

    const auto iter = std::as_const(_sessions).find(session.get());
    if (iter == _sessions.end()) {
        return;
    }

    _sessions.erase(iter);

    LOG_INFO("[pool] client session disconnected: local = {}, remote = {}, count = {}",
             session->local_address(),
             _server_addr,
             _sessions.size());
    METRIC_SET(_conn_metrics, network_client_sessions, _sessions.size());

    if (_sessions.empty()) {
        on_empty();
    }
}

void rpc_session_pool::close() const
{
    utils::auto_read_lock l(_lock);
    for (const auto &[session, _] : _sessions) {
        session->close();
    }
}

} // namespace dsn
