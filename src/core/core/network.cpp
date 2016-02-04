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

# ifdef _WIN32
# include <Winsock2.h>
# endif
# include <dsn/internal/network.h>
# include <dsn/internal/factory_store.h>
# include "rpc_engine.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "rpc_session"

namespace dsn 
{
    rpc_session::~rpc_session()
    {
        clear_send_queue(false);

        {
            utils::auto_lock<utils::ex_lock_nr> l(_lock);
            dassert(0 == _sending_msgs.size(), "sending queue is not cleared yet");
            dassert(0 == _message_count.load(), "sending queue is not cleared yet");
        }
    }

    bool rpc_session::try_connecting()
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        if (_connect_state == SS_DISCONNECTED)
        {
            _connect_state = SS_CONNECTING;
            return true;
        }
        else
            return false;
    }

    void rpc_session::set_connected()
    {
        {
            utils::auto_lock<utils::ex_lock_nr> l(_lock);
            dassert(_connect_state == SS_CONNECTING, "session must be connecting");
            _connect_state = SS_CONNECTED;
        }

        dinfo("client session connected to %s", remote_address().to_string());
    }

    void rpc_session::set_disconnected()
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        _connect_state = SS_DISCONNECTED;
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

        std::vector<message_ex*> swapped_sending_msgs;
        {
            // protect _sending_msgs and _sending_buffers in lock
            utils::auto_lock<utils::ex_lock_nr> l(_lock);
            _sending_msgs.swap(swapped_sending_msgs);
            _sending_buffers.clear();
        }

        // resend pending messages if need
        for (auto& msg : swapped_sending_msgs)
        {
            if (resend_msgs)
            {
                _net.send_message(msg);
            }

            // if not resend, the message's callback will not be invoked until timeout,
            // it's too slow - let's try to mimic the failure by recving an empty reply
            else if (msg->header->context.u.is_request 
                && !msg->header->context.u.is_forwarded)
            {
                _net.on_recv_reply(msg->header->id, nullptr, 0);
            }

            // added in rpc_engine::reply (for server) or rpc_session::send_message (for client)
            msg->release_ref();
        }

        while (true)
        {
            dlink* msg;
            {
                utils::auto_lock<utils::ex_lock_nr> l(_lock);
                msg = _messages.next();
                if (msg == &_messages)
                    break;

                msg->remove();

                _message_count.fetch_sub(1, std::memory_order_relaxed);
            }
                        
            auto rmsg = CONTAINING_RECORD(msg, message_ex, dl);            
            rmsg->io_session = nullptr;

            if (resend_msgs)
            {
                _net.send_message(rmsg);
            }

            // if not resend, the message's callback will not be invoked until timeout,
            // it's too slow - let's try to mimic the failure by recving an empty reply
            else if (rmsg->header->context.u.is_request
                && !rmsg->header->context.u.is_forwarded)
            {
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
        int tlen = 0;

        dbg_dassert(0 == _sending_buffers.size(), "");
        dbg_dassert(0 == _sending_msgs.size(), "");

        while (n != &_messages)
        {
            auto lmsg = CONTAINING_RECORD(n, message_ex, dl);
            auto lcount = _parser->get_send_buffers_count_and_total_length(lmsg, &tlen);
            if (bcount > 0 && bcount + lcount > _max_buffer_block_count_per_send)
            {
                break;
            }

            _sending_buffers.resize(bcount + lcount);
            _parser->prepare_buffers_on_send(lmsg, 0, &_sending_buffers[bcount]);
            bcount += lcount;
            _sending_msgs.push_back(lmsg);

            n = n->next();
            lmsg->dl.remove();
        }
        
        // added in send_message
        _message_count.fetch_sub((int)_sending_msgs.size(), std::memory_order_relaxed);
        return _sending_msgs.size() > 0;
    }

    void rpc_session::start_read_next(int read_next)
    {
        // server only
        if (!is_client())
        {
            auto s = _recv_state.load(std::memory_order_relaxed);
            switch (s)
            {
            case recv_state::normal:
                do_read(read_next);
                break;
            case recv_state::to_be_paused:
                if (_recv_state.compare_exchange_strong(s, recv_state::paused))
                {
                    // paused
                }
                else if (s == recv_state::normal)
                {
                    do_read(read_next);
                }
                else
                {
                    dassert (s == recv_state::paused, "invalid state %d", (int)s);
                    dassert (false, "invalid execution flow here");
                }
                break;
            default:
                dassert(false, "invalid state %d", (int)s);
            }
        }
        else
        {
            do_read(read_next);
        }
    }
    
    void rpc_session::send_message(message_ex* msg)
    {
        //dinfo("%s: rpc_id = %016llx, code = %s", __FUNCTION__, msg->header->rpc_id, msg->header->rpc_name);
        _message_count.fetch_add(1, std::memory_order_relaxed); // -- in unlink_message

        msg->add_ref(); // released in on_send_completed        
        uint64_t sig;
        {
            utils::auto_lock<utils::ex_lock_nr> l(_lock);
            msg->dl.insert_before(&_messages);
            if (SS_CONNECTED == _connect_state && !_is_sending_next)
            {
                _is_sending_next = true;
                sig = _message_sent + 1;
                unlink_message_for_send();
            }
            else
            {
                return;
            }
        }

        this->send(sig);
    }

    bool rpc_session::cancel(message_ex* request)
    {
        if (request->io_session.get() != this)
            return false;

        {
            utils::auto_lock<utils::ex_lock_nr> l(_lock);
            if (request->dl.is_alone())
                return false;

            request->dl.remove();  
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
            if (signature != 0)
            {
                dassert(_is_sending_next
                    && signature == _message_sent + 1,
                    "sent msg must be sending");
                _is_sending_next = false;

                // the _sending_msgs may have been cleared when reading of the rpc_session is failed.
                if (_sending_msgs.size() == 0)
                {
                    dassert(_connect_state == SS_DISCONNECTED,
                            "assume sending queue is cleared due to session closed");
                    return;
                }
                
                for (auto& msg : _sending_msgs)
                {
                    // added in rpc_engine::reply (for server) or rpc_session::send_message (for client)
                    msg->release_ref();
                    _message_sent++;
                }
                _sending_msgs.clear();
                _sending_buffers.clear();
            }
            
            if (!_is_sending_next)
            {
                if (unlink_message_for_send())
                {
                    sig = _message_sent + 1;
                    _is_sending_next = true;
                }
            }
        }

        // for next send messages
        if (sig != 0)
            this->send(sig);
    }

    bool rpc_session::has_pending_out_msgs()
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        return !_messages.is_alone();
    }

    rpc_session::rpc_session(
        connection_oriented_network& net, 
        ::dsn::rpc_address remote_addr,
        std::unique_ptr<message_parser>&& parser,
        bool is_client
        )
        : _net(net),
        _remote_addr(remote_addr),
        _max_buffer_block_count_per_send(net.max_buffer_block_count_per_send()),
        _parser(std::move(parser)),
        _is_client(is_client),
        _matcher(_net.engine()->matcher()),
        _message_count(0),
        _is_sending_next(false),
        _connect_state(is_client ? SS_DISCONNECTED : SS_CONNECTED),
        _message_sent(0),
        _recv_state(recv_state::normal)
    {
    }

    bool rpc_session::on_disconnected(bool is_write)
    {
        if (is_client())
        {
            set_disconnected();
            rpc_session_ptr sp = this;
            _net.on_client_session_disconnected(sp);
        }
        
        else
        {
            rpc_session_ptr sp = this;
            _net.on_server_session_disconnected(sp);
        }

        if (is_write)
        {
            clear_send_queue(false);
        }

        return true;
    }

    void rpc_session::on_recv_message(message_ex* msg, int delay_ms)
    {
        msg->to_address = _net.address();
        msg->io_session = this;

        if (msg->header->context.u.is_request)
        {
            dbg_dassert(!is_client(), 
                "only rpc server session can recv rpc requests");
            _net.on_recv_request(msg, delay_ms);
        }

        // both rpc server session and rpc client session can receive rpc reply
        // specially, rpc client session can receive general rpc reply,  
        // and rpc server session can receive forwarded rpc reply  
        else
        {
            _matcher->on_recv_reply(&_net, msg->header->id, msg, delay_ms);
        }
    }
    
    ////////////////////////////////////////////////////////////////////////////////////////////////
    network::network(rpc_engine* srv, network* inner_provider)
        : _engine(srv), _parser_type(NET_HDR_DSN)
    {   
        _message_buffer_block_size = 1024 * 64;
        _max_buffer_block_count_per_send = 64; // TODO: windows, how about the other platforms?
        _send_queue_threshold = (int)dsn_config_get_value_uint64(
            "network", "send_queue_threshold",
            4 * 1024, "send queue size above which throttling is applied"
            );
    }

    void network::reset_parser(network_header_format name, int message_buffer_block_size)
    {
        _message_buffer_block_size = message_buffer_block_size;
        _parser_type = name;
    }

    service_node* network::node() const
    {
        return _engine->node();
    }

    void network::on_recv_request(message_ex* msg, int delay_ms)
    {
        return _engine->on_recv_request(this, msg, delay_ms);
    }

    void network::on_recv_reply(uint64_t id, message_ex* msg, int delay_ms)
    {
        _engine->matcher()->on_recv_reply(this, id, msg, delay_ms);
    }

    std::unique_ptr<message_parser> network::new_message_parser()
    {
        message_parser * parser = message_parser_manager::instance().create_parser(
            _parser_type,
            _message_buffer_block_size,
            false
            );
        dassert(parser, "message parser '%s' not registerd or invalid!", _parser_type.to_string());

        return std::unique_ptr<message_parser>(parser);
    }

    std::pair<message_parser::factory2, size_t>  network::get_message_parser_info()
    {
        auto& pinfo = message_parser_manager::instance().get(_parser_type);
        return std::make_pair(pinfo.factory2, pinfo.parser_size);
    }

    uint32_t network::get_local_ipv4()
    {
        static const char* explicit_host = dsn_config_get_value_string(
            "network", "explicit_host_address",
            "", "explicit host name or ip (v4) assigned to this node (e.g., service ip for pods in kubernets)"
            );

        static const char* inteface = dsn_config_get_value_string(
            "network", "primary_interface",
            "eth0", "network interface name used to init primary ip address");

        uint32_t ip = 0;

        if (strlen(explicit_host) > 0)
        {
            ip = dsn_ipv4_from_host(explicit_host);
        }

        if (0 == ip)
        {
            ip = dsn_ipv4_local(inteface);
        }
        
        if (0 == ip)
        {
            char name[128];
            if (gethostname(name, sizeof(name)) != 0)
            {
                dassert(false, "gethostname failed, err = %s\n", strerror(errno));
            }
            ip = dsn_ipv4_from_host(name);
        }

        return ip;
    }

    connection_oriented_network::connection_oriented_network(rpc_engine* srv, network* inner_provider)
        : network(srv, inner_provider)
    {        
    }

    void connection_oriented_network::inject_drop_message(message_ex* msg, bool is_send)
    {
        rpc_session_ptr s = msg->io_session;
        if (s == nullptr)
        {
            // - if io_session == nulltr, there must be is_send == true;
            // - but if is_send == true, there may be is_session != nullptr, when it is a
            //   normal (not forwarding) reply message from server to client, in which case
            //   the io_session has also been set.
            dassert(is_send, "received message should always has io_session set");
            utils::auto_read_lock l(_clients_lock);
            auto it = _clients.find(msg->to_address);
            if (it != _clients.end())
            {
                s = it->second;
            }
        }

        if (s != nullptr)
        {
            s->close_on_fault_injection();
        }
    }

    void connection_oriented_network::send_message(message_ex* request)
    {
        rpc_session_ptr client = nullptr;
        bool new_client = false;
        auto& to = request->to_address;

        // TODO: thread-local client ptr cache
        {
            utils::auto_read_lock l(_clients_lock);
            auto it = _clients.find(to);
            if (it != _clients.end())
            {
                client = it->second;
            }
        }

        if (nullptr == client.get())
        {
            utils::auto_write_lock l(_clients_lock);
            auto it = _clients.find(to);
            if (it != _clients.end())
            {
                client = it->second;
            }
            else
            {
                client = create_client_session(to);
                _clients.insert(client_sessions::value_type(to, client));
                new_client = true;
            }
        }

        // init connection if necessary
        if (new_client) 
        {
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

    void connection_oriented_network::on_server_session_accepted(rpc_session_ptr& s)
    {
        int scount = 0;
        {
            utils::auto_write_lock l(_servers_lock);
            auto pr = _servers.insert(server_sessions::value_type(s->remote_address(), s));
            if (pr.second)
            {
                // nothing to do 
            }
            else
            {
                pr.first->second = s;
                dwarn("server session on %s already exists, preempted", s->remote_address().to_string());
            }
            scount = (int)_servers.size();
        }

        ddebug("server session %s accepted (%d in total)", s->remote_address().to_string(), scount);
    }

    void connection_oriented_network::on_server_session_disconnected(rpc_session_ptr& s)
    {
        int scount;
        bool r = false;
        {
            utils::auto_write_lock l(_servers_lock);
            auto it = _servers.find(s->remote_address());
            if (it != _servers.end() && it->second.get() == s.get())
            {
                _servers.erase(it);
                r = true;
            }      
            scount = (int)_servers.size();
        }

        if (r)
        {
            ddebug("server session %s disconnected (%d in total)",
                s->remote_address().to_string(),
                scount
                );
        }
    }

    rpc_session_ptr connection_oriented_network::get_client_session(::dsn::rpc_address ep)
    {
        utils::auto_read_lock l(_clients_lock);
        auto it = _clients.find(ep);
        return it != _clients.end() ? it->second : nullptr;
    }

    void connection_oriented_network::on_client_session_disconnected(rpc_session_ptr& s)
    {
        int scount;
        bool r = false;
        {
            utils::auto_write_lock l(_clients_lock);
            auto it = _clients.find(s->remote_address());
            if (it != _clients.end() && it->second.get() == s.get())
            {
                _clients.erase(it);
                r = true;
            }
            scount = (int)_clients.size();
        }

        if (r)
        {
            ddebug("client session %s disconnected (%d in total)", s->remote_address().to_string(), scount);
        }
    }
}
