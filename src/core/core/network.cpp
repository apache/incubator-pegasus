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
        // for sending msgs
        for (auto& msg : _sending_msgs)
        {
            msg->release_ref();
        }
        _sending_buffers.clear();
        _sending_msgs.clear();

        while (true)
        {
            message_ex* msg;
            {
                utils::auto_lock<utils::ex_lock_nr> l(_lock);
                msg = _messages.pop_all();
                if (msg == nullptr)
                    break;
            }

            // added in rpc_engine::reply (for server) or rpc_session::call (for client)
            while (msg)
            {
                auto next = msg->next;
                msg->release_ref();
                msg = next;
            }
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
            _reconnect_count_after_last_success = 0;
        }

        dinfo("client session connected to %s", remote_address().to_string());
    }

    void rpc_session::set_disconnected()
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        _connect_state = SS_DISCONNECTED;
    }

    inline bool rpc_session::unlink_message_for_send()
    {
        int bcount = 0;
        int tlen = 0;
        message_ex *current = _messages._first, *first = current, *last = nullptr;

        dbg_dassert(0 == _sending_buffers.size(), "");
        dbg_dassert(0 == _sending_msgs.size(), "");

        while (current)
        {
            auto lcount = _parser->get_send_buffers_count_and_total_length(current, &tlen);
            if (bcount > 0 && bcount + lcount > _max_buffer_block_count_per_send)
            {
                break;
            }   

            _sending_buffers.resize(bcount + lcount);
            _parser->prepare_buffers_on_send(current, 0, &_sending_buffers[bcount]);
            bcount += lcount;
            _sending_msgs.push_back(current);

            last = current;
            current = current->next;
            last->next = nullptr;
        }

        // no pending messages are included for sending
        if (nullptr == last)
        {
            return false;
        }

        // part of the message are included
        else
        {
            _messages._first = current;
            if (last == _messages._last)
                _messages._last = nullptr;
            return true;
        }
    }
    
    void rpc_session::send_message(message_ex* msg)
    {
        uint64_t sig;
        {
            utils::auto_lock<utils::ex_lock_nr> l(_lock);
            _messages.add(msg);
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
    
    void rpc_session::on_send_completed(uint64_t signature)
    {
        uint64_t sig = 0;
        {
            utils::auto_lock<utils::ex_lock_nr> l(_lock);
            if (signature != 0)
            {
                dassert(_is_sending_next 
                    && _sending_msgs.size() > 0 
                    && signature == _message_sent + 1, 
                    "sent msg must be sending");

                _is_sending_next = false; 
                
                for (auto& msg : _sending_msgs)
                {
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

        // for next send messages (double-linked list)
        if (sig != 0)
            this->send(sig);
    }

    bool rpc_session::has_pending_out_msgs()
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        return !_messages.is_empty();
    }

    // client
    rpc_session::rpc_session(
        connection_oriented_network& net,
        ::dsn::rpc_address remote_addr,
        rpc_client_matcher_ptr& matcher,
        std::shared_ptr<message_parser>& parser
        )
        : _net(net), _remote_addr(remote_addr), _parser(parser)
    {
        _matcher = matcher;
        _is_sending_next = false;
        _connect_state = SS_DISCONNECTED;
        _reconnect_count_after_last_success = 0;
        _message_sent = 0;
        _max_buffer_block_count_per_send = net.max_buffer_block_count_per_send();
    }

    // server
    rpc_session::rpc_session(
        connection_oriented_network& net, 
        ::dsn::rpc_address remote_addr,
        std::shared_ptr<message_parser>& parser
        )
        : _net(net), _remote_addr(remote_addr), _parser(parser)
    {
        _matcher = nullptr;
        _is_sending_next = false;
        _connect_state = SS_CONNECTED;
        _reconnect_count_after_last_success = 0;
        _message_sent = 0;
        _max_buffer_block_count_per_send = net.max_buffer_block_count_per_send();
    }

    void rpc_session::call(message_ex* request, rpc_response_task* call)
    {
        if (call != nullptr)
        {
            _matcher->on_call(request, call);
        }

        request->add_ref(); // released in on_send_completed
        send_message(request);
    }

    bool rpc_session::on_disconnected()
    {
        if (is_client())
        {
            // still connecting, let's retry
            if (is_connecting() && ++_reconnect_count_after_last_success < 3
                )
            {
                set_disconnected();
                connect();
                return false;
            }

            set_disconnected();
            rpc_session_ptr sp = this;
            _net.on_client_session_disconnected(sp);
        }
        
        else
        {
            rpc_session_ptr sp = this;
            _net.on_server_session_disconnected(sp);
        }
        return true;
    }

    bool rpc_session::on_recv_reply(uint64_t key, message_ex* reply, int delay_ms)
    {
        if (reply != nullptr)
        {
            reply->from_address = remote_address();
            reply->to_address = _net.address();
        }

        return _matcher->on_recv_reply(key, reply, delay_ms);
    }

    void rpc_session::on_recv_request(message_ex* msg, int delay_ms)
    {
        msg->from_address = remote_address();
        msg->from_address.c_addr_ptr()->u.v4.port = msg->header->client.port;
        msg->to_address = _net.address();

        msg->server_session = this;
        return _net.on_recv_request(msg, delay_ms);
    }
       

    ////////////////////////////////////////////////////////////////////////////////////////////////
    network::network(rpc_engine* srv, network* inner_provider)
        : _engine(srv), _parser_type(NET_HDR_DSN)
    {   
        _message_buffer_block_size = 1024 * 64;
        _max_buffer_block_count_per_send = 64; // TODO: windows, how about the other platforms?
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
        return _engine->on_recv_request(msg, delay_ms);
    }
    
    rpc_client_matcher_ptr network::get_client_matcher()
    {
        //return rpc_client_matcher_ptr(new rpc_client_matcher(_engine));
        return _engine->matcher();
    }

    std::shared_ptr<message_parser> network::new_message_parser()
    {
        message_parser * parser = utils::factory_store<message_parser>::create(_parser_type.to_string(), PROVIDER_TYPE_MAIN, _message_buffer_block_size);
        dassert(parser, "message parser '%s' not registerd or invalid!", _parser_type.to_string());
        return std::shared_ptr<message_parser>(parser);
    }

    uint32_t network::get_local_ipv4()
    {
        static const char* inteface = dsn_config_get_value_string(
            "network", "primary_interface",
            "eth0", "network interface name used to init primary ip address");

        uint32_t ip = dsn_ipv4_local(inteface);
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

    void connection_oriented_network::call(message_ex* request, rpc_response_task* call)
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
        client->call(request, call);
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

        dwarn("server session %s accepted (%d in total)", s->remote_address().to_string(), scount);
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
            dwarn("server session %s disconnected (%d in total)",
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
            dwarn("client session %s disconnected (%d in total)", s->remote_address().to_string(), scount);
        }
    }
}
