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
        dlink* msg = nullptr;

        if (_sending_msgs)
        {
            auto hdr = &_sending_msgs->dl;
            msg = hdr;
            do
            {
                // added in rpc_engine::reply (for server) or rpc_session::call (for client)
                auto rmsg = CONTAINING_RECORD(msg, message_ex, dl);
                msg = msg->next();
                rmsg->dl.remove();                
                rmsg->release_ref();
            } while (msg != hdr);

            _sending_msgs = nullptr;
        }

        while (true)
        {
            {
                utils::auto_lock<utils::ex_lock_nr> l(_lock);
                msg = _messages.next();
                if (msg == &_messages)
                    break;

                msg->remove();
            }
            // added in rpc_engine::reply (for server) or rpc_session::call (for client)
            auto rmsg = CONTAINING_RECORD(msg, message_ex, dl);
            rmsg->release_ref();
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

        dwarn("client session connected to %s:%hu",
            remote_address().name(),
            remote_address().port()
            );
    }

    void rpc_session::set_disconnected()
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        _connect_state = SS_DISCONNECTED;
    }

    inline message_ex* rpc_session::unlink_message_for_send()
    {
        auto n = _messages.next();
        auto msg = CONTAINING_RECORD(n, message_ex, dl);        
        int bcount = 0;
        int tlen = 0;
        dlink* last_msg = nullptr;

        _sending_buffers.clear();

        do
        {
            auto lmsg = CONTAINING_RECORD(n, message_ex, dl);            
            auto lcount = _parser->get_send_buffers_count_and_total_length(lmsg, &tlen);
            if (bcount > 0 && bcount + lcount > _max_buffer_block_count_per_send)
            {
                last_msg = n;
                break;
            }   

            _sending_buffers.resize(bcount + lcount);
            _parser->prepare_buffers_on_send(lmsg, 0, &_sending_buffers[bcount]);
            bcount += lcount;

            n = n->next();
        } while (n != &_messages);

        // all pending messages are included for sending
        if (nullptr == last_msg)
        {
            _messages.remove();
        }

        // part of the message are included
        else
        {
            auto rmsg = _messages.range_remove(last_msg->prev());
            dassert(rmsg == &msg->dl, "must return the next sending msg");
        }

        return msg;
    }
    
    void rpc_session::send_message(message_ex* msg)
    {
        {
            utils::auto_lock<utils::ex_lock_nr> l(_lock);
            msg->dl.insert_before(&_messages);
            if (SS_CONNECTED == _connect_state && !_is_sending_next)
            {
                _is_sending_next = true;
                _sending_msgs = unlink_message_for_send();
            }
            else
            {
                return;
            }
        }

        // send msgs (double-linked list)
        this->send(_sending_msgs);
    }
    
    void rpc_session::on_send_completed(message_ex* msg)
    {
        message_ex* next_msg;
        {
            utils::auto_lock<utils::ex_lock_nr> l(_lock);
            if (nullptr != msg)
            {
                dassert(_is_sending_next && msg == _sending_msgs,
                    "sent msg must be sending");
                _is_sending_next = false; 
                _sending_msgs = nullptr;
            }
            
            if (!_messages.is_alone() && !_is_sending_next)
            {
                _is_sending_next = true;
                next_msg = _sending_msgs = unlink_message_for_send();
            }
            else
            {
                next_msg = nullptr;
            }
        }

        // for next send messages (double-linked list)
        if (next_msg)
            this->send(next_msg);

        // for old msgs
        // added in rpc_engine::reply (for server) or rpc_session::call (for client)
        if (msg)
        {
            while (true)
            {
                /*dinfo("msg %s for rpc %llx is now sent",
                task_spec::get(msg->local_rpc_code)->name.c_str(),
                msg->header->rpc_id
                );*/
                _message_sent++;

                if (msg->dl.is_alone())
                {
                    msg->release_ref();
                    break;
                }

                auto msg2 = CONTAINING_RECORD(msg->dl.next(), message_ex, dl);
                msg->dl.remove();
                msg->release_ref();
                msg = msg2;
            }
        }
    }

    bool rpc_session::has_pending_out_msgs()
    {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        return !_messages.is_alone();
    }

    // client
    rpc_session::rpc_session(
        connection_oriented_network& net,
        const ::dsn::rpc_address& remote_addr,
        rpc_client_matcher_ptr& matcher,
        std::shared_ptr<message_parser>& parser
        )
        : _net(net), _remote_addr(remote_addr), _parser(parser)
    {
        _matcher = matcher;
        _sending_msgs = nullptr;
        _is_sending_next = false;
        _connect_state = SS_DISCONNECTED;
        _reconnect_count_after_last_success = 0;
        _message_sent = 0;
        _max_buffer_block_count_per_send = net.max_buffer_block_count_per_send();
    }

    // server
    rpc_session::rpc_session(
        connection_oriented_network& net, 
        const ::dsn::rpc_address& remote_addr,
        std::shared_ptr<message_parser>& parser
        )
        : _net(net), _remote_addr(remote_addr), _parser(parser)
    {
        _matcher = nullptr;
        _sending_msgs = nullptr;
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
        msg->from_address.c_addr_ptr()->port = msg->header->client.port;
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

    connection_oriented_network::connection_oriented_network(rpc_engine* srv, network* inner_provider)
        : network(srv, inner_provider)
    {
    }

    void connection_oriented_network::call(const rpc_address& to, message_ex* request, rpc_response_task* call)
    {
        rpc_session_ptr client = nullptr;
        bool new_client = false;

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

    rpc_session_ptr connection_oriented_network::get_server_session(const ::dsn::rpc_address& ep)
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
                dwarn("server session on %s:%hu already exists, preempted",
                    s->remote_address().name(),
                    s->remote_address().port()
                    );
            }
            scount = (int)_servers.size();
        }

        dwarn("server session %s:%hu accepted (%d in total)", 
            s->remote_address().name(), 
            s->remote_address().port(),
            scount
            );
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
            dwarn("server session %s:%hu disconnected (%d in total)",
                s->remote_address().name(),
                s->remote_address().port(),
                scount
                );
        }
    }

    rpc_session_ptr connection_oriented_network::get_client_session(const ::dsn::rpc_address& ep)
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
            dwarn("client session %s:%hu disconnected (%d in total)", s->remote_address().name(),
                s->remote_address().port(), scount
                );
        }
    }
}
