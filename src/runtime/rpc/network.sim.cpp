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

#include <boost/asio.hpp>
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/singleton_store.h"
#include "utils/rand.h"
#include "runtime/node_scoper.h"
#include "network.sim.h"

namespace dsn {
namespace tools {

// switch[channel][header_format]
// multiple machines connect to the same switch
// 10 should be >= than rpc_channel::max_value() + 1
// 10 should be >= than network_header_format::max_value() + 1
static utils::safe_singleton_store<::dsn::rpc_address, sim_network_provider *> s_switch[10][10];

sim_client_session::sim_client_session(sim_network_provider &net,
                                       ::dsn::rpc_address remote_addr,
                                       message_parser_ptr &parser)
    : rpc_session(net, remote_addr, parser, true)
{
}

void sim_client_session::connect()
{
    if (set_connecting())
        set_connected();
}

static message_ex *virtual_send_message(message_ex *msg)
{
    std::shared_ptr<char> buffer(
        dsn::utils::make_shared_array<char>(msg->header->body_length + sizeof(message_header)));
    char *tmp = buffer.get();

    for (auto &buf : msg->buffers) {
        memcpy((void *)tmp, (const void *)buf.data(), (size_t)buf.length());
        tmp += buf.length();
    }

    blob bb(buffer, 0, msg->header->body_length + sizeof(message_header));
    message_ex *recv_msg = message_ex::create_receive_message(bb);
    recv_msg->to_address = msg->to_address;

    msg->copy_to(*recv_msg); // extensible object state move

    return recv_msg;
}

void sim_client_session::send(uint64_t sig)
{
    for (auto &msg : _sending_msgs) {
        sim_network_provider *rnet = nullptr;
        if (!s_switch[task_spec::get(msg->local_rpc_code)->rpc_call_channel][msg->hdr_format].get(
                remote_address(), rnet)) {
            LOG_ERROR("cannot find destination node %s in simulator", remote_address().to_string());
            // on_disconnected();  // disable this to avoid endless resending
        } else {
            auto server_session = rnet->get_server_session(_net.address());
            if (nullptr == server_session) {
                rpc_session_ptr cptr = this;
                message_parser_ptr parser(_net.new_message_parser(msg->hdr_format));
                server_session = new sim_server_session(*rnet, _net.address(), cptr, parser);
                rnet->on_server_session_accepted(server_session);
            }

            message_ex *recv_msg = virtual_send_message(msg);

            {
                node_scoper ns(rnet->node());

                CHECK(server_session->on_recv_message(recv_msg,
                                                      recv_msg->to_address ==
                                                              recv_msg->header->from_address
                                                          ? 0
                                                          : rnet->net_delay_milliseconds()),
                      "");
            }
        }
    }

    on_send_completed(sig);
}

sim_server_session::sim_server_session(sim_network_provider &net,
                                       ::dsn::rpc_address remote_addr,
                                       rpc_session_ptr &client,
                                       message_parser_ptr &parser)
    : rpc_session(net, remote_addr, parser, false)
{
    _client = client;
}

void sim_server_session::send(uint64_t sig)
{
    for (auto &msg : _sending_msgs) {
        message_ex *recv_msg = virtual_send_message(msg);

        {
            node_scoper ns(_client->net().node());

            CHECK(_client->on_recv_message(
                      recv_msg,
                      recv_msg->to_address == recv_msg->header->from_address
                          ? 0
                          : (static_cast<sim_network_provider *>(&_net))->net_delay_milliseconds()),
                  "");
        }
    }

    on_send_completed(sig);
}

sim_network_provider::sim_network_provider(rpc_engine *rpc, network *inner_provider)
    : connection_oriented_network(rpc, inner_provider)
{
    _address.assign_ipv4("localhost", 1);

    _min_message_delay_microseconds = 1;
    _max_message_delay_microseconds = 100000;

    _min_message_delay_microseconds =
        (uint32_t)dsn_config_get_value_uint64("tools.simulator",
                                              "min_message_delay_microseconds",
                                              _min_message_delay_microseconds,
                                              "min message delay (us)");
    _max_message_delay_microseconds =
        (uint32_t)dsn_config_get_value_uint64("tools.simulator",
                                              "max_message_delay_microseconds",
                                              _max_message_delay_microseconds,
                                              "max message delay (us)");
}

error_code sim_network_provider::start(rpc_channel channel, int port, bool client_only)
{
    CHECK(channel == RPC_CHANNEL_TCP || channel == RPC_CHANNEL_UDP,
          "invalid given channel {}",
          channel);

    _address = ::dsn::rpc_address("localhost", port);
    auto hostname = boost::asio::ip::host_name();
    if (!client_only) {
        for (int i = NET_HDR_INVALID + 1; i <= network_header_format::max_value(); i++) {
            if (s_switch[channel][i].put(_address, this)) {
                auto ep2 = ::dsn::rpc_address(hostname.c_str(), port);
                s_switch[channel][i].put(ep2, this);
            } else {
                return ERR_ADDRESS_ALREADY_USED;
            }
        }
        return ERR_OK;
    } else {
        return ERR_OK;
    }
}

uint32_t sim_network_provider::net_delay_milliseconds() const
{
    return static_cast<uint32_t>(
               rand::next_u32(_min_message_delay_microseconds, _max_message_delay_microseconds)) /
           1000;
}
}
} // end namespace
