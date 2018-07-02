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

#include "asio_rpc_session.h"

namespace dsn {
namespace tools {

asio_rpc_session::~asio_rpc_session() {}

void asio_rpc_session::set_options()
{
    if (_socket->is_open()) {
        boost::system::error_code ec;
        boost::asio::socket_base::send_buffer_size option, option2(16 * 1024 * 1024);
        _socket->get_option(option, ec);
        if (ec)
            dwarn("asio socket get option failed, error = %s", ec.message().c_str());
        int old = option.value();
        _socket->set_option(option2, ec);
        if (ec)
            dwarn("asio socket set option failed, error = %s", ec.message().c_str());
        _socket->get_option(option, ec);
        if (ec)
            dwarn("asio socket get option failed, error = %s", ec.message().c_str());
        dinfo("boost asio send buffer size is %u, set as 16MB, now is %u", old, option.value());

        boost::asio::socket_base::receive_buffer_size option3, option4(16 * 1024 * 1024);
        _socket->get_option(option3, ec);
        if (ec)
            dwarn("asio socket get option failed, error = %s", ec.message().c_str());
        old = option3.value();
        _socket->set_option(option4, ec);
        if (ec)
            dwarn("asio socket set option failed, error = %s", ec.message().c_str());
        _socket->get_option(option3, ec);
        if (ec)
            dwarn("asio socket get option failed, error = %s", ec.message().c_str());
        dinfo("boost asio recv buffer size is %u, set as 16MB, now is %u", old, option.value());

        // Nagle algorithm may cause an extra delay in some cases, because if
        // the data in a single write spans 2n packets, the last packet will be
        // withheld, waiting for the ACK for the previous packet. For more, please
        // refer to <https://en.wikipedia.org/wiki/Nagle's_algorithm>.
        //
        // Disabling the Nagle algorithm would cause these affacts:
        //   * decrease delay time (positive affact)
        //   * decrease the qps (negative affact)
        _socket->set_option(boost::asio::ip::tcp::no_delay(true), ec);
        if (ec)
            dwarn("asio socket set option failed, error = %s", ec.message().c_str());
        dinfo("boost asio set no_delay = true");
    }
}

void asio_rpc_session::do_read(int read_next)
{
    add_ref();

    void *ptr = _reader.read_buffer_ptr(read_next);
    int remaining = _reader.read_buffer_capacity();

    _socket->async_read_some(
        boost::asio::buffer(ptr, remaining),
        [this](boost::system::error_code ec, std::size_t length) {
            if (!!ec) {
                if (ec == boost::asio::error::make_error_code(boost::asio::error::eof)) {
                    ddebug("asio read from %s failed: %s",
                           _remote_addr.to_string(),
                           ec.message().c_str());
                } else {
                    derror("asio read from %s failed: %s",
                           _remote_addr.to_string(),
                           ec.message().c_str());
                }
                on_failure();
            } else {
                _reader.mark_read(length);

                int read_next = -1;

                if (!_parser) {
                    read_next = prepare_parser();
                }

                if (_parser) {
                    message_ex *msg = _parser->get_message_on_receive(&_reader, read_next);

                    while (msg != nullptr) {
                        this->on_message_read(msg);
                        msg = _parser->get_message_on_receive(&_reader, read_next);
                    }
                }

                if (read_next == -1) {
                    derror("asio read from %s failed", _remote_addr.to_string());
                    on_failure();
                } else {
                    start_read_next(read_next);
                }
            }

            release_ref();
        });
}

void asio_rpc_session::write(uint64_t signature)
{
    std::vector<boost::asio::const_buffer> buffers2;
    int bcount = (int)_sending_buffers.size();

    // prepare buffers
    buffers2.resize(bcount);
    for (int i = 0; i < bcount; i++) {
        buffers2[i] = boost::asio::const_buffer(_sending_buffers[i].buf, _sending_buffers[i].sz);
    }

    add_ref();
    boost::asio::async_write(
        *_socket, buffers2, [this, signature](boost::system::error_code ec, std::size_t length) {
            if (!!ec) {
                derror(
                    "asio write to %s failed: %s", _remote_addr.to_string(), ec.message().c_str());
                on_failure(true);
            } else {
                on_send_completed(signature);
            }

            release_ref();
        });
}

asio_rpc_session::asio_rpc_session(asio_network_provider &net,
                                   ::dsn::rpc_address remote_addr,
                                   std::shared_ptr<boost::asio::ip::tcp::socket> &socket,
                                   message_parser_ptr &parser,
                                   bool is_client)
    : rpc_session(net, remote_addr, parser, is_client), _socket(socket)
{
    set_options();
}

void asio_rpc_session::on_failure(bool is_write)
{
    if (on_disconnected(is_write)) {
        safe_close();
    }
}

void asio_rpc_session::safe_close()
{
    boost::system::error_code ec;
    _socket->shutdown(boost::asio::socket_base::shutdown_type::shutdown_both, ec);
    if (ec)
        dwarn("asio socket shutdown failed, error = %s", ec.message().c_str());
    _socket->close(ec);
    if (ec)
        dwarn("asio socket close failed, error = %s", ec.message().c_str());
}

void asio_rpc_session::connect()
{
    if (set_connecting()) {
        boost::asio::ip::tcp::endpoint ep(boost::asio::ip::address_v4(_remote_addr.ip()),
                                          _remote_addr.port());

        add_ref();
        _socket->async_connect(ep, [this](boost::system::error_code ec) {
            if (!ec) {
                dinfo("client session %s connected", _remote_addr.to_string());

                set_options();
                set_connected();
                on_send_completed();
                start_read_next();
            } else {
                derror("client session connect to %s failed, error = %s",
                       _remote_addr.to_string(),
                       ec.message().c_str());
                on_failure(true);
            }
            release_ref();
        });
    }
}
}
}
