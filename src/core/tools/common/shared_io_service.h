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
#pragma once

# include <boost/asio.hpp>
# include <dsn/internal/singleton.h>
# include <thread>
# include <memory>
# include <vector>
# include <dsn/tool_api.h>

namespace dsn {
    namespace tools {

        // TODO: seperate this into per-node service, so we can use
        // task::get_current_node for faster access to the nodes in all tasks
        // coz tasks may run in io-threads when [task.xxx] fast_execution_in_network_thread is true
        class shared_io_service : public utils::singleton<shared_io_service>
        {
        public:
            shared_io_service()
            {
                _io_service_worker_count = config()->get_value<int>("core", "timer_service_worker_count", 1,
                    "thread number for timer service for core itself");
                for (int i = 0; i < _io_service_worker_count; i++)
                {
                    _workers.push_back(std::shared_ptr<std::thread>(new std::thread([this]()
                    {
                        boost::asio::io_service::work work(ios);
                        ios.run();
                    })));
                }
            }

            boost::asio::io_service ios;

        private:
            int                                       _io_service_worker_count;
            std::vector<std::shared_ptr<std::thread>> _workers;
        };

    }
}
