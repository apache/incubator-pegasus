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

#pragma once

#include <boost/asio.hpp>
#include <thread>
#include <memory>
#include <vector>
#include "utils/singleton.h"

namespace pegasus {
namespace server {

// TODO: seperate this into per-node service, so we can use
// task::get_current_node for faster access to the nodes in all tasks
// coz tasks may run in io-threads when [task.xxx] allow_inline is true
class pegasus_io_service : public dsn::utils::singleton<pegasus_io_service>
{
public:
    boost::asio::io_service ios;

private:
    pegasus_io_service()
    {
        _io_service_worker_count = 2;
        for (int i = 0; i < _io_service_worker_count; i++) {
            _workers.push_back(std::shared_ptr<std::thread>(new std::thread([this]() {
                boost::asio::io_service::work work(ios);
                ios.run();
            })));
        }
    }

    ~pegasus_io_service()
    {
        ios.stop();
        for (auto worker : _workers) {
            worker->join();
        }
    }

    int _io_service_worker_count;
    std::vector<std::shared_ptr<std::thread>> _workers;

    friend class dsn::utils::singleton<pegasus_io_service>;
};
} // namespace server
} // namespace pegasus
