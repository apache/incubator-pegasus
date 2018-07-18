// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <boost/asio.hpp>
#include <thread>
#include <memory>
#include <vector>
#include <dsn/utility/singleton.h>

namespace pegasus {
namespace server {

// TODO: seperate this into per-node service, so we can use
// task::get_current_node for faster access to the nodes in all tasks
// coz tasks may run in io-threads when [task.xxx] allow_inline is true
class pegasus_io_service : public ::dsn::utils::singleton<pegasus_io_service>
{
public:
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

    boost::asio::io_service ios;

private:
    int _io_service_worker_count;
    std::vector<std::shared_ptr<std::thread>> _workers;
};
}
} // namespace
