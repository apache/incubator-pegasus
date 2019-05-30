// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/tool-api/task_code.h>
#include <dsn/tool-api/task_tracker.h>
#include <dsn/tool-api/task_queue.h>
#include <dsn/tool-api/async_calls.h>
#include <pegasus/client.h>

namespace pegasus {
namespace server {

class result_writer
{
public:
    explicit result_writer(pegasus_client *client);

    // The default try_count is 300.
    // If set failed, keep on retrying at one minute interval until
    // set succeed or the number of tries has reached 'try_count'.
    void set_result(const std::string &hash_key,
                    const std::string &sort_key,
                    const std::string &value,
                    int try_count = 300);

private:
    dsn::task_tracker _tracker;
    // client to access server.
    pegasus_client *_client;
};
} // namespace server
} // namespace pegasus
