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

    ~result_writer();

    // set try_count to 300 (keep on retrying at one minute interval) to avoid losing result
    // if the result table is also unavailable for a long time.
    void set_result(const std::string &hash_key,
                    const std::string &sort_key,
                    const std::string &value,
                    int try_count = 300);

private:
    dsn::task_tracker _tracker;
    // client to access server.
    pegasus_client *_client;
};
}
}
