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

#include <string>

#include "task/task_tracker.h"

namespace pegasus {
class pegasus_client;

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
