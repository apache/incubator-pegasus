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

#include "shell/commands.h"

class command_output
{
public:
    explicit command_output(const std::string &file_name) : _file_name(file_name)
    {
        if (!file_name.empty()) {
            _file_stream = std::make_unique<std::ofstream>(_file_name);
        }
    }
    std::ostream *stream() const
    {
        if (_file_stream && !_file_stream->is_open()) {
            fmt::print(stderr, "open output file {} failed!\n", _file_name);
            return nullptr;
        }
        return _file_stream ? _file_stream.get() : &std::cout;
    }

private:
    std::string _file_name;
    std::unique_ptr<std::ofstream> _file_stream;
};
