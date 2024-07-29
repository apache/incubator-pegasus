// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <spdlog/common.h>
#include <spdlog/pattern_formatter.h>
#include <ctime>
#include <memory>
#include <string>

namespace spdlog::details {
struct log_msg;
} // namespace spdlog::details
struct tm;

// To keep the log format consistent with the existing log format, we need to print thread pool
// information in the log, spdlog use custom_flag_formatter to implement this feature. See
// https://github.com/gabime/spdlog/wiki/3.-Custom-formatting#extending-spdlog-with-your-own-flags.
class pegasus_formatter_flag : public spdlog::custom_flag_formatter
{
public:
    void
    format(const spdlog::details::log_msg &, const std::tm &, spdlog::memory_buf_t &dest) override;
    [[nodiscard]] std::unique_ptr<custom_flag_formatter> clone() const override;
};

extern void dsn_log_init(const std::string &log_dir, const std::string &role_name);
