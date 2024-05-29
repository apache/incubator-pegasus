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

#include "utils/command_manager.h"

#include <boost/algorithm/string/join.hpp>
#include <fmt/core.h>

#include "gtest/gtest.h"

using std::string;
using std::vector;

namespace dsn {

class command_manager_test : public ::testing::Test
{
public:
    command_manager_test()
    {
        _cmd = command_manager::instance().register_single_command(
            "test-cmd",
            "Just for command_manager unit-test",
            "arg1 arg2 ...",
            [](const vector<string> &args) {
                return fmt::format("test-cmd response: [{}]", boost::join(args, " "));
            });
    }

private:
    std::unique_ptr<command_deregister> _cmd;
};

TEST_F(command_manager_test, exist_command)
{
    const string cmd = "test-cmd";
    const vector<string> cmd_args{"this", "is", "test", "argument"};
    string output;
    command_manager::instance().run_command(cmd, cmd_args, output);

    const string expect_output = "test-cmd response: [this is test argument]";
    ASSERT_EQ(expect_output, output);
}

TEST_F(command_manager_test, not_exist_command)
{
    const string cmd = "not-exist-cmd";
    const vector<string> cmd_args{"arg1", "arg2"};
    string output;
    command_manager::instance().run_command(cmd, cmd_args, output);

    const string expect_output = string("unknown command '") + cmd + "'";
    ASSERT_EQ(expect_output, output);
}

} // namespace dsn
