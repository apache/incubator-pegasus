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

#include "utils/command_manager.h"

#include <gtest/gtest.h>

using std::string;
using std::vector;

namespace dsn {

class command_manager_test : public ::testing::Test
{
public:
    command_manager_test()
    {
        _get_allow_list = command_manager::instance().register_command(
            {"test-cmd"},
            "test-cmd - just for command_manager unit-test",
            "test-cmd arg1 arg2 ...",
            [](const vector<string> &args) {
                std::stringstream ss;
                ss << "test-cmd response: [";
                for (size_t i = 0; i < args.size(); ++i) {
                    if (i != 0)
                        ss << " ";
                    ss << args[i];
                }
                ss << "]";
                return ss.str();
            });
    }

private:
    std::unique_ptr<command_deregister> _get_allow_list;
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