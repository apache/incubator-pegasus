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

/*
 * Description:
 *     Unit-test for command_manager.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/tool-api/command_manager.h>
#include <gtest/gtest.h>

using namespace ::dsn;

void command_manager_module_init()
{
    dsn::command_manager::instance().register_command(
        {"test-cmd"},
        "test-cmd - just for command_manager unit-test",
        "test-cmd arg1 arg2 ...",
        [](const std::vector<std::string> &args) {
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

TEST(command_manager, exist_command)
{
    const std::string cmd = "test-cmd";
    const std::vector<std::string> cmd_args{"this", "is", "test", "argument"};
    std::string output;
    dsn::command_manager::instance().run_command(cmd, cmd_args, output);

    std::string expect_output = "test-cmd response: [this is test argument]";
    ASSERT_EQ(output, expect_output);
}

TEST(command_manager, not_exist_command)
{
    const std::string cmd = "not-exist-cmd";
    const std::vector<std::string> cmd_args{"arg1", "arg2"};
    std::string output;
    dsn::command_manager::instance().run_command(cmd, cmd_args, output);

    std::string expect_output = std::string("unknown command '") + cmd + "'";
    ASSERT_EQ(output, expect_output);
}
