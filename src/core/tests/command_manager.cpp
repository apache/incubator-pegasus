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

#include <dsn/tool/cli/cli.client.h>
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

/*
TEST(core, command_manager)
{
    cli_client cli(rpc_address("localhost", 20101));

    error_code err;
    std::string result;
    command rcmd;

    rcmd.cmd = "help";
    rcmd.arguments.clear();
    result.clear();
    std::tie(err, result) = cli.call_sync(rcmd, std::chrono::seconds(10));
    ASSERT_EQ(ERR_OK, err);

    rcmd.cmd = "help";
    rcmd.arguments.clear();
    rcmd.arguments.push_back("engine");
    result.clear();
    std::tie(err, result) = cli.call_sync(rcmd, std::chrono::seconds(10));
    ASSERT_EQ(ERR_OK, err);

    rcmd.cmd = "help";
    rcmd.arguments.clear();
    rcmd.arguments.push_back("unexist-cmd");
    result.clear();
    std::tie(err, result) = cli.call_sync(rcmd, std::chrono::seconds(10));
    ASSERT_EQ(ERR_OK, err);
    ASSERT_EQ("cannot find command 'unexist-cmd'", result);

    rcmd.cmd = "engine";
    rcmd.arguments.clear();
    result.clear();
    std::tie(err, result) = cli.call_sync(rcmd, std::chrono::seconds(10));
    ASSERT_EQ(ERR_OK, err);

    rcmd.cmd = "task-code";
    rcmd.arguments.clear();
    result.clear();
    std::tie(err, result) = cli.call_sync(rcmd, std::chrono::seconds(10));
    ASSERT_EQ(ERR_OK, err);

    rcmd.cmd = "test-cmd";
    rcmd.arguments.clear();
    rcmd.arguments.push_back("this");
    rcmd.arguments.push_back("is");
    rcmd.arguments.push_back("test");
    rcmd.arguments.push_back("argument");
    result.clear();
    std::tie(err, result) = cli.call_sync(rcmd, std::chrono::seconds(10));
    ASSERT_EQ(ERR_OK, err);
    ASSERT_EQ("test-cmd response: [this is test argument]", result);

    rcmd.cmd = "unexist-cmd";
    rcmd.arguments.clear();
    rcmd.arguments.push_back("arg1");
    rcmd.arguments.push_back("arg2");
    result.clear();
    std::tie(err, result) = cli.call_sync(rcmd, std::chrono::seconds(10));
    ASSERT_EQ(ERR_OK, err);
    ASSERT_EQ("unknown command 'unexist-cmd'", result);

    rcmd.cmd = "help";
    rcmd.arguments.clear();
    rpc_address addr("localhost", 20109);
    result.clear();
    std::tie(err, result) = cli.call_sync(rcmd, std::chrono::seconds(10), 0, 0, addr);
    ASSERT_TRUE(err == ERR_TIMEOUT || err == ERR_NETWORK_FAILURE);
}
*/
