#include <sstream>
#include <vector>
#include <string>
#include <queue>
#include <iostream>
#include <dsn/internal/aio_provider.h>
#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/internal/priority_queue.h>
#include "../core/group_address.h"
#include "../core/command_manager.h"
#include "test_utils.h"
#include "../../dev/cpp/utils.cpp"
#include "../../core/service_engine.h"
#include "../../tools/hpc/hpc_tail_logger.h"

TEST(tools_hpc, tail_logger)
{
    dwarn("this is a warning");
    dinfo("this is a log %d", 1);
}

TEST(tools_hpc, tail_logger_cb)
{
    std::string output;
    bool ret = dsn::command_manager::instance().run_command("tail-log", output);
    if(!ret)
        return;

    EXPECT_TRUE(strcmp(output.c_str(), "invalid arguments for tail-log command") == 0 );

    std::ostringstream in;
    in << "tail-log 12345 4 1 " << dsn::utils::get_current_tid();
    sleep(3);
    dwarn("key:12345");
    sleep(2);
    dwarn("key:12345");
    output.clear();
    dsn::command_manager::instance().run_command(in.str().c_str(), output);
    EXPECT_TRUE(strstr(output.c_str(), "In total (1) log entries are found between") != nullptr);
    dsn::command_manager::instance().run_command("tail-log-dump", output);

    ::dsn::logging_provider* logger = ::dsn::service_engine::fast_instance().logging();
    if (logger != nullptr)
    {
        logger->flush();
    }
}
