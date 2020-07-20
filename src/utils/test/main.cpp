#include <gtest/gtest.h>
#include <dsn/c/api_utilities.h>
#include <dsn/tool-api/logging_provider.h>

extern void command_manager_module_init();

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);

    command_manager_module_init();
    // init logging
    dsn_log_init("dsn::tools::simple_logger", "./", nullptr);

    return RUN_ALL_TESTS();
}
