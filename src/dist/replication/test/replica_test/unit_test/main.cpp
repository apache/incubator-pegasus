#include <cmath>
#include <fstream>
#include <iostream>
#include <thread>

#include <gtest/gtest.h>
#include <dsn/dist/replication/replication_service_app.h>

#include "replication_service_test_app.h"

int gtest_flags = 0;
int gtest_ret = 0;
replication_service_test_app *app;

TEST(cold_backup_context, check_backup_on_remote) { app->check_backup_on_remote_test(); }

TEST(cold_backup_context, read_current_chkpt_file) { app->read_current_chkpt_file_test(); }

TEST(cold_backup_context, remote_chkpt_dir_exist) { app->remote_chkpt_dir_exist_test(); }

TEST(cold_backup_context, upload_checkpoint_to_remote) { app->upload_checkpoint_to_remote_test(); }

TEST(cold_backup_context, read_backup_metadata) { app->read_backup_metadata_test(); }

TEST(cold_backup_context, on_upload_chkpt_dir) { app->on_upload_chkpt_dir_test(); }

TEST(cold_backup_context, write_metadata_file) { app->write_backup_metadata_test(); }

TEST(cold_backup_context, write_current_chkpt_file) { app->write_current_chkpt_file_test(); }

error_code replication_service_test_app::start(const std::vector<std::string> &args)
{
    int argc = args.size();
    char *argv[20];
    for (int i = 0; i < argc; ++i) {
        argv[i] = (char *)(args[i].c_str());
    }

    testing::InitGoogleTest(&argc, argv);
    app = this;
    gtest_ret = RUN_ALL_TESTS();
    gtest_flags = 1;
    return dsn::ERR_OK;
}

GTEST_API_ int main(int argc, char **argv)
{
    dsn::service_app::register_factory<replication_service_test_app>("replica");
    if (argc < 2)
        dassert(dsn_run_config("config-test.ini", false), "");
    else
        dassert(dsn_run_config(argv[1], false), "");

    while (gtest_flags == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

#ifndef ENABLE_GCOV
    dsn_exit(gtest_ret);
#endif
    return gtest_ret;
}
