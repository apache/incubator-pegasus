#pragma once

#include <dsn/dist/replication/replication_service_app.h>
using ::dsn::replication::replication_service_app;
using ::dsn::error_code;

class replication_service_test_app : public replication_service_app
{
public:
    replication_service_test_app(dsn_gpid pid) : replication_service_app(pid) {}
    virtual error_code start(int argc, char **argv) override;
    virtual dsn::error_code stop(bool /*cleanup*/) { return dsn::ERR_OK; }

    // test for cold_backup_context
    void check_backup_on_remote_test();
    void read_current_chkpt_file_test();
    void remote_chkpt_dir_exist_test();

    void upload_checkpoint_to_remote_test();
    void read_backup_metadata_test();
    void on_upload_chkpt_dir_test();
    void write_backup_metadata_test();
    void write_current_chkpt_file_test();
};
