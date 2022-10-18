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

#pragma once

#include "replica/replication_service_app.h"
using ::dsn::replication::replication_service_app;
using ::dsn::error_code;

class replication_service_test_app : public replication_service_app
{
public:
    replication_service_test_app(const dsn::service_app_info *info) : replication_service_app(info)
    {
    }
    virtual error_code start(const std::vector<std::string> &args) override;
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
