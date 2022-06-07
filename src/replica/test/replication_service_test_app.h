/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <dsn/dist/replication/replication_service_app.h>
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
