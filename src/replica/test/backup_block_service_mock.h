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

#pragma once

#include <iostream>
#include "utils/filesystem.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/task/task_tracker.h"
#include "block_service/block_service.h"
#include "replica/replica_context.h"
#include "replication_service_test_app.h"
#include "block_service/test/block_service_mock.h"
#include "common/backup_common.h"

using namespace ::dsn;
using namespace ::dsn::dist::block_service;
using namespace ::dsn::replication;

extern ref_ptr<block_file_mock> current_chkpt_file;
extern ref_ptr<block_file_mock> backup_metadata_file;
extern ref_ptr<block_file_mock> regular_file;

class backup_block_service_mock : public block_service_mock
{
public:
    virtual dsn::task_ptr create_file(const create_file_request &req,
                                      dsn::task_code code,
                                      const create_file_callback &cb,
                                      dsn::task_tracker *tracker = nullptr)
    {
        create_file_response resp;
        if (enable_create_file_fail) {
            resp.err = ERR_MOCK_INTERNAL;
        } else {
            resp.err = ERR_OK;
            auto it = files.find(req.file_name);
            if (it != files.end()) {
                resp.file_handle =
                    new block_file_mock(req.file_name, it->second.first, it->second.second);
            } else {
                std::string filename = ::dsn::utils::filesystem::get_file_name(req.file_name);
                if (filename == cold_backup_constant::CURRENT_CHECKPOINT) {
                    resp.file_handle = current_chkpt_file;
                    std::cout << "current_ckpt_file is selected..." << std::endl;
                } else if (filename == cold_backup_constant::BACKUP_METADATA) {
                    resp.file_handle = backup_metadata_file;
                    std::cout << "backup_metadata_file is selected..." << std::endl;
                } else {
                    resp.file_handle = regular_file;
                    std::cout << "regular_file is selected..." << std::endl;
                }
            }
        }

        cb(resp);
        return task_ptr();
    }
};
