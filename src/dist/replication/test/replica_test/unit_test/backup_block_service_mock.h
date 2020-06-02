// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <iostream>
#include <dsn/utility/filesystem.h>
#include <dsn/tool-api/auto_codes.h>
#include <dsn/tool-api/task_tracker.h>
#include <dsn/dist/block_service.h>
#include "dist/replication/lib/replica_context.h"
#include "dist/replication/test/replica_test/unit_test/replication_service_test_app.h"
#include "dist/block_service/test/block_service_mock.h"

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
