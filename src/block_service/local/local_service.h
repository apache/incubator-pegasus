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

#include <nlohmann/detail/macro_scope.hpp>
#include <nlohmann/json.hpp>     // IWYU pragma: keep
#include <nlohmann/json_fwd.hpp> // IWYU pragma: keep
#include <stdint.h>
#include <string>
#include <vector>

#include "block_service/block_service.h"
#include "task/task.h"
#include "task/task_code.h"
#include "utils/error_code.h"

namespace dsn {
class task_tracker;

namespace dist {
namespace block_service {

struct file_metadata
{
    int64_t size = 0;
    std::string md5;

    file_metadata(int64_t s = 0, const std::string &m = "") : size(s), md5(m) {}
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(file_metadata, size, md5);

class local_service : public block_filesystem
{
public:
    local_service();
    local_service(const std::string &root);
    virtual error_code initialize(const std::vector<std::string> &args) override;
    virtual dsn::task_ptr list_dir(const ls_request &req,
                                   dsn::task_code code,
                                   const ls_callback &callback,
                                   dsn::task_tracker *tracker = nullptr) override;

    virtual dsn::task_ptr create_file(const create_file_request &req,
                                      dsn::task_code code,
                                      const create_file_callback &cb,
                                      dsn::task_tracker *tracker = nullptr) override;

    virtual dsn::task_ptr remove_path(const remove_path_request &req,
                                      dsn::task_code code,
                                      const remove_path_callback &cb,
                                      dsn::task_tracker *tracker = nullptr) override;

    virtual ~local_service();

    static std::string get_metafile(const std::string &filepath);

private:
    std::string _root;
};

class local_file_object : public block_file
{
public:
    local_file_object(const std::string &name);

    virtual ~local_file_object();

    virtual uint64_t get_size() override;
    virtual const std::string &get_md5sum() override;

    virtual dsn::task_ptr write(const write_request &req,
                                dsn::task_code code,
                                const write_callback &cb,
                                dsn::task_tracker *tracker = nullptr) override;

    virtual dsn::task_ptr read(const read_request &req,
                               dsn::task_code code,
                               const read_callback &cb,
                               dsn::task_tracker *tracker = nullptr) override;

    virtual dsn::task_ptr upload(const upload_request &req,
                                 dsn::task_code code,
                                 const upload_callback &cb,
                                 dsn::task_tracker *tracker = nullptr) override;

    virtual dsn::task_ptr download(const download_request &req,
                                   dsn::task_code code,
                                   const download_callback &cb,
                                   dsn::task_tracker *tracker = nullptr) override;

    error_code load_metadata();

private:
    std::string compute_md5();

private:
    uint64_t _size;
    std::string _md5_value;
    bool _has_meta_synced;
};
} // namespace block_service
} // namespace dist
} // namespace dsn
