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

#include "block_service/block_service.h"
#include <hdfs/hdfs.h>

namespace folly {
template <typename Clock>
class BasicDynamicTokenBucket;

using DynamicTokenBucket = BasicDynamicTokenBucket<std::chrono::steady_clock>;
}

namespace dsn {
namespace dist {
namespace block_service {

class hdfs_service : public block_filesystem
{
public:
    hdfs_service();
    error_code create_fs();
    hdfsFS get_fs() { return _fs; }

    ~hdfs_service();
    error_code initialize(const std::vector<std::string> &args) override;
    dsn::task_ptr list_dir(const ls_request &req,
                           dsn::task_code code,
                           const ls_callback &cb,
                           dsn::task_tracker *tracker) override;
    dsn::task_ptr create_file(const create_file_request &req,
                              dsn::task_code code,
                              const create_file_callback &cb,
                              dsn::task_tracker *tracker) override;
    dsn::task_ptr remove_path(const remove_path_request &req,
                              dsn::task_code code,
                              const remove_path_callback &cb,
                              dsn::task_tracker *tracker) override;
    void close();

    static std::string get_hdfs_entry_name(const std::string &hdfs_path);

    bool is_root_path_set() const override { return _hdfs_path != "/"; }

private:
    hdfsFS _fs;
    std::string _hdfs_name_node;
    std::string _hdfs_path;

    std::unique_ptr<folly::DynamicTokenBucket> _read_token_bucket;
    std::unique_ptr<folly::DynamicTokenBucket> _write_token_bucket;

    friend class hdfs_file_object;
};

class hdfs_file_object : public block_file
{
public:
    hdfs_file_object(hdfs_service *s, const std::string &name);
    ~hdfs_file_object();
    uint64_t get_size() override { return _size; }
    const std::string &get_md5sum() override { return _md5sum; }
    dsn::task_ptr write(const write_request &req,
                        dsn::task_code code,
                        const write_callback &cb,
                        dsn::task_tracker *tracker) override;
    dsn::task_ptr read(const read_request &req,
                       dsn::task_code code,
                       const read_callback &cb,
                       dsn::task_tracker *tracker) override;
    dsn::task_ptr upload(const upload_request &req,
                         dsn::task_code code,
                         const upload_callback &cb,
                         dsn::task_tracker *tracker) override;
    dsn::task_ptr download(const download_request &req,
                           dsn::task_code code,
                           const download_callback &cb,
                           dsn::task_tracker *tracker) override;
    error_code get_file_meta();

private:
    error_code
    write_data_in_batches(const char *data, const uint64_t data_size, uint64_t &written_size);
    error_code read_data_in_batches(uint64_t start_pos,
                                    int64_t length,
                                    std::string &read_buffer,
                                    size_t &read_length);

    hdfs_service *_service;
    std::string _md5sum;
    uint64_t _size;
    bool _has_meta_synced;
};
} // namespace block_service
} // namespace dist
} // namespace dsn
