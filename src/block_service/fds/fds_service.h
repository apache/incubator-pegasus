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

#ifndef FDS_SERVICE_H
#define FDS_SERVICE_H

#include "block_service/block_service.h"

namespace folly {
template <typename Clock>
class BasicDynamicTokenBucket;

using DynamicTokenBucket = BasicDynamicTokenBucket<std::chrono::steady_clock>;
} // namespace folly

namespace galaxy {
namespace fds {
class GalaxyFDSClient;
}
} // namespace galaxy

namespace dsn {
namespace dist {
namespace block_service {

class fds_service : public block_filesystem
{
public:
    static const std::string FILE_MD5_KEY;
    static const std::string FILE_LENGTH_KEY;
    static const std::string FILE_LENGTH_CUSTOM_KEY;

public:
    fds_service();
    galaxy::fds::GalaxyFDSClient *get_client() { return _client.get(); }
    const std::string &get_bucket_name() { return _bucket_name; }

    virtual ~fds_service() override;
    virtual error_code initialize(const std::vector<std::string> &args) override;
    virtual dsn::task_ptr list_dir(const ls_request &req,
                                   dsn::task_code code,
                                   const ls_callback &callback,
                                   dsn::task_tracker *tracker) override;

    virtual dsn::task_ptr create_file(const create_file_request &req,
                                      dsn::task_code code,
                                      const create_file_callback &cb,
                                      dsn::task_tracker *tracker) override;

    //
    // Attention：
    //   -- remove the path directly on fds, will not enter trash
    //   -- when req.path is a directory, this operation may consume much time if there are many
    //      files under this directory
    //
    virtual dsn::task_ptr remove_path(const remove_path_request &req,
                                      dsn::task_code code,
                                      const remove_path_callback &cb,
                                      dsn::task_tracker *tracker) override;

private:
    std::shared_ptr<galaxy::fds::GalaxyFDSClient> _client;
    std::string _bucket_name;
    std::unique_ptr<folly::DynamicTokenBucket> _write_token_bucket;
    std::unique_ptr<folly::DynamicTokenBucket> _read_token_bucket;

    friend class fds_file_object;
};

class fds_file_object : public block_file
{
public:
    fds_file_object(fds_service *s, const std::string &name, const std::string &fds_path);

    virtual ~fds_file_object();
    virtual uint64_t get_size() override { return _size; }
    virtual const std::string &get_md5sum() override { return _md5sum; }

    virtual dsn::task_ptr write(const write_request &req,
                                dsn::task_code code,
                                const write_callback &cb,
                                dsn::task_tracker *tracker) override;

    virtual dsn::task_ptr read(const read_request &req,
                               dsn::task_code code,
                               const read_callback &cb,
                               dsn::task_tracker *tracker) override;

    virtual dsn::task_ptr upload(const upload_request &req,
                                 dsn::task_code code,
                                 const upload_callback &cb,
                                 dsn::task_tracker *tracker) override;

    virtual dsn::task_ptr download(const download_request &req,
                                   dsn::task_code code,
                                   const download_callback &cb,
                                   dsn::task_tracker *tracker) override;

    // Possible errors:
    // - ERR_FS_INTERNAL
    // - ERR_OBJECT_NOT_FOUND
    // - ERR_TIMEOUT
    error_code get_file_meta();

private:
    error_code get_content_in_batches(uint64_t start,
                                      int64_t length,
                                      /*out*/ std::ostream &os,
                                      /*out*/ uint64_t &transfered_bytes);
    error_code get_content(uint64_t pos,
                           uint64_t length,
                           /*out*/ std::ostream &os,
                           /*out*/ uint64_t &transfered_bytes);
    error_code put_content(/*in-out*/ std::istream &is,
                           /*int*/ int64_t to_transfer_bytes,
                           /*out*/ uint64_t &transfered_bytes);

    fds_service *_service;
    std::string _fds_path;
    std::string _md5sum;
    uint64_t _size;
    bool _has_meta_synced;

    static const size_t PIECE_SIZE = 16384; // 16k
};
} // namespace block_service
} // namespace dist
} // namespace dsn
#endif // FDS_SERVICE_H
