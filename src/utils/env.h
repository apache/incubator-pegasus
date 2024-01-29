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

#include <rocksdb/env.h>
#include <rocksdb/status.h>
#include <cstddef>
#include <cstdint>
#include <string>

namespace dsn {
namespace utils {

// Indicate whether the file is sensitive or not.
// Only the sensitive file will be encrypted if FLAGS_encrypt_data_at_rest
// is enabled at the same time.
enum class FileDataType
{
    kSensitive = 0,
    kNonSensitive = 1
};

static const size_t kEncryptionHeaderkSize = rocksdb::kDefaultPageSize;

// Get the rocksdb::Env instance for the given file type.
rocksdb::Env *PegasusEnv(FileDataType type);

// Encrypt the original non-encrypted 'src_fname' to 'dst_fname'.
// The 'total_size' is the total size of the file content, exclude the file encryption header
// (typically 4KB).
rocksdb::Status encrypt_file(const std::string &src_fname,
                             const std::string &dst_fname,
                             uint64_t *total_size = nullptr);

// Similar to the above, but encrypt the file in the same path.
rocksdb::Status encrypt_file(const std::string &fname, uint64_t *total_size = nullptr);

// Copy the original 'src_fname' to 'dst_fname'.
// Both 'src_fname' and 'dst_fname' are sensitive files.
rocksdb::Status copy_file(const std::string &src_fname,
                          const std::string &dst_fname,
                          uint64_t *total_size = nullptr);

// Similar to the above, but copy the file by a limited size.
// Both 'src_fname' and 'dst_fname' are sensitive files, 'limit_size' is the max size of the
// file to copy, and -1 means no limit.
rocksdb::Status copy_file_by_size(const std::string &src_fname,
                                  const std::string &dst_fname,
                                  int64_t limit_size = -1);
} // namespace utils
} // namespace dsn
