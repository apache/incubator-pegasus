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

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "utils/errors.h"

namespace dsn {
namespace replication {
struct kms_info;
} // namespace replication

namespace security {
// A class designed to generate an encryption key from KMS for file writing,
// implemented using an HTTP client.
// This class is not thread-safe. Thus maintain one instance for each thread.
class kms_client
{
public:
    kms_client(const std::vector<std::string> &kms_url, std::string cluster_key_name)
        : _kms_urls(kms_url), _cluster_key_name(std::move(cluster_key_name))
    {
    }

    // Retrieve the Decrypted Encryption Key (DEK) from KMS after generating the EEK, IV, and KV.
    dsn::error_s DecryptEncryptionKey(const dsn::replication::kms_info &info,
                                      std::string *decrypted_key);

    // Generated the EEK, IV, KV from KMS.
    dsn::error_s GenerateEncryptionKey(dsn::replication::kms_info *info);

private:
    dsn::error_s GenerateEncryptionKeyFromKMS(const std::string &key_name,
                                              dsn::replication::kms_info *info);

    std::vector<std::string> _kms_urls;
    std::string _cluster_key_name;
};
} // namespace security
} // namespace dsn
