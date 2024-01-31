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

#include <string>
#include <utility>
#include <vector>

#include "security/kms_client.h"
#include "utils/errors.h"

namespace dsn {
namespace replication {
struct kms_info;
} // namespace replication

namespace security {
// This class generates EEK IV KV from KMS (a.k.a Key Management Service) and retrieves DEK from
// KMS.
class kms_key_provider
{
public:
    ~kms_key_provider() {}

    kms_key_provider(const std::vector<std::string> &kms_url, std::string cluster_key_name)
        : _client(kms_url, std::move(cluster_key_name))
    {
    }

    // Decrypt the encryption key in 'kms_info' via KMS. The 'decrypted_key' will be a hex string.
    dsn::error_s DecryptEncryptionKey(const dsn::replication::kms_info &info,
                                      std::string *decrypted_key);

    // Generate an encryption key from KMS.
    dsn::error_s GenerateEncryptionKey(dsn::replication::kms_info *info);

private:
    kms_client _client;
};
} // namespace security
} // namespace dsn
