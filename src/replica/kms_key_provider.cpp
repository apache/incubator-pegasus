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

#include <string>

#include "replica/kms_key_provider.h"
#include "utils/errors.h"

namespace dsn {
namespace security {

dsn::error_s KMSKeyProvider::DecryptEncryptionKey(const dsn::replication::kms_info &kms_info,
                                                  std::string *decrypted_key)
{
    return client_.DecryptEncryptionKey(kms_info, decrypted_key);
}

dsn::error_s KMSKeyProvider::GenerateEncryptionKey(dsn::replication::kms_info *kms_info)
{
    return client_.GenerateEncryptionKey(kms_info);
}

} // namespace security
} // namespace dsn
