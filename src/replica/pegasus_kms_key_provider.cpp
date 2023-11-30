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

#include "replica/pegasus_kms_key_provider.h"
#include "utils/errors.h"

namespace dsn {
namespace security {

dsn::error_s PegasusKMSKeyProvider::DecryptEncryptionKey(const std::string &encryption_key,
                                                         const std::string &iv,
                                                         const std::string &key_version,
                                                         std::string *decrypted_key)
{
    return client_.DecryptEncryptionKey(encryption_key, iv, key_version, decrypted_key);
}

dsn::error_s PegasusKMSKeyProvider::GenerateEncryptionKey(std::string *encryption_key,
                                                          std::string *iv,
                                                          std::string *key_version)
{
    return client_.GenerateEncryptionKey(encryption_key, iv, key_version);
}

} // namespace security
} // namespace dsn
