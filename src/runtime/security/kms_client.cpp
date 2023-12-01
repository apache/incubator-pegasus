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

#include <algorithm>
#include <initializer_list>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "absl/strings/escaping.h"
#include "absl/strings/substitute.h"
#include "http/http_client.h"
#include "http/http_method.h"
#include "nlohmann/json.hpp"
#include "nlohmann/json_fwd.hpp"
#include "runtime/security/kms_client.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace security {

dsn::error_s PegasusKMSClient::DecryptEncryptionKey(const std::string &encryption_key,
                                                    const std::string &iv,
                                                    const std::string &key_version,
                                                    std::string *decrypted_key)
{
    nlohmann::json post;
    post["name"] = cluster_key_name_;
    std::string iv_plain = ::absl::HexStringToBytes(iv);
    std::string iv_b64;
    ::absl::WebSafeBase64Escape(iv_plain, &iv_b64);
    post["iv"] = iv_b64;
    std::string eek_plain = ::absl::HexStringToBytes(encryption_key);
    std::string eek_b64;
    ::absl::WebSafeBase64Escape(eek_plain, &eek_b64);
    post["material"] = eek_b64;

    http_client client;
    auto err = client.init();
    if (!err.is_ok()) {
        return dsn::error_s::make(ERR_CURL_FAILED, "Start http client failed");
    }

    err = client.set_auth(http_auth_type::SPNEGO);
    if (!err.is_ok()) {
        return dsn::error_s::make(ERR_CURL_FAILED, "http client set auth type failed");
    }

    std::vector<std::string> urls;
    urls.reserve(kms_urls_.size());
    for (const auto &url : kms_urls_) {
        urls.emplace_back(
            ::absl::Substitute("$0/v1/keyversion/$1/_eek?eek_op=decrypt", url, key_version));
    }

    client.clear_header_fields();
    client.set_content_type("application/json");
    client.set_accept("*/*");
    err = client.with_post_method(post.dump());
    if (!err.is_ok()) {
        return dsn::error_s::make(ERR_CURL_FAILED, "http client set method failed");
    }

    nlohmann::json j;
    for (const auto &url : urls) {
        err = client.set_url(url);
        if (!err.is_ok()) {
            return dsn::error_s::make(ERR_CURL_FAILED, "http clientt set url failed");
        }
        std::string resp;
        err = client.exec_method(&resp);
        if (!err.is_ok()) {
            return dsn::error_s::make(ERR_CURL_FAILED, "http client exec post method failed");
        }
        long http_status;
        client.get_http_status(http_status);
        LOG_INFO("http status = ({})", http_status);
        if (http_status == 200) {
            j = nlohmann::json::parse(resp);
        }
    }

    std::string dek_b64;
    if (j.contains("material")) {
        dek_b64 = j.at("material");
    } else {
        return dsn::error_s::make(ERR_INVALID_DATA, "Null material received");
    }
    std::string dek_plain;
    if (!::absl::WebSafeBase64Unescape(dek_b64, &dek_plain)) {
        return dsn::error_s::make(ERR_INVALID_DATA, "Invalid IV received");
    }
    *decrypted_key = ::absl::BytesToHexString(dek_plain);
    return dsn::error_s::ok();
}

dsn::error_s PegasusKMSClient::GenerateEncryptionKeyFromKMS(const std::string &key_name,
                                                            std::string *encryption_key,
                                                            std::string *iv,
                                                            std::string *key_version)
{
    http_client client;
    auto err = client.init();
    if (!err.is_ok()) {
        return dsn::error_s::make(ERR_CURL_FAILED, "Start http client failed");
    }
    err = client.set_auth(http_auth_type::SPNEGO);
    if (!err.is_ok()) {
        return dsn::error_s::make(ERR_CURL_FAILED, "http client set auth type failed");
    }
    std::vector<std::string> urls;
    urls.reserve(kms_urls_.size());
    for (const auto &url : kms_urls_) {
        urls.emplace_back(
            ::absl::Substitute("$0/v1/key/$1/_eek?eek_op=generate&num_keys=1", url, key_name));
    }

    nlohmann::json j = nlohmann::json::object();
    for (const auto &url : urls) {
        err = client.set_url(url);
        if (!err.is_ok()) {
            return dsn::error_s::make(ERR_CURL_FAILED, "http client set url failed");
        }

        err = client.with_get_method();
        if (!err.is_ok()) {
            return dsn::error_s::make(ERR_CURL_FAILED, "http client set get method failed");
        }

        std::string resp;
        err = client.exec_method(&resp);
        if (!err.is_ok()) {
            return dsn::error_s::make(ERR_CURL_FAILED, "http client exec get method failed");
        }

        long http_status;
        client.get_http_status(http_status);
        LOG_INFO("http status = ({})", http_status);
        if (http_status == 200) {
            j = nlohmann::json::parse(resp);
            nlohmann::json jsonObject = j.at(0);
            std::string res = jsonObject.dump();
            j = nlohmann::json::parse(res);
        }
    }

    if (j.contains("versionName")) {
        *key_version = j.at("versionName");
    } else {
        return dsn::error_s::make(ERR_INVALID_DATA, "Null versionName received");
    }
    std::string iv_b64;
    if (j.contains("iv")) {
        iv_b64 = j.at("iv");
    } else {
        return dsn::error_s::make(ERR_INVALID_DATA, "Null IV received");
    }
    std::string iv_plain;
    if (!::absl::WebSafeBase64Unescape(iv_b64, &iv_plain)) {
        return dsn::error_s::make(ERR_INVALID_DATA, "Invalid IV received");
    }
    *iv = ::absl::BytesToHexString(iv_plain);
    std::string key_b64;
    if (j.contains("encryptedKeyVersion") && j.at("encryptedKeyVersion").contains("material")) {
        key_b64 = j.at("encryptedKeyVersion").at("material");
    } else {
        return dsn::error_s::make(ERR_INVALID_DATA,
                                  "Null encryptedKeyVersion or material received");
    }
    std::string key_plain;
    if (!::absl::WebSafeBase64Unescape(key_b64, &key_plain)) {
        return dsn::error_s::make(ERR_INVALID_DATA, "Invalid encryption key received");
    }
    *encryption_key = ::absl::BytesToHexString(key_plain);
    return dsn::error_s::ok();
}

dsn::error_s PegasusKMSClient::GenerateEncryptionKey(std::string *encryption_key,
                                                     std::string *iv,
                                                     std::string *key_version)
{
    return GenerateEncryptionKeyFromKMS(cluster_key_name_, encryption_key, iv, key_version);
}

} // namespace security
} // namespace dsn
