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

#include <initializer_list>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "absl/strings/escaping.h"
#include "fmt/core.h"
#include "http/http_client.h"
#include "http/http_method.h"
#include "http/http_status_code.h"
#include "nlohmann/json.hpp"
#include "nlohmann/json_fwd.hpp"
#include "replica/replication_app_base.h"
#include "security/kms_client.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace security {

dsn::error_s KMSClient::DecryptEncryptionKey(const dsn::replication::kms_info &kms_info,
                                             std::string *decrypted_key)
{
    nlohmann::json payload;
    payload["name"] = cluster_key_name_;
    std::string iv_plain = ::absl::HexStringToBytes(kms_info.iv);
    std::string iv_b64;
    ::absl::WebSafeBase64Escape(iv_plain, &iv_b64);
    payload["iv"] = iv_b64;
    std::string eek_plain = ::absl::HexStringToBytes(kms_info.eek);
    std::string eek_b64;
    ::absl::WebSafeBase64Escape(eek_plain, &eek_b64);
    payload["material"] = eek_b64;

    http_client client;
    RETURN_NOT_OK(client.init());
    RETURN_NOT_OK(client.set_auth(http_auth_type::SPNEGO));

    std::vector<std::string> urls;
    urls.reserve(kms_urls_.size());
    for (const auto &url : kms_urls_) {
        urls.emplace_back(fmt::format("{}/v1/keyversion/{}/_eek?eek_op=decrypt", url, kms_info.kv));
    }
    client.clear_header_fields();
    client.set_content_type("application/json");
    client.set_accept("*/*");

    RETURN_NOT_OK(client.with_post_method(payload.dump()));

    nlohmann::json j;
    for (const auto &url : urls) {
        RETURN_NOT_OK(client.set_url(url));
        std::string resp;
        auto err = client.exec_method(&resp);
        if (err.code() == ERR_NETWORK_FAILURE || err.code() == ERR_TIMEOUT) {
            continue;
        }
        RETURN_NOT_OK(err);
        http_status_code http_status;
        client.get_http_status(http_status);
        if (http_status != http_status_code::kOk) {
            LOG_WARNING("The http status is ({}), and url is ({})",
                        get_http_status_message(http_status),
                        url);
            continue;
        }
        try {
            j = nlohmann::json::parse(resp);
        } catch (nlohmann::json::exception &exp) {
            LOG_ERROR("encode kms_info to json failed: {}, data = [{}]", exp.what(), resp);
        }
        break;
    }

    std::string dek_b64;
    RETURN_ERRS_NOT_TRUE(
        j.contains("material"),
        ERR_INVALID_DATA,
        "Received null material in kms json data, network may have some problems.");
    dek_b64 = j.at("material");

    std::string dek_plain;
    RETURN_ERRS_NOT_TRUE(::absl::WebSafeBase64Unescape(dek_b64, &dek_plain),
                         ERR_INVALID_DATA,
                         "Decryption key base64 decoding failed.");

    *decrypted_key = ::absl::BytesToHexString(dek_plain);
    return dsn::error_s::ok();
}

dsn::error_s KMSClient::GenerateEncryptionKeyFromKMS(const std::string &key_name,
                                                     dsn::replication::kms_info *kms_info)
{
    http_client client;
    RETURN_NOT_OK(client.init());
    RETURN_NOT_OK(client.set_auth(http_auth_type::SPNEGO));

    std::vector<std::string> urls;
    urls.reserve(kms_urls_.size());
    for (const auto &url : kms_urls_) {
        urls.emplace_back(
            fmt::format("{}/v1/key/{}/_eek?eek_op=generate&num_keys=1", url, key_name));
    }

    nlohmann::json j = nlohmann::json::object();
    for (const auto &url : urls) {
        RETURN_NOT_OK(client.set_url(url));
        RETURN_NOT_OK(client.with_get_method());
        std::string resp;
        auto err = client.exec_method(&resp);
        if (err.code() == ERR_NETWORK_FAILURE || err.code() == ERR_TIMEOUT) {
            continue;
        }
        RETURN_NOT_OK(err);
        http_status_code http_status;
        client.get_http_status(http_status);
        if (http_status != http_status_code::kOk) {
            LOG_WARNING("The http status is ({}), and url is ({})",
                        get_http_status_message(http_status),
                        url);
            continue;
        }
        try {
            j = nlohmann::json::parse(resp).at(0);
        } catch (nlohmann::json::exception &exp) {
            LOG_ERROR("encode kms_info to json failed: {}, data = [{}]", exp.what(), resp);
        }
        break;
    }

    RETURN_ERRS_NOT_TRUE(
        !j["versionName"].is_null(),
        ERR_INVALID_DATA,
        "Received null versionName in kms json data, network may have some problems.");
    j["versionName"].get_to(kms_info->kv);

    std::string iv_b64;
    RETURN_ERRS_NOT_TRUE(!j["iv"].is_null(),
                         ERR_INVALID_DATA,
                         "Received null IV in kms json data, network may have some problems.");
    j["iv"].get_to(iv_b64);

    std::string iv_plain;
    RETURN_ERRS_NOT_TRUE(::absl::WebSafeBase64Unescape(iv_b64, &iv_plain),
                         ERR_INVALID_DATA,
                         "IV base64 decoding failed.");
    kms_info->iv = ::absl::BytesToHexString(iv_plain);

    std::string key_b64;
    RETURN_ERRS_NOT_TRUE(
        !j["encryptedKeyVersion"].is_null(),
        ERR_INVALID_DATA,
        "Received null encryptedKeyVersion in kms json data, network may have some problems.");
    RETURN_ERRS_NOT_TRUE(!j["encryptedKeyVersion"]["material"].is_null(),
                         ERR_INVALID_DATA,
                         "Received null material of encryptedKeyVersion in kms json data, network "
                         "may have some problems.");
    j["encryptedKeyVersion"]["material"].get_to(key_b64);

    std::string key_plain;
    RETURN_ERRS_NOT_TRUE(::absl::WebSafeBase64Unescape(key_b64, &key_plain),
                         ERR_INVALID_DATA,
                         "Encryption key base64 decoding failed.");
    kms_info->eek = ::absl::BytesToHexString(key_plain);
    return dsn::error_s::ok();
}

dsn::error_s KMSClient::GenerateEncryptionKey(dsn::replication::kms_info *kms_info)
{
    return GenerateEncryptionKeyFromKMS(cluster_key_name_, kms_info);
}

} // namespace security
} // namespace dsn
