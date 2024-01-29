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

#include "uri_decoder.h"

#include <stddef.h>

#include "fmt/core.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace uri {

error_with<char> from_hex(const char c)
{
    switch (c) {
    case '0' ... '9':
        return c - '0';
    case 'a' ... 'f':
        return c - 'a' + 10;
    case 'A' ... 'F':
        return c - 'A' + 10;
    default:
        return error_s::make(ERR_INVALID_PARAMETERS);
    }
}

error_with<char> decode_char(const absl::string_view &hex)
{
    CHECK_EQ(2, hex.size());

    auto high = from_hex(hex[0]);
    auto low = from_hex(hex[1]);
    if (high.is_ok() && low.is_ok()) {
        return (high.get_value() << 4) | low.get_value();
    }

    return error_s::make(ERR_INVALID_PARAMETERS);
}

error_with<std::string> decode(const absl::string_view &encoded_uri)
{
    std::string decoded_uri;
    for (size_t i = 0; i < encoded_uri.size(); ++i) {
        // '%' is followed by 2 hex chars
        if ('%' == encoded_uri[i]) {
            if (i + 2 >= encoded_uri.size()) {
                return error_s::make(ERR_INVALID_PARAMETERS,
                                     "Encountered partial escape sequence at end of string");
            }

            const absl::string_view encoded_char(encoded_uri.data() + i + 1, 2);
            auto decoded_char = decode_char(encoded_char);
            if (!decoded_char.is_ok()) {
                return error_s::make(
                    ERR_INVALID_PARAMETERS,
                    fmt::format("The characters {} do not "
                                "form a hex value. Please escape it or pass a valid hex value",
                                encoded_char.data()));
            }
            decoded_uri += decoded_char.get_value();
            i += 2;
        } else {
            decoded_uri += encoded_uri[i];
        }
    }

    return decoded_uri;
}

} // namespace uri
} // namespace dsn
