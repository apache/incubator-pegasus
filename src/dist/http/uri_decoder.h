// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>
#include <dsn/utility/errors.h>

namespace dsn {
namespace uri {

/// \brief Decodes a sequence according to the percent decoding rules.
/// \returns the decoded uri path
error_with<std::string> decode(const string_view &encoded_uri);

} // namespace uri
} // namespace dsn
