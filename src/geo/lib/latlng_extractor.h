// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>
#include <vector>
#include <s2/s2latlng.h>
#include <dsn/utility/strings.h>

namespace pegasus {
namespace geo {

class latlng_extractor
{
public:
    virtual ~latlng_extractor() = default;
    virtual const char *name() const = 0;
    virtual const char *value_sample() const = 0;
    virtual bool extract_from_value(const std::string &value, S2LatLng &latlng) const = 0;
};

class latlng_extractor_for_lbs : public latlng_extractor
{
public:
    const char *name() const final;
    const char *value_sample() const final;
    bool extract_from_value(const std::string &value, S2LatLng &latlng) const final;
};

} // namespace geo
} // namespace pegasus
