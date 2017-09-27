// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

namespace pegasus {

#define PEGASUS_ERR_CODE(x, y, z) static const int x = y
#include <pegasus/error_def.h>
#undef PEGASUS_ERR_CODE

} // namespace
