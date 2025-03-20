
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

// https://github.com/abseil/abseil-cpp/issues/1708

#ifdef __SANITIZE_ADDRESS__
#define ORIGINAL_SANITIZE_ADDRESS __SANITIZE_ADDRESS__
#undef __SANITIZE_ADDRESS__
#endif

#include <absl/base/config.h>

#ifdef ORIGINAL_SANITIZE_ADDRESS
#undef __SANITIZE_ADDRESS__
#define __SANITIZE_ADDRESS__ ORIGINAL_SANITIZE_ADDRESS
#undef ORIGINAL_SANITIZE_ADDRESS
#endif
