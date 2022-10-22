// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstddef>

namespace dsn {
namespace strings_internal {

// This is significantly faster for case-sensitive matches with very
// few possible matches.  See unit test for benchmarks.
const char *memmatch(const char *phaystack, size_t haylen, const char *pneedle, size_t neelen);

} // namespace strings_internal
} // namespace dsn
