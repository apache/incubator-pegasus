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

namespace cpp dsn.utils
namespace go utils
namespace java org.apache.pegasus.utils

// How a string matches to a given pattern.
enum pattern_match_type
{
    PMT_INVALID = 0,

    // The string always matches no matter what the given pattern is.
    PMT_MATCH_ALL,

    // The string must exactly equal to the given pattern.
    PMT_MATCH_EXACT,

    // The string must appear anywhere in the given pattern.
    PMT_MATCH_ANYWHERE,

    // The string must start with the given pattern.
    PMT_MATCH_PREFIX,

    // The string must end with the given pattern.
    PMT_MATCH_POSTFIX,

    // The string must match the given pattern as a regular expression.
    PMT_MATCH_REGEX,
}
