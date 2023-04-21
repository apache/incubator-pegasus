/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

include "../../idl/dsn.thrift"

namespace cpp dsn.apps

struct duplicate_request
{
    1: list<duplicate_entry> entries
}

struct duplicate_entry
{
    // The timestamp of this write.
    1: optional i64 timestamp

    // The code to identify this write.
    2: optional dsn.task_code task_code

    // The binary form of the write.
    3: optional dsn.blob raw_message

    // ID of the cluster where this write comes from.
    4: optional byte cluster_id

    // Whether to compare the timetag of old value with the new write's.
    5: optional bool verify_timetag
}

struct duplicate_response
{
    1: optional i32 error;

    // hints on the reason why this duplicate failed.
    2: optional string error_hint;
}
