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

namespace cpp dsn

// Metadata field of the request in rDSN's thrift protocol (version 1).
// TODO(wutao1): add design doc of the thrift protocol.
struct thrift_request_meta_v1
{
    // The replica's gpid.
    1:optional i32 app_id;
    2:optional i32 partition_index;

    // The timeout of this request that's set on client side.
    3:optional i32 client_timeout;

    // The hash value calculated from the hash key.
    4:optional i64 client_partition_hash;

    // Whether it is a backup request. If true, this request (only if it's a read) can be handled by
    // a secondary replica, which does not guarantee strong consistency.
    5:optional bool is_backup_request;
}
