/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <string>

namespace dsn {
/// Returns the cluster name ("onebox" in the following example) if it's
/// configured under "replication" section:
///    [replication]
///      cluster_name = "onebox"
extern const char *get_current_cluster_name();

/// Returns the cluster name ("onebox" in the following example) which is
/// only used for duplication (see the definition for `dup_cluster_name`
/// flag for details) if it's configured under "replication" section:
///    [replication]
///      dup_cluster_name = "onebox"
///
/// However, once `[replication]dup_cluster_name` is not configured,
/// `[replication]cluster_name` would be returned.
extern const char *get_current_dup_cluster_name();

extern const std::string PEGASUS_CLUSTER_SECTION_NAME;
} // namespace dsn
