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

#include "common.h"

#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"

DSN_DEFINE_string(replication, cluster_name, "", "The name of this cluster");

// Many Pegasus clusters are configured with the same `cluster_name`(namely
// `[replication]cluster_name`). However, once we decide to duplicate tables
// between them, their `cluster_name` have to be changed to be distinguished
// from each other -- this might lead to side effects. Thus use `dup_cluster_name`
// only for duplication in case `cluster_name` has to be changed.
DSN_DEFINE_string(replication,
                  dup_cluster_name,
                  "",
                  "The name of this cluster only used for duplication");

namespace dsn {

/*extern*/ const char *get_current_cluster_name()
{
    CHECK(!utils::is_empty(FLAGS_cluster_name), "cluster_name is not set");
    return FLAGS_cluster_name;
}

/*extern*/ const char *get_current_dup_cluster_name()
{
    if (!utils::is_empty(FLAGS_dup_cluster_name)) {
        return FLAGS_dup_cluster_name;
    }

    // Once `dup_cluster_name` is not configured, use cluster_name instead.
    return get_current_cluster_name();
}

const std::string PEGASUS_CLUSTER_SECTION_NAME("pegasus.clusters");

} // namespace dsn
