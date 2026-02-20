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

package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.common.utils.JsonParser;

class BulkLoadInfo {

  private String cluster;
  private int app_id;
  private String app_name;
  private int partition_count;

  BulkLoadInfo(String cluster, String app_name, int app_id, int partition_count) {
    this.cluster = cluster;
    this.app_name = app_name;
    this.app_id = app_id;
    this.partition_count = partition_count;
  }

  String toJsonString() {
    return JsonParser.getGson().toJson(this);
  }
}
