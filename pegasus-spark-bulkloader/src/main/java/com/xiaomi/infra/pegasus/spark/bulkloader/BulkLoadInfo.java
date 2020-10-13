package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.utils.JsonParser;

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
