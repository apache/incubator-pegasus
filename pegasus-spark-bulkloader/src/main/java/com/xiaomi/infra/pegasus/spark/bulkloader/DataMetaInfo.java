package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.utils.JsonParser;
import java.util.ArrayList;

class DataMetaInfo {

  ArrayList<FileInfo> files = new ArrayList<>();
  long file_total_size;

  class FileInfo {

    String name;
    long size;
    String md5;

    FileInfo(String name, long size, String md5) {
      this.name = name;
      this.size = size;
      this.md5 = md5;
    }
  }

  String toJsonString() {
    return JsonParser.getGson().toJson(this);
  }
}
