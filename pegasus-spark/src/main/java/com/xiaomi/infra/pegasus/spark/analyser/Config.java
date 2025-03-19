package com.xiaomi.infra.pegasus.spark.analyser;

public interface Config {

  enum DataType {
    COLD_BACKUP,
    ONLINE,
    INVALID
  }

  DataType getDataType();
}
