package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.analyser.Config.DataType;
import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;

class PegasusLoaderFactory {

  public static PegasusLoader createDataLoader(Config cfg) throws PegasusSparkException {
    if (cfg.getDataType() == DataType.COLD_BACKUP) {
      return new ColdBackupLoader((ColdBackupConfig) cfg);
    } else {
      // TODO(jiashuo) will support more data type, such as online data
      throw new PegasusSparkException(
          "now only support cold backup data, data type = " + cfg.getDataType());
    }
  }
}
