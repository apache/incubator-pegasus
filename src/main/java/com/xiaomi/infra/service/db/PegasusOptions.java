package com.xiaomi.infra.service.db;

import com.xiaomi.infra.config.Config;
import org.rocksdb.Env;
import org.rocksdb.HdfsEnv;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Logger;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;

public class PegasusOptions {

  public Options options;
  public ReadOptions readOptions;
  private Env env;

  public PegasusOptions(Config config) {
    if (config.DATA_URL.contains("fds")) {
      env = new HdfsEnv(config.DATA_URL + "#" + config.DATA_PORT);
    } else {
      env = new HdfsEnv(config.DATA_URL + ":" + config.DATA_URL);
    }

    options =
        new Options()
            .setDisableAutoCompactions(true)
            .setCreateIfMissing(true)
            .setEnv(env)
            .setLevel0FileNumCompactionTrigger(-1)
            .setMaxOpenFiles(config.MAX_FILE_OPEN_COUNTER);

    readOptions = new ReadOptions().setReadaheadSize(config.READ_AHEAD_SIZE);

    Logger rocksdbLog =
        new Logger(options) {
          @Override
          public void log(InfoLogLevel infoLogLevel, String s) {}
        };

    options.setLogger(rocksdbLog);
  }

  public void close() {
    options.close();
    readOptions.close();
    env.close();
  }
}
