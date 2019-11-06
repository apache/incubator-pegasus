package com.xiaomi.infra.pegasus.analyser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.*;

/**
 * The wrapper of RocksDB Options in JNI.
 *
 * <p>NOTE: Must be closed manually to release the underlying memory.
 */
class RocksDBOptions {

  private static final Log LOG = LogFactory.getLog(RocksDBOptions.class);

  Options options;
  ReadOptions readOptions;
  private Env env;

  RocksDBOptions(Config config) {
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

    Logger rocksDBLog =
        new Logger(options) {
          @Override
          public void log(InfoLogLevel infoLogLevel, String s) {
            LOG.info("[rocksDB native log info]" + s);
          }
        };

    options.setLogger(rocksDBLog);
  }

  void close() {
    options.close();
    readOptions.close();
    env.close();
  }
}
