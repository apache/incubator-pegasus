package com.xiaomi.infra.pegasus.spark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.*;

/**
 * The wrapper of RocksDB Options in JNI.
 *
 * <p>NOTE: Must be closed manually to release the underlying memory.
 */
public class RocksDBOptions {

  private static final Log LOG = LogFactory.getLog(RocksDBOptions.class);

  public Options options = new Options();
  public ReadOptions readOptions = new ReadOptions();
  private Env env;
  public EnvOptions envOptions = new EnvOptions();

  public RocksDBOptions(String remoteFsUrl, String remoteFsPort) throws PegasusSparkException {
    if (remoteFsUrl.startsWith("fds://")) {
      env = new HdfsEnv(remoteFsUrl + "#" + remoteFsPort);
    } else if (remoteFsUrl.startsWith("hdfs://")) {
      env = new HdfsEnv(remoteFsUrl + ":" + remoteFsPort);
    } else if (remoteFsUrl.equals("default")) {
      env = new HdfsEnv(remoteFsUrl);
      LOG.warn("You now access hdfs using default url in config(core-site.xml)");
    } else {
      throw new PegasusSparkException("Not support the url:" + remoteFsUrl);
    }

    Logger rocksDBLog =
        new Logger(options) {
          @Override
          public void log(InfoLogLevel infoLogLevel, String s) {
            LOG.info("[rocksDB native log info]" + s);
          }
        };
    options.setCreateIfMissing(true).setEnv(env).setLogger(rocksDBLog);
  }

  public void close() {
    options.close();
    readOptions.close();
    env.close();
  }
}
