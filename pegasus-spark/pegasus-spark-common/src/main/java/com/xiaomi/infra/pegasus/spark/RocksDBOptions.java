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
