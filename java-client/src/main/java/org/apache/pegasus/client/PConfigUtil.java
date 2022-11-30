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
package org.apache.pegasus.client;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author qinzuoyan
 *     <p>This class encapsulates Tools for loading configuration.
 */
public class PConfigUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(PConfigUtil.class);

  public static final String ZK_PREFIX = "zk://";
  public static final String LOCAL_FILE_PREFIX = "file://";
  public static final String RESOURCE_PREFIX = "resource://";
  public static final String SLASH = "/";
  public static final int ZK_SESSION_TIMEOUT = 30000;
  public static final int ZK_CONNECTION_TIMEOUT = 30000;

  public static final String PEGASUS_BUSINESS_ROOT_NODE = "/databases/pegasus";

  public static boolean isZkPath(String path) {
    return path.startsWith(ZK_PREFIX);
  }

  public static boolean isLocalFile(String path) {
    return path.startsWith(LOCAL_FILE_PREFIX);
  }

  public static boolean isResource(String path) {
    return path.startsWith(RESOURCE_PREFIX);
  }

  // zkServers should be "host1:port1,host2:port2,host3:port3"
  // return as "zk://{zkServers}/databases/pegasus/{businessName}"
  public static String getBusinessConfigZkUri(String zkServers, String businessName)
      throws PException {
    return ZK_PREFIX + zkServers + getBusinessConfigZkPath(businessName);
  }

  // load client configuration from configPath, which could be local file path or zk path or
  // resource path.
  public static Properties loadConfiguration(String configPath) throws PException {
    InputStream stream = null;
    try {
      Properties config = new Properties();
      if (PConfigUtil.isZkPath(configPath)) {
        stream = new ByteArrayInputStream(PConfigUtil.loadConfigFromZK(configPath));
      } else if (PConfigUtil.isLocalFile(configPath)) {
        stream =
            new BufferedInputStream(
                new FileInputStream(configPath.substring(PConfigUtil.LOCAL_FILE_PREFIX.length())));
      } else if (PConfigUtil.isResource(configPath)) {
        stream =
            PegasusClient.class.getResourceAsStream(
                configPath.substring(PConfigUtil.RESOURCE_PREFIX.length()));
      } else {
        throw new PException(
            "configPath format error, "
                + "should be local file format as 'file:///path/to/config', "
                + "or zookeeper path format as 'zk://host1:port1,host2:port2,host3:port3/path/to/config', "
                + "or java resource format as 'resource:///path/to/config', "
                + "but actual configPath is "
                + configPath);
      }
      if (stream == null) {
        throw new PException("config resource not found: " + configPath);
      }
      config.load(stream);
      return config;
    } catch (Throwable e) {
      if (e instanceof PException) {
        throw (PException) e;
      } else {
        throw new PException(e);
      }
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (Exception e) {
          throw new PException(e);
        }
      }
    }
  }

  public static byte[] loadConfigFromZK(String zkUri) throws PException {
    Pair<String, String> zkServerAndPath = getZkServerAndPath(zkUri);
    String server = zkServerAndPath.getKey();
    String path = zkServerAndPath.getValue();
    LOGGER.info("Pegasus load client information from zkServer=" + server + ", zkPath=" + path);
    ZooKeeper zk = null;
    try {
      zk = new ZooKeeper(server, ZK_SESSION_TIMEOUT, null);
      return zk.getData(path, false, null);
    } catch (Exception e) {
      throw new PException(e);
    } finally {
      if (zk != null) {
        try {
          zk.close();
        } catch (InterruptedException e) {
          LOGGER.warn("failed to close zookeeper", e);
        }
      }
    }
  }

  protected static String getBusinessConfigZkPath(String businessName) {
    return PEGASUS_BUSINESS_ROOT_NODE + SLASH + businessName;
  }

  // a simple function to get server and path from zkUri: zk://server/path
  // where server is formatted as 'host1:port1,host2:port2,host3:port3'
  protected static Pair<String, String> getZkServerAndPath(String zkUri) throws PException {
    try {
      // skip "zk://"
      String tempZkUri = zkUri.substring(5);
      int firstSlashIndex = tempZkUri.indexOf("/");
      String server = tempZkUri.substring(0, firstSlashIndex);
      String path = tempZkUri.substring(firstSlashIndex);
      return Pair.of(server, path);
    } catch (Exception e) {
      throw new PException(e);
    }
  }
}
