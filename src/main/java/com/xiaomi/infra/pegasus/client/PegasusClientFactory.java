// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author qinzuoyan
 *     <p>This class provides interfaces to create an instance of {@link PegasusClientInterface}.
 */
public class PegasusClientFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PegasusClientFactory.class);

  private static volatile PegasusClient singletonClient = null;
  private static String singletonClientConfigPath = null;
  private static Object singletonClientLock = new Object();

  private static ClientOptions singletonClientOptions = null;

  /**
   * Create a client instance. After used, should call client.close() to release resource.
   *
   * @param configPath client config path,could be:
   *     <pre>
   * - zookeeper path  : zk://host1:port1,host2:port2,host3:port3/path/to/config
   * - local file path : file:///path/to/config
   * - java resource   : resource:///path/to/config</pre>
   *
   * @return PegasusClientInterface {@link PegasusClientInterface}
   * @throws PException throws exception if any error occurs.
   */
  public static PegasusClientInterface createClient(String configPath) throws PException {
    return new PegasusClient(configPath);
  }

  /**
   * Create a client instance instance with {@link ClientOptions}. After used, should call
   * client.close() to release resource.
   *
   * @param options The client option
   * @return PegasusClientInterface {@link PegasusClientInterface}
   * @throws PException throws exception if any error occurs.
   */
  public static PegasusClientInterface createClient(ClientOptions options) throws PException {
    Properties pegasusConfig = new Properties();
    pegasusConfig.setProperty("meta_servers", options.getMetaServers());
    pegasusConfig.setProperty(
        "operation_timeout", String.valueOf(options.getOperationTimeout().toMillis()));
    pegasusConfig.setProperty("async_workers", String.valueOf(options.getAsyncWorkers()));
    pegasusConfig.setProperty("enable_perf_counter", String.valueOf(options.isEnablePerfCounter()));
    pegasusConfig.setProperty("perf_counter_tags", String.valueOf(options.isEnablePerfCounter()));
    pegasusConfig.setProperty(
        "push_counter_interval_secs", String.valueOf(options.getFalconPushInterval().getSeconds()));
    return new PegasusClient(pegasusConfig);
  }

  /**
   * Get the singleton client instance with default config path of "resource:///pegasus.properties".
   * After used, should call PegasusClientFactory.closeSingletonClient() to release resource.
   *
   * @return PegasusClientInterface {@link PegasusClientInterface}
   * @throws PException throws exception if any error occurs.
   */
  public static PegasusClientInterface getSingletonClient() throws PException {
    return getSingletonClient("resource:///pegasus.properties");
  }

  /**
   * Get the singleton client instance with customized config path. After used, should call
   * PegasusClientFactory.closeSingletonClient() to release resource.
   *
   * @param configPath configPath could be:
   *     <pre>
   * - zookeeper path  : zk://host1:port1,host2:port2,host3:port3/path/to/config
   * - local file path : file:///path/to/config
   * - java resource   : resource:///path/to/config</pre>
   *
   * @return PegasusClientInterface {@link PegasusClientInterface}
   * @throws PException throws exception if any error occurs.
   */
  public static PegasusClientInterface getSingletonClient(String configPath) throws PException {
    synchronized (singletonClientLock) {
      if (singletonClient == null) {
        try {
          singletonClient = new PegasusClient(configPath);
          singletonClientConfigPath = configPath;
          LOGGER.info("Create Singleton PegasusClient with config path \"" + configPath + "\"");
        } catch (Throwable e) {
          throw new PException("Create Singleton PegasusClient Failed", e);
        }
      } else if (!singletonClientConfigPath.equals(configPath)) {
        LOGGER.error(
            "Singleton PegasusClient Config Path Conflict: \""
                + configPath
                + "\" VS \""
                + singletonClientConfigPath
                + "\"");
        throw new PException("Singleton PegasusClient Config Path Conflict");
      }
      return singletonClient;
    }
  }

  /**
   * Get the singleton client instance instance with {@link ClientOptions}. After used, should call
   * PegasusClientFactory.closeSingletonClient() to release resource.
   *
   * @param options The client option
   * @return PegasusClientInterface {@link PegasusClientInterface}
   * @throws PException throws exception if any error occurs.
   */
  public static PegasusClientInterface getSingletonClient(ClientOptions options) throws PException {
    synchronized (singletonClientLock) {
      if (singletonClient == null) {
        try {
          singletonClient = (PegasusClient) createClient(options);
          singletonClientOptions = options;
          LOGGER.info("Create Singleton PegasusClient with options \"" + options.toString() + "\"");
        } catch (Throwable e) {
          throw new PException("Create Singleton PegasusClient Failed", e);
        }
      } else if (!singletonClientOptions.equals(options)) {
        LOGGER.error(
            "Singleton PegasusClient options Conflict: \""
                + options.toString()
                + "\" VS \""
                + singletonClientOptions.toString()
                + "\"");
        throw new PException("Singleton PegasusClient options Conflict");
      }
      return singletonClient;
    }
  }

  /**
   * Close the singleton client instance.
   *
   * @throws PException throws exception if any error occurs.
   */
  public static void closeSingletonClient() throws PException {
    synchronized (singletonClientLock) {
      if (singletonClient != null) {
        LOGGER.info(
            "Close Singleton PegasusClient with config path \"" + singletonClientConfigPath + "\"");
        singletonClient.close();
        singletonClient = null;
        singletonClientConfigPath = null;
      }
    }
  }
}
