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
   * Create a client instance. After used, should call client.close() to release resource.
   *
   * @param properties properties
   * @return PegasusClientInterface {@link PegasusClientInterface}
   * @throws PException throws exception if any error occurs.
   */
  public static PegasusClientInterface createClient(Properties properties) throws PException {
    return new PegasusClient(properties);
  }

  /**
   * Create a client instance instance with {@link ClientOptions}. After used, should call
   * client.close() to release resource.
   *
   * @param clientOptions The client option
   * @return PegasusClientInterface {@link PegasusClientInterface}
   * @throws PException throws exception if any error occurs.
   */
  public static PegasusClientInterface createClient(ClientOptions clientOptions) throws PException {
    return new PegasusClient(clientOptions);
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
   * Get the singleton client instance with properties. After used, should call
   * PegasusClientFactory.closeSingletonClient() to release resource.
   *
   * @param properties properties
   * @return PegasusClientInterface {@link PegasusClientInterface}
   * @throws PException throws exception if any error occurs.
   */
  public static PegasusClientInterface getSingletonClient(Properties properties) throws PException {
    return getSingletonClient(ClientOptions.create(properties));
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
