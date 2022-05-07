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

package com.xiaomi.infra.pegasus.client;

import java.util.Properties;

public class PegasusAdminClientFactory {
  /**
   * Create a client instance. After used, should call client.close() to release resource.
   *
   * @param configPath client config path,could be:
   *     <pre>
   * - zookeeper path  : zk://host1:port1,host2:port2,host3:port3/path/to/config
   * - local file path : file:///path/to/config
   * - java resource   : resource:///path/to/config</pre>
   *
   * @return PegasusAdminClientInterface {@link PegasusAdminClientInterface}
   * @throws PException throws exception if any error occurs.
   */
  public static PegasusAdminClientInterface createClient(String configPath) throws PException {
    return new PegasusAdminClient(configPath);
  }

  /**
   * Create a client instance. After used, should call client.close() to release resource.
   *
   * @param properties properties
   * @return PegasusAdminClientInterface {@link PegasusAdminClientInterface}
   * @throws PException throws exception if any error occurs.
   */
  public static PegasusAdminClientInterface createClient(Properties properties) throws PException {
    return new PegasusAdminClient(properties);
  }

  /**
   * Create a client instance instance with {@link ClientOptions}. After used, should call
   * client.close() to release resource.
   *
   * @param clientOptions The client option
   * @return PegasusAdminClientInterface {@link PegasusAdminClientInterface}
   * @throws PException throws exception if any error occurs.
   */
  public static PegasusAdminClientInterface createClient(ClientOptions clientOptions)
      throws PException {
    return new PegasusAdminClient(clientOptions);
  }
}
