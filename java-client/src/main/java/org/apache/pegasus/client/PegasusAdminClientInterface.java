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

import java.io.Closeable;
import java.util.Map;

public interface PegasusAdminClientInterface extends Closeable {
  /**
   * Create A new pegasus app which is not stateless However the successful execution of the
   * interface does not guarantee the fully healthy of every partition of the newly created app You
   * can use @isAppHealthy interface to check if the newly created app is fully healthy
   *
   * @param appName App name which will be created by this interface
   * @param partitionCount The partition count of the newly creating app
   * @param replicaCount The replica count of the newly creating app
   * @param envs Environment variables of pegasus app, you can see the supported envs in the website
   *     : https://pegasus.apache.org/administration/table-env
   * @param timeoutMs The timeout of the interface, milli-seconds
   * @param successIfExist whether return success if app exist
   * @throws PException if rpc to the pegasus server cause timeout or other error happens in the
   *     server side, or the newly created app is not fully healthy when the 'timeoutMs' has
   *     elapsed, the interface will throw exception
   */
  public void createApp(
      String appName,
      int partitionCount,
      int replicaCount,
      Map<String, String> envs,
      long timeoutMs,
      boolean successIfExist)
      throws PException;

  public void createApp(
      String appName,
      int partitionCount,
      int replicaCount,
      Map<String, String> envs,
      long timeoutMs)
      throws PException;

  /**
   * Judge If An App Is 'healthy'(every partition of the app has enough replicas specified by the
   * 'replicaCount' parameter)
   *
   * @param appName App name which will be judged 'healthy' or not by this interface
   * @param replicaCount replicaCount of the app
   * @return true if the app in the pegasus server side has enough healthy replicas specified by the
   *     'replicaCount' parameter, otherwise return false
   * @throws PException If 'appName' not exists or other error happens in the server side, the
   *     interface will throw PException
   */
  public boolean isAppHealthy(String appName, int replicaCount) throws PException;

  public void dropApp(String appName, int reserveSeconds) throws PException;

  /** close the client */
  @Override
  public void close();
}
