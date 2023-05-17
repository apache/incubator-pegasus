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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.pegasus.base.error_code;
import org.apache.pegasus.base.gpid;
import org.apache.pegasus.operator.create_app_operator;
import org.apache.pegasus.operator.drop_app_operator;
import org.apache.pegasus.operator.list_apps_operator;
import org.apache.pegasus.operator.query_cfg_operator;
import org.apache.pegasus.replication.*;
import org.apache.pegasus.rpc.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PegasusAdminClient extends PegasusAbstractClient
    implements PegasusAdminClientInterface {
  private static final Logger LOGGER = LoggerFactory.getLogger(PegasusClient.class);
  private static final String APP_TYPE = "pegasus";
  private static final int META_RETRY_MIN_COUNT = 5;

  private Meta meta;

  @Override
  public String clientType() {
    return "Admin";
  }

  private void initMeta() {
    this.meta = this.cluster.getMeta();
  }

  public PegasusAdminClient(Properties properties) throws PException {
    super(properties);
    initMeta();
  }

  public PegasusAdminClient(String configPath) throws PException {
    super(configPath);
    initMeta();
  }

  public PegasusAdminClient(ClientOptions options) throws PException {
    super(options);
    initMeta();
  }

  @Override
  public void createApp(
      String appName,
      int partitionCount,
      int replicaCount,
      Map<String, String> envs,
      long timeoutMs,
      boolean successIfExist)
      throws PException {
    if (partitionCount < 1) {
      throw new PException(
          new IllegalArgumentException("createApp failed: partitionCount should >= 1!"));
    }

    if (replicaCount < 1) {
      throw new PException(
          new IllegalArgumentException("createApp failed: replicaCount should >= 1!"));
    }

    int i = 0;
    for (; i < appName.length(); i++) {
      char c = appName.charAt(i);
      if (!((c >= 'a' && c <= 'z')
          || (c >= 'A' && c <= 'Z')
          || (c >= '0' && c <= '9')
          || c == '_'
          || c == '.'
          || c == ':')) {
        break;
      }
    }

    if (appName.isEmpty() || i < appName.length()) {
      throw new PException(
          new IllegalArgumentException(
              String.format("createApp  failed: invalid appName: %s", appName)));
    }

    if (timeoutMs <= 0) {
      throw new PException(
          new IllegalArgumentException(
              String.format("createApp  failed: invalid timeoutMs: %d", timeoutMs)));
    }

    long startTime = System.currentTimeMillis();

    create_app_options options = new create_app_options();
    options.setPartition_count(partitionCount);
    options.setReplica_count(replicaCount);
    options.setSuccess_if_exist(successIfExist);
    options.setApp_type(APP_TYPE);
    options.setEnvs(envs);
    options.setIs_stateful(true);

    configuration_create_app_request request = new configuration_create_app_request();
    request.setApp_name(appName);
    request.setOptions(options);

    create_app_operator app_operator = new create_app_operator(appName, request);
    error_code.error_types error = this.meta.operate(app_operator, META_RETRY_MIN_COUNT);
    if (error != error_code.error_types.ERR_OK) {
      throw new PException(
          String.format(
              "Create app:%s failed, partitionCount: %d, replicaCount: %s, error:%s.",
              appName, partitionCount, replicaCount, error.toString()));
    }

    long remainDuration = timeoutMs - (System.currentTimeMillis() - startTime);
    if (remainDuration <= 0) {
      remainDuration = 8;
    }

    boolean isHealthy = false;
    while (remainDuration > 0) {
      isHealthy = this.isAppHealthy(appName, replicaCount);
      if (isHealthy) {
        break;
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        continue;
      }

      remainDuration = timeoutMs - (System.currentTimeMillis() - startTime);
    }

    if (!isHealthy) {
      throw new PException(
          String.format(
              "The newly created app:%s is not fully healthy now, but the interface duration expires 'timeoutMs', partitionCount: %d, replicaCount: %s.",
              appName, partitionCount, replicaCount));
    }
  }

  @Override
  public void createApp(
      String appName,
      int partitionCount,
      int replicaCount,
      Map<String, String> envs,
      long timeoutMs)
      throws PException {
    this.createApp(appName, partitionCount, replicaCount, envs, timeoutMs, true);
  }

  @Override
  public boolean isAppHealthy(String appName, int replicaCount) throws PException {
    if (replicaCount < 1) {
      throw new PException(
          new IllegalArgumentException(
              String.format("Query app:%s Status failed: replicaCount should >= 1!", appName)));
    }

    query_cfg_request request = new query_cfg_request();
    request.setApp_name(appName);

    query_cfg_operator query_op = new query_cfg_operator(new gpid(-1, -1), request);
    error_code.error_types error = this.meta.operate(query_op, META_RETRY_MIN_COUNT);
    if (error != error_code.error_types.ERR_OK) {
      throw new PException(
          String.format(
              "Query app status failed, app:%s, replicaCount: %s, error:%s.",
              appName, replicaCount, error.toString()));
    }

    query_cfg_response response = query_op.get_response();

    int readyCount = 0;
    for (int i = 0; i < response.partition_count; ++i) {
      partition_configuration pc = response.partitions.get(i);
      if (!pc.primary.isInvalid() && (pc.secondaries.size() + 1 >= replicaCount)) {
        ++readyCount;
      }
    }

    LOGGER.info(
        String.format(
            "Check app healthy, appName:%s, partitionCount:%d, ready_count:%d.",
            appName, response.partition_count, readyCount));

    return readyCount == response.partition_count;
  }

  @Override
  public void dropApp(String appName, int reserveSeconds) throws PException {
    if (appName.isEmpty()) {
      throw new PException(new IllegalArgumentException("dropApp failed: empty appName"));
    }

    drop_app_options options = new drop_app_options();
    options.setSuccess_if_not_exist(true);
    options.setReserve_seconds(reserveSeconds);

    configuration_drop_app_request request = new configuration_drop_app_request();
    request.setApp_name(appName);
    request.setOptions(options);

    drop_app_operator app_operator = new drop_app_operator(appName, request);
    error_code.error_types error = this.meta.operate(app_operator, META_RETRY_MIN_COUNT);

    if (error != error_code.error_types.ERR_OK) {
      throw new PException(
          String.format("Drop app:%s failed! error: %s.", appName, error.toString()));
    }
  }

  @Override
  public List<app_info> listApps(ListAppInfoType listAppInfoType) throws PException {
    configuration_list_apps_request request = new configuration_list_apps_request();
    if (listAppInfoType == ListAppInfoType.LT_AVAILABLE_APPS) {
      // if set request.setStatus as 'AS_AVAILABLE', It will return 'app_info' of all available
      // tables
      request.setStatus(app_status.AS_AVAILABLE);
    } else if (listAppInfoType == ListAppInfoType.LT_ALL_APPS) {
      // if set request.setStatus as 'AS_INVALID', It will return app_info of all tables, including
      // dropped but currently reserved tables
      request.setStatus(app_status.AS_INVALID);
    } else {
      throw new PException(String.format("List apps failed, unknown ListAppInfoType."));
    }

    list_apps_operator app_operator = new list_apps_operator(request);
    error_code.error_types error = this.meta.operate(app_operator, META_RETRY_MIN_COUNT);

    if (error != error_code.error_types.ERR_OK) {
      throw new PException(
          String.format(
              "List apps failed, query status: %s, error: %s.",
              request.getStatus(), error.toString()));
    }

    configuration_list_apps_response response = app_operator.get_response();
    return response.infos;
  }
}
