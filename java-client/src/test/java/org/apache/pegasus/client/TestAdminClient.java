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

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pegasus.rpc.async.MetaHandler;
import org.apache.pegasus.rpc.async.MetaSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestAdminClient {
  PegasusAdminClientInterface toolsClient;
  final String metaServerList = "127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603";
  final int tablePartitionCount = 8;
  final int tableReplicaCount = 3;
  final int tableOpTimeoutMs = 66000;
  ClientOptions clientOptions;

  @Before
  public void Setup() throws PException {
    this.clientOptions =
        ClientOptions.builder()
            .metaServers(this.metaServerList)
            .asyncWorkers(6)
            .enablePerfCounter(false)
            .build();

    toolsClient = PegasusAdminClientFactory.createClient(this.clientOptions);
  }

  @After
  public void after() {
    toolsClient.close();
  }

  private void testOneCreateApp(String appName) throws PException {
    toolsClient.createApp(
        appName,
        this.tablePartitionCount,
        this.tableReplicaCount,
        new HashMap<>(),
        this.tableOpTimeoutMs);

    boolean isAppHealthy = toolsClient.isAppHealthy(appName, this.tableReplicaCount);

    Assert.assertTrue(isAppHealthy);

    int fakeReplicaCount = 5;
    isAppHealthy = toolsClient.isAppHealthy(appName, fakeReplicaCount);
    Assert.assertFalse(isAppHealthy);
  }

  @Test
  public void testCreateNewApp() throws PException {
    String appName = "testCreateApp1";
    testOneCreateApp(appName);
  }

  @Test
  public void testCreateNewAppConsideringMetaForward() throws PException, IllegalAccessException {
    String[] metaServerArray = this.metaServerList.split(",");
    for (int i = 0; i < metaServerArray.length; ++i) {
      PegasusAdminClient realToolClient = (PegasusAdminClient) toolsClient;
      MetaHandler metaHandler = (MetaHandler) FieldUtils.readField(realToolClient, "meta", true);
      MetaSession metaSession = (MetaSession) FieldUtils.readField(metaHandler, "session", true);
      FieldUtils.writeField(metaSession, "curLeader", i, true);

      String appName = "testMetaForward_" + i;
      testOneCreateApp(appName);
    }
  }

  @Test
  public void testIsAppHealthyIfTableNotExists() throws PException {
    // test a not existed app
    String appName = "testIsAppHealthyIfNotExists";
    int replicaCount = 3;

    try {
      toolsClient.isAppHealthy(appName, this.tableReplicaCount);
    } catch (PException e) {
      return;
    }

    Assert.fail();
  }

  @Test
  public void testDropApp() throws PException {
    String appName = "testDropApp";

    toolsClient.createApp(
        appName,
        this.tablePartitionCount,
        this.tableReplicaCount,
        new HashMap<>(),
        this.tableOpTimeoutMs);
    boolean isAppHealthy = toolsClient.isAppHealthy(appName, this.tableReplicaCount);
    Assert.assertTrue(isAppHealthy);

    toolsClient.dropApp(appName, tableOpTimeoutMs);

    PegasusClientInterface pClient = PegasusClientFactory.createClient(this.clientOptions);
    try {
      pClient.openTable(appName);
    } catch (PException e) {
      assertThat(e.getMessage(), containsString("No such table"));
      pClient.close();
      return;
    }
    pClient.close();
    Assert.fail("expected PException for openTable");
  }
}
