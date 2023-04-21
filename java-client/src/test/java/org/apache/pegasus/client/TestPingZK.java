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

import com.google.common.io.ByteStreams;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;

/** @author qinzuoyan */
public class TestPingZK {

  @Test
  public void testPingZK() throws Exception {
    String zkServer = "127.0.0.1:22181";
    String zkPath = "/databases/pegasus/test-java-client";
    String configPath = "zk://" + zkServer + zkPath;

    // init zk config
    ZooKeeper zk = new ZooKeeper(zkServer, 30000, null);
    try {
      String[] components = zkPath.split("/");
      String curPath = "";
      for (int i = 0; i < components.length; ++i) {
        if (components[i].isEmpty()) {
          continue;
        }
        curPath += "/" + components[i];
        if (zk.exists(curPath, false) == null) {
          zk.create(curPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
      }
      byte[] configData;
      try (InputStream is = PegasusClient.class.getResourceAsStream("/pegasus.properties")) {
        configData = ByteStreams.toByteArray(is);
      }
      System.out.println("write config to " + configPath);
      zk.setData(zkPath, configData, -1);
    } finally {
      zk.close();
    }

    PegasusClientInterface client = PegasusClientFactory.createClient(configPath);
    String tableName = "temp";

    byte[] hashKey = "hello".getBytes();
    byte[] sortKey = "0".getBytes();
    byte[] value = "world".getBytes();

    System.out.println("set value ...");
    client.set(tableName, hashKey, sortKey, value, 0);
    System.out.println("set value ok");

    System.out.println("get value ...");
    byte[] result = client.get(tableName, hashKey, sortKey);
    Assert.assertTrue(Arrays.equals(value, result));
    System.out.println("get value ok");

    System.out.println("del value ...");
    client.del(tableName, hashKey, sortKey);
    System.out.println("del value ok");

    System.out.println("get deleted value ...");
    result = client.get(tableName, hashKey, sortKey);
    Assert.assertEquals(result, null);
    System.out.println("get deleted value ok");

    System.out.println("set value ...");
    client.set(tableName, hashKey, sortKey, value, 0);
    System.out.println("set value ok");

    PegasusClientFactory.closeSingletonClient();
  }
}
