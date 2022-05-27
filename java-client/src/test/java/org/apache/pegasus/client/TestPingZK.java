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

import java.io.InputStream;
import java.util.Arrays;
import java.util.Scanner;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.junit.Assert;
import org.junit.Test;

/** @author qinzuoyan */
public class TestPingZK {

  @Test
  public void testPingZK() throws PException {
    String zkServer = "127.0.0.1:22181";
    String zkPath = "/databases/pegasus/test-java-client";
    String configPath = "zk://" + zkServer + zkPath;

    // init zk config
    ZkClient zkClient = new ZkClient(zkServer, 30000, 30000, new BytesPushThroughSerializer());
    String[] components = zkPath.split("/");
    String curPath = "";
    for (int i = 0; i < components.length; ++i) {
      if (components[i].isEmpty()) continue;
      curPath += "/" + components[i];
      if (!zkClient.exists(curPath)) {
        zkClient.createPersistent(curPath);
      }
    }
    InputStream is = PegasusClient.class.getResourceAsStream("/pegasus.properties");
    Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
    String configData = s.hasNext() ? s.next() : "";
    System.out.println("write config to " + configPath);
    zkClient.writeData(zkPath, configData.getBytes());

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
