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

import org.junit.jupiter.api.Test;

/** Created by weijiesun on 16-11-24. */
public class TestNoOperate {
  @Test
  public void testNoOperate() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";

    System.out.println("start to write some keys");
    for (int i = 0; i < 100; ++i) {
      String hashKey = "hello" + String.valueOf(i);
      String sortKey = "0";
      String value = "world";
      client.set(tableName, hashKey.getBytes(), sortKey.getBytes(), value.getBytes());
    }

    System.out.println("start to wait some time");

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    PegasusClientFactory.closeSingletonClient();
  }
}
