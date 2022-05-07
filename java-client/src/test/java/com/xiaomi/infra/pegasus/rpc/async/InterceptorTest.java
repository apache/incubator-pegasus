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
package com.xiaomi.infra.pegasus.rpc.async;

import com.xiaomi.infra.pegasus.client.ClientOptions;
import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.client.PegasusClientFactory;
import com.xiaomi.infra.pegasus.client.PegasusTableInterface;
import com.xiaomi.infra.pegasus.client.TableOptions;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class InterceptorTest {
  @Test
  public void testCompressionInterceptor() throws PException {
    PegasusTableInterface commonTable =
        PegasusClientFactory.createClient(ClientOptions.create()).openTable("temp");
    PegasusTableInterface compressTable =
        PegasusClientFactory.createClient(ClientOptions.create())
            .openTable("temp", new TableOptions().withCompression(true));

    byte[] hashKey = "hashKey".getBytes();
    byte[] sortKey = "sortKey".getBytes();
    byte[] commonValue = "commonValue".getBytes();
    byte[] compressionValue = "compressionValue".getBytes();

    // if origin value was not compressed, both commonTable and compressTable can read origin value
    commonTable.set(hashKey, sortKey, commonValue, 10000);
    Assertions.assertEquals(
        new String(commonTable.get(hashKey, sortKey, 10000)), new String(commonValue));
    Assertions.assertEquals(
        new String(compressTable.get(hashKey, sortKey, 10000)), new String(commonValue));

    // if origin value was compressed, only compressTable can read successfully
    compressTable.set(hashKey, sortKey, compressionValue, 10000);
    Assertions.assertNotEquals(
        new String(commonTable.get(hashKey, sortKey, 10000)), new String(compressionValue));
    Assertions.assertEquals(
        new String(compressTable.get(hashKey, sortKey, 10000)), new String(compressionValue));

    // if origin value is null, return null
    byte[] ret = compressTable.get("not-exist".getBytes(), sortKey, 10000);
    Assertions.assertNull(ret);
  }
}
