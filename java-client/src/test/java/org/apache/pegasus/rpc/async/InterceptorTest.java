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
package org.apache.pegasus.rpc.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.pegasus.client.ClientOptions;
import org.apache.pegasus.client.PException;
import org.apache.pegasus.client.PegasusClientFactory;
import org.apache.pegasus.client.PegasusTableInterface;
import org.apache.pegasus.client.TableOptions;
import org.junit.jupiter.api.Test;

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
    assertEquals(new String(commonTable.get(hashKey, sortKey, 10000)), new String(commonValue));
    assertEquals(new String(compressTable.get(hashKey, sortKey, 10000)), new String(commonValue));

    // if origin value was compressed, only compressTable can read successfully
    compressTable.set(hashKey, sortKey, compressionValue, 10000);
    assertNotEquals(
        new String(commonTable.get(hashKey, sortKey, 10000)), new String(compressionValue));
    assertEquals(
        new String(compressTable.get(hashKey, sortKey, 10000)), new String(compressionValue));

    // if origin value is null, return null
    byte[] ret = compressTable.get("not-exist".getBytes(), sortKey, 10000);
    assertNull(ret);
  }
}
