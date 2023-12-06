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
package org.apache.pegasus.tools;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.pegasus.client.PException;
import org.apache.pegasus.client.PegasusClientFactory;
import org.apache.pegasus.client.PegasusClientInterface;
import org.apache.pegasus.client.PegasusTableInterface;
import org.junit.jupiter.api.Test;

public class TestZstdWrapper {
  @Test
  public void testCompression() throws Exception {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    PegasusTableInterface table = client.openTable("temp");

    for (int t = 0; t < 4; t++) {
      // generate a 10KB value
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < 10000; i++) {
        builder.append('a' + t);
      }
      byte[] value = builder.toString().getBytes();

      // write the record into pegasus
      table.set("h".getBytes(), "s".getBytes(), ZstdWrapper.compress(value), 1000);

      // read the record from pegasus
      byte[] compressedBuf = table.get("h".getBytes(), "s".getBytes(), 1000);

      // decompress the value
      assertArrayEquals(ZstdWrapper.decompress(compressedBuf), value);
    }

    // ensure empty value won't break the program
    {
      try {
        ZstdWrapper.decompress("".getBytes());
        fail("expecting a IllegalArgumentException");
      } catch (Exception e) {
        assertTrue(e instanceof IllegalArgumentException);
      }
      try {
        ZstdWrapper.decompress(null);
        fail("expecting a IllegalArgumentException");
      } catch (Exception e) {
        assertTrue(e instanceof IllegalArgumentException);
      }
    }

    { // decompress invalid data
      try {
        ZstdWrapper.decompress("abc123".getBytes());
        fail("expecting a PException");
      } catch (Exception e) {
        assertTrue(e instanceof PException);
      }
    }
  }
}
