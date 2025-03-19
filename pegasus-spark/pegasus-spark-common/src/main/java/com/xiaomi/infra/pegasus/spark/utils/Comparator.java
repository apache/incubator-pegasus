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

package com.xiaomi.infra.pegasus.spark.utils;

public class Comparator {

  public static int bytesCompare(byte[] byteArray1, byte[] byteArray2) {
    if (byteArray1 == byteArray2) {
      return 0;
    }
    if (byteArray1 == null) {
      return -1;
    }
    if (byteArray2 == null) {
      return 1;
    }

    if (byteArray1.length < byteArray2.length) {
      int pos = 0;

      for (byte b1 : byteArray1) {
        byte b2 = byteArray2[pos];

        if (b1 == b2) {
          pos++;
        } else if (b1 < b2) {
          return -1;
        } else {
          return 1;
        }
      }

      return -1;
    } else {
      int pos = 0;

      for (byte b2 : byteArray2) {
        byte b1 = byteArray1[pos];

        if (b1 == b2) {
          pos++;
        } else if (b1 < b2) {
          return -1;
        } else {
          return 1;
        }
      }

      if (pos < byteArray1.length) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
