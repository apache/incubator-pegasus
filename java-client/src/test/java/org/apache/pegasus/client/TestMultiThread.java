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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import org.junit.jupiter.api.Test;

/** Created by mi on 16-3-23. */
public class TestMultiThread {
  public static final String TABLE_NAME = "temp";

  private static class VisitThread extends Thread {
    // so total operations = total_keys + removed_keys
    private static int total_keys = 3000;
    private static int removed_keys = 2000;

    private String name;
    private PegasusClientInterface client;

    public VisitThread(String name, PegasusClientInterface client) {
      this.name = name;
      this.client = client;
    }

    public void run() {
      ArrayList<Integer> values = new ArrayList<Integer>(total_keys);

      long value_sum = 0;
      for (int i = 0; i < total_keys; ++i) {
        values.add((int) (Math.random() * 1000));
      }

      int left_put = total_keys;
      int key_cursor = 0;
      int total_count = total_keys + removed_keys;

      for (int i = 1; i <= total_count; ++i) {
        if (i % 1000 == 0) {
          System.out.println(name + "round " + i);
        }
        int t = (int) (Math.random() * (total_count - i));
        if (t < left_put) {
          String key = name + String.valueOf(key_cursor);
          String value = String.valueOf(values.get(key_cursor));
          try {
            client.set(TABLE_NAME, key.getBytes(), null, value.getBytes(), 0);
          } catch (PException e) {
            e.printStackTrace();
            assertTrue(false);
          }
          --left_put;
          ++key_cursor;
        } else {
          int index = (int) (Math.random() * Math.min(key_cursor + 100, total_keys));
          String key = name + String.valueOf(index);
          try {
            client.del(TABLE_NAME, key.getBytes(), null);
            if (index < key_cursor) {
              values.set(index, 0);
            }
          } catch (PException e) {
            e.printStackTrace();
            assertTrue(false);
          }
        }
      }

      for (int i = 1; i <= total_keys; ++i) {
        if (i % 1000 == 0) {
          System.out.println(name + "get round " + i);
        }
        String key = name + String.valueOf(i - 1);
        try {
          byte[] resp = client.get(TABLE_NAME, key.getBytes(), null);
          if (resp != null) {
            value_sum += Integer.valueOf(new String(resp));
          }
        } catch (PException e) {
          e.printStackTrace();
        }
      }

      long expected_sum = 0;
      for (int v : values) {
        expected_sum += v;
      }
      assertEquals(expected_sum, value_sum);
    }
  }

  @Test
  public void testMultiThread() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();

    System.out.println("start to run multi-thread test");

    ArrayList<VisitThread> threadList = new ArrayList<VisitThread>();
    for (int i = 0; i < 10; ++i) {
      threadList.add(new VisitThread("Thread_" + String.valueOf(i) + "_", client));
    }
    for (VisitThread vt : threadList) {
      vt.start();
    }
    for (VisitThread vt : threadList) {
      while (true) {
        try {
          vt.join();
          break;
        } catch (InterruptedException e) {
        }
      }
    }

    PegasusClientFactory.closeSingletonClient();
  }

  //    private static final class FillThread extends Thread {
  //        private String name;
  //        private PegasusClientInterface client;
  //
  //        public FillThread(String aName, PegasusClientInterface aClient) {
  //            name = aName;
  //            client = aClient;
  //        }
  //
  //        public void run() {
  //            long i = 1;
  //            while (true) {
  //                String value = String.valueOf(i%10000);
  //                try {
  //                    client.set(TABLE_NAME, value.getBytes(), value.getBytes(),
  // value.getBytes());
  //                    i++;
  //                } catch (PException ex) {
  //                    ex.printStackTrace();
  //                }
  //
  //                if (i%1000 == 0) {
  //                    System.out.println(name + " round " + i);
  //                }
  //            }
  //        }
  //    }
  //
  //    @Test
  //    public void testFillValue() throws PException {
  //        PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
  //
  //        System.out.println("start to run multithread fill");
  //
  //        ArrayList<FillThread> ftList = new ArrayList<FillThread>();
  //        for (int i=0; i<3; ++i) {
  //            ftList.add(new FillThread("Thread" + String.valueOf(i), client));
  //        }
  //
  //        for (int i=0; i<3; ++i) {
  //            ftList.get(i).start();
  //        }
  //
  //        for (int i=0; i<3; ++i) {
  //            try {
  //                ftList.get(i).join();
  //            } catch (Exception ex) {
  //                ex.printStackTrace();
  //            }
  //        }
  //    }
}
