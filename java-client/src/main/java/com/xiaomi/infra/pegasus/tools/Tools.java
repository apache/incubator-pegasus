// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.tools;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;

public class Tools {
  private static class dsn_crc {
    public static final long crc64_poly = 0x9a6c9329ac4bc9b5l;
    public static final int crc32_poly = 0x82f63b78;
    public static final int crc32_table[] = new int[0x100];
    public static final long crc64_table[] = new long[0x100];

    static {
      for (int i = 0; i < 256; ++i) {
        int k1 = i;
        long k2 = (long) i;
        for (int j = 0; j < 8; ++j) {
          if ((k1 & 1) == 1) k1 = (k1 >>> 1) ^ crc32_poly;
          else k1 = (k1 >>> 1);

          if ((k2 & 1) == 1) k2 = (k2 >>> 1) ^ crc64_poly;
          else k2 = (k2 >>> 1);
        }
        crc32_table[i] = k1;
        crc64_table[i] = k2;
      }
    }
  }

  public static void sleepFor(long ms) {
    long startTime = System.currentTimeMillis();
    long elapse;
    while (ms > 0) {
      try {
        Thread.sleep(ms);
        break;
      } catch (InterruptedException e) {
        elapse = System.currentTimeMillis() - startTime;
        ms -= elapse;
        startTime += elapse;
      }
    }
  }

  public static void waitForever(Object obj) {
    try {
      obj.wait();
    } catch (InterruptedException e) {
    }
  }

  public static long waitFor(Object obj, long millseconds) {
    long current = System.currentTimeMillis();
    try {
      obj.wait(millseconds);
    } catch (InterruptedException e) {
    }
    return System.currentTimeMillis() - current;
  }

  public static void notify(Object obj) {
    synchronized (obj) {
      obj.notify();
    }
  }

  public static <T> T waitUninterruptable(FutureTask<T> task, long millseconds)
      throws ExecutionException {
    long current = System.currentTimeMillis();
    while (millseconds >= 0) {
      try {
        T result = task.get(millseconds, TimeUnit.MILLISECONDS);
        return result;
      } catch (InterruptedException e) {
        long t = System.currentTimeMillis();
        millseconds -= (t - current);
        current = t;
      } catch (TimeoutException e) {
        return null;
      }
    }
    return null;
  }

  public static int dsn_crc32(byte[] array) {
    return dsn_crc32(array, 0, array.length);
  }

  public static int dsn_crc32(byte[] array, int offset, int length) {
    int crc = -1;
    int end = offset + length;
    for (int i = offset; i < end; ++i)
      crc = dsn_crc.crc32_table[(array[i] ^ crc) & 0xFF] ^ (crc >>> 8);
    return ~crc;
  }

  public static long dsn_crc64(byte[] array) {
    return dsn_crc64(array, 0, array.length);
  }

  public static long dsn_crc64(byte[] array, int offset, int length) {
    long crc = -1;
    int end = offset + length;
    for (int i = offset; i < end; ++i)
      crc = dsn_crc.crc64_table[(array[i] ^ (int) crc) & 0xFF] ^ (crc >>> 8);
    return ~crc;
  }

  public static int dsn_gpid_to_thread_hash(int app_id, int partition_index) {
    return app_id * 7919 + partition_index;
  }

  private static final long epoch_begin = 1451606400; // seconds since 2016.01.01-00:00:00 GMT

  public static long epoch_now() {
    Date d = new Date();
    return d.getTime() / 1000 - epoch_begin;
  }

  public static long unixEpochMills() {
    Date d = new Date();
    return d.getTime();
  }

  public static InetAddress getLocalHostAddress() {
    try {
      Iterator e = Collections.list(NetworkInterface.getNetworkInterfaces()).iterator();

      while (e.hasNext()) {
        NetworkInterface iface = (NetworkInterface) e.next();
        Iterator var2 = Collections.list(iface.getInetAddresses()).iterator();

        while (var2.hasNext()) {
          InetAddress addr = (InetAddress) var2.next();
          logger.debug("Checking ip address {}", addr);
          // 172.17.42.1 is docker address
          if (addr.isSiteLocalAddress()
              && !addr.isLoopbackAddress()
              && !(addr instanceof Inet6Address)
              && !(addr.getHostAddress().equals("172.17.42.1"))) {
            logger.debug("Ok, the ip {} will be used.", addr);
            return addr;
          }
        }
      }
    } catch (SocketException var5) {
      logger.error("Couldn\'t find the local machine ip.", var5);
    }
    throw new IllegalStateException("Couldn\'t find the local machine ip.");
  }

  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(Tools.class);
}
