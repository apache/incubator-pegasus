package com.xiaomi.infra.pegasus.spark.common.utils;

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
        int b1int = b1 & 0xff;
        int b2int = byteArray2[pos] & 0xff;

        if (b1int == b2int) {
          pos++;
        } else if (b1int < b2int) {
          return -1;
        } else {
          return 1;
        }
      }

      return -1;
    } else {
      int pos = 0;

      for (byte b2 : byteArray2) {
        int b1int = byteArray1[pos] & 0xff;
        int b2int = b2 & 0xff;

        if (b1int == b2int) {
          pos++;
        } else if (b1int < b2int) {
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
