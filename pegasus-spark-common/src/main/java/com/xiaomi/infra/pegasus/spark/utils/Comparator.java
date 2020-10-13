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
