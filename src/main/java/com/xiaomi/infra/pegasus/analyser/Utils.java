package com.xiaomi.infra.pegasus.analyser;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class Utils {

  public static Pair<byte[], byte[]> restoreKey(byte[] key) {
    Validate.isTrue(key != null && key.length >= 2);
    ByteBuffer buf = ByteBuffer.wrap(key);
    int hashKeyLen = 0xFFFF & buf.getShort();
    Validate.isTrue(hashKeyLen != 0xFFFF && (2 + hashKeyLen <= key.length));
    return new ImmutablePair<byte[], byte[]>(
        Arrays.copyOfRange(key, 2, 2 + hashKeyLen),
        Arrays.copyOfRange(key, 2 + hashKeyLen, key.length));
  }

  public static byte[] restoreValue(byte[] value) {
    return Arrays.copyOfRange(value, 4, value.length);
  }
}
