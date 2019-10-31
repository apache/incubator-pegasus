// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc;

import java.nio.ByteBuffer;

public class ThriftHeader {
  public static final int HEADER_LENGTH = 48;
  static final byte[] HEADER_TYPE = {'T', 'H', 'F', 'T'};
  public int hdr_version = 0;
  public int header_length;
  public int header_crc32 = 0;
  public int body_length;
  public int body_crc32 = 0;
  public int app_id;
  public int partition_index;
  public int client_timeout = 0;
  public int thread_hash;
  public long partition_hash;

  public byte[] toByteArray() {
    ByteBuffer bf = ByteBuffer.allocate(HEADER_LENGTH);
    bf.put(HEADER_TYPE);
    bf.putInt(hdr_version);
    bf.putInt(header_length);
    bf.putInt(header_crc32);
    bf.putInt(body_length);
    bf.putInt(body_crc32);
    bf.putInt(app_id);
    bf.putInt(partition_index);
    bf.putInt(client_timeout);
    bf.putInt(thread_hash);
    bf.putLong(partition_hash);
    return bf.array();
  }
}
