// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc.async;

import org.apache.thrift.transport.TTransport;

/** Created by sunweijie@xiaomi.com on 16-11-9. */
public class TByteBufTransport extends TTransport {
  private io.netty.buffer.ByteBuf buffer_;

  public TByteBufTransport(io.netty.buffer.ByteBuf b) {
    buffer_ = b;
  }

  public boolean isOpen() {
    return true;
  }

  public void open() {}

  public void close() {}

  public int read(byte[] buf, int off, int len) {
    if (buffer_.readableBytes() < len) len = buffer_.readableBytes();
    buffer_.readBytes(buf, off, len);
    return len;
  }

  public void write(byte[] buf, int off, int len) {
    buffer_.writeBytes(buf, off, len);
  }

  public String toString(String enc) {
    return "";
  }

  public int length() {
    return buffer_.readableBytes();
  }

  public byte[] getArray() {
    return buffer_.array();
  }
}
