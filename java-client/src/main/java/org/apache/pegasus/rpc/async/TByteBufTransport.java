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

import org.apache.thrift.transport.TTransport;

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
