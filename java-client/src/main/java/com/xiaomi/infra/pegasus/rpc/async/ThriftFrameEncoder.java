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
package com.xiaomi.infra.pegasus.rpc.async;

import com.xiaomi.infra.pegasus.rpc.ThriftHeader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;

public class ThriftFrameEncoder extends MessageToByteEncoder<ReplicaSession.RequestEntry> {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(ThriftFrameEncoder.class);

  public ThriftFrameEncoder() {}

  @Override
  protected ByteBuf allocateBuffer(
      ChannelHandlerContext ctx, ReplicaSession.RequestEntry entry, boolean preferDirect)
      throws Exception {
    return preferDirect ? ctx.alloc().ioBuffer(256) : ctx.alloc().heapBuffer(256);
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, ReplicaSession.RequestEntry e, ByteBuf out)
      throws Exception {
    int initIndex = out.writerIndex();

    // write the Memory buffer
    out.writerIndex(initIndex + ThriftHeader.HEADER_LENGTH);
    TBinaryProtocol protocol = new TBinaryProtocol(new TByteBufTransport(out));

    // write meta
    e.op.prepare_thrift_meta(protocol, (int) e.timeoutMs, e.isBackupRequest);
    int meta_length = out.readableBytes() - ThriftHeader.HEADER_LENGTH;

    // write body
    e.op.send_data(protocol, e.sequenceId);

    // write header
    out.setBytes(
        initIndex,
        e.op.prepare_thrift_header(
            meta_length, out.readableBytes() - ThriftHeader.HEADER_LENGTH - meta_length));
  }
}
