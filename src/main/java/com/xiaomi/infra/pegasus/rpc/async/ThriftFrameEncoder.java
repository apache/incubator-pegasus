// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc.async;

import com.xiaomi.infra.pegasus.rpc.ThriftHeader;
import com.xiaomi.infra.pegasus.thrift.protocol.TBinaryProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;

/** Created by sunweijie@xiaomi.com on 16-11-9. */
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

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.warn(
        "got exception in outbound handler of {}, just ignore this: ",
        ctx.channel().toString(),
        cause);
  }
}
