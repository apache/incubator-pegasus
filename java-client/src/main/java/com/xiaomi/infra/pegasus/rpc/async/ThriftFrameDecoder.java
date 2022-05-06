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

import com.xiaomi.infra.pegasus.base.error_code;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.slf4j.Logger;

/** Created by sunweijie@xiaomi.com on 16-11-9. */
public class ThriftFrameDecoder extends ByteToMessageDecoder {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(ThriftFrameDecoder.class);

  private ReplicaSession session;

  public ThriftFrameDecoder(ReplicaSession s) {
    session = s;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws TException {
    if (in.readableBytes() < 4) return;

    in.markReaderIndex();
    int frameSize = in.readInt() - 4;
    if (in.readableBytes() < frameSize) {
      in.resetReaderIndex();
      return;
    }

    int nextReaderIndex = in.readerIndex() + frameSize;
    TBinaryProtocol iprot = new TBinaryProtocol(new TByteBufTransport(in));
    com.xiaomi.infra.pegasus.base.error_code ec = new com.xiaomi.infra.pegasus.base.error_code();

    try {
      ec.read(iprot);
      TMessage msgHeader = iprot.readMessageBegin();
      if (session.filter != null && session.filter.abandonIt(ec.errno, msgHeader)) {
        logger.info(
            "{}: abaondon a message, err({}), header({})",
            ctx.channel().toString(),
            ec.errno.toString(),
            msgHeader.toString());
      } else {
        ReplicaSession.RequestEntry e = session.getAndRemoveEntry(msgHeader.seqid);
        if (e != null) {
          if (e.timeoutTask != null) {
            e.timeoutTask.cancel(true);
          }
          e.op.rpc_error.errno = ec.errno;
          if (e.op.rpc_error.errno == error_code.error_types.ERR_OK) {
            try {
              e.op.recv_data(iprot);
            } catch (TException readException) {
              logger.error(
                  "{}: unable to parse message body [seqId: {}, error: {}]",
                  ctx.channel().toString(),
                  msgHeader.seqid,
                  readException);
              e.op.rpc_error.errno = error_code.error_types.ERR_INVALID_DATA;
            }
          }
          out.add(e);
        } else {
          logger.info("{}: {} removed, perhaps timeout", ctx.channel().toString(), msgHeader.seqid);
        }
      }
    } catch (TException e) {
      logger.error("{}: got exception in thrift decode: ", ctx.channel().toString(), e);
      throw e;
    } finally {
      in.readerIndex(nextReaderIndex);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.warn(
        "{} for session {} got exception in inbound handler: ",
        ctx.channel().toString(),
        session.name(),
        cause);
    super.exceptionCaught(ctx, cause);
  }
}
