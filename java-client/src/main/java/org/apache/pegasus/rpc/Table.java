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
package org.apache.pegasus.rpc;

import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import org.apache.pegasus.base.gpid;
import org.apache.pegasus.operator.ClientOperator;

public abstract class Table {

  protected String tableName;
  protected int appID;
  protected KeyHasher hasher;

  public interface ClientOPCallback {
    public void onCompletion(ClientOperator clientOP) throws Throwable;
  }

  public final long getHash(byte[] data) {
    return hasher.hash(data);
  }

  public final long getKeyHash(byte[] data) {
    return KeyHasher.DEFAULT.hash(data);
  }

  public final gpid[] getAllGpid() {
    int count = getPartitionCount();
    gpid[] ret = new gpid[count];
    for (int i = 0; i < count; i++) {
      ret[i] = new gpid(appID, i);
    }
    return ret;
  }

  public final String getTableName() {
    return tableName;
  }

  public final int getAppID() {
    return appID;
  }

  public final <T> DefaultPromise<T> newPromise() {
    return new DefaultPromise<T>(getExecutor());
  }

  public abstract int getDefaultTimeout();

  public abstract int getPartitionCount();

  public abstract void operate(ClientOperator op, int timeoutMs) throws ReplicationException;

  public abstract void asyncOperate(ClientOperator op, ClientOPCallback callback, int timeoutMs);

  public abstract gpid getGpidByHash(long hashValue);

  public abstract EventExecutor getExecutor();

  public static long remainder_unsigned(long dividend, long divisor) {
    if (dividend > 0) {
      return dividend % divisor;
    }
    long reminder = (dividend >>> 1) % divisor * 2 + (dividend & 1);
    return reminder >= 0 && reminder < divisor ? reminder : reminder - divisor;
  }
}
