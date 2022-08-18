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
package org.apache.pegasus.rpc.interceptor;

import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pegasus.base.error_code;
import org.apache.pegasus.rpc.async.ClientRequestRound;
import org.apache.pegasus.rpc.async.ReplicaSession;
import org.apache.pegasus.rpc.async.TableHandler;

public class BackupRequestInterceptor implements TableInterceptor {

  private static final Random RANDOM = new Random();
  private final long backupRequestDelayMs;

  public BackupRequestInterceptor(long backupRequestDelayMs) {
    this.backupRequestDelayMs = backupRequestDelayMs;
  }

  @Override
  public void before(ClientRequestRound clientRequestRound, TableHandler tableHandler) {
    backupCall(clientRequestRound, tableHandler);
  }

  @Override
  public void after(
      ClientRequestRound clientRequestRound,
      error_code.error_types errno,
      TableHandler tableHandler) {
    // cancel the backup request task
    ScheduledFuture<?> backupRequestTask = clientRequestRound.backupRequestTask();
    if (backupRequestTask != null) {
      backupRequestTask.cancel(true);
    }
  }

  private void backupCall(ClientRequestRound clientRequestRound, TableHandler tableHandler) {
    if (!clientRequestRound.getOperator().supportBackupRequest()) {
      return;
    }

    final TableHandler.ReplicaConfiguration handle =
        tableHandler.getReplicaConfig(clientRequestRound.getOperator().getgpid().get_pidx());

    clientRequestRound.backupRequestTask(
        tableHandler
            .getExecutor()
            .schedule(
                () -> {
                  // pick a secondary at random
                  ReplicaSession secondarySession =
                      handle.secondarySessions.get(RANDOM.nextInt(handle.secondarySessions.size()));
                  secondarySession.asyncSend(
                      clientRequestRound.getOperator(),
                      () ->
                          tableHandler.onRpcReply(
                              clientRequestRound,
                              tableHandler.updateVersion(),
                              secondarySession.name()),
                      clientRequestRound.timeoutMs(),
                      true);
                },
                backupRequestDelayMs,
                TimeUnit.MILLISECONDS));
  }
}
