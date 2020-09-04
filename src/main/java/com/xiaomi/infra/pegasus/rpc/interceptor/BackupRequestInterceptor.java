package com.xiaomi.infra.pegasus.rpc.interceptor;

import com.xiaomi.infra.pegasus.base.error_code.error_types;
import com.xiaomi.infra.pegasus.rpc.async.ClientRequestRound;
import com.xiaomi.infra.pegasus.rpc.async.ReplicaSession;
import com.xiaomi.infra.pegasus.rpc.async.TableHandler;
import com.xiaomi.infra.pegasus.rpc.async.TableHandler.ReplicaConfiguration;
import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class BackupRequestInterceptor implements TableInterceptor {
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
      ClientRequestRound clientRequestRound, error_types errno, TableHandler tableHandler) {
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

    final ReplicaConfiguration handle =
        tableHandler.getReplicaConfig(clientRequestRound.getOperator().get_gpid().get_pidx());

    clientRequestRound.backupRequestTask(
        tableHandler
            .getExecutor()
            .schedule(
                () -> {
                  // pick a secondary at random
                  ReplicaSession secondarySession =
                      handle.secondarySessions.get(
                          new Random().nextInt(handle.secondarySessions.size()));
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
