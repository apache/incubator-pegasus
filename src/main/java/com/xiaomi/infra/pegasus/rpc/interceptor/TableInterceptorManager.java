package com.xiaomi.infra.pegasus.rpc.interceptor;

import com.xiaomi.infra.pegasus.base.error_code.error_types;
import com.xiaomi.infra.pegasus.client.TableOptions;
import com.xiaomi.infra.pegasus.rpc.async.ClientRequestRound;
import com.xiaomi.infra.pegasus.rpc.async.TableHandler;
import java.util.ArrayList;
import java.util.List;

public class TableInterceptorManager {

  private List<TableInterceptor> interceptors = new ArrayList<>();

  public TableInterceptorManager(TableOptions options) {
    if (options.enableBackupRequest()) {
      interceptors.add(new BackupRequestInterceptor(options.backupRequestDelayMs()));
    }

    if (options.enableCompression()) {
      interceptors.add(new CompressionInterceptor());
    }
  }

  public void before(ClientRequestRound clientRequestRound, TableHandler tableHandler) {
    for (TableInterceptor interceptor : interceptors) {
      interceptor.before(clientRequestRound, tableHandler);
    }
  }

  public void after(
      ClientRequestRound clientRequestRound, error_types errno, TableHandler tableHandler) {
    for (TableInterceptor interceptor : interceptors) {
      interceptor.after(clientRequestRound, errno, tableHandler);
    }
  }
}
