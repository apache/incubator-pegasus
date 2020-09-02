package com.xiaomi.infra.pegasus.rpc.interceptor;

import com.xiaomi.infra.pegasus.base.error_code.error_types;
import com.xiaomi.infra.pegasus.rpc.async.ClientRequestRound;
import com.xiaomi.infra.pegasus.rpc.async.TableHandler;

public interface TableInterceptor {
  // The behavior before sending the RPC to a table.
  void before(ClientRequestRound clientRequestRound, TableHandler tableHandler);
  // The behavior after getting reply or failure of the RPC.
  void after(ClientRequestRound clientRequestRound, error_types errno, TableHandler tableHandler);
}
