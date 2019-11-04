// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import com.xiaomi.infra.pegasus.base.error_code;
import com.xiaomi.infra.pegasus.rpc.ReplicationException;
import java.util.concurrent.TimeoutException;

/**
 * The generic type of exception thrown by all of the Pegasus APIs.
 *
 * <p>Common strategies of handling PException include retrying, or ignoring. We recommend you to
 * log the exception for future debugging.
 */
public class PException extends Exception {
  private static final long serialVersionUID = 4436491238550521203L;

  public PException() {
    super();
  }

  public PException(String message, Throwable cause) {
    super(message, cause);
  }

  public PException(String message) {
    super(message);
  }

  public PException(Throwable cause) {
    super(cause);
  }

  static PException threadInterrupted(String tableName, InterruptedException e) {
    return new PException(
        new ReplicationException(
            error_code.error_types.ERR_THREAD_INTERRUPTED,
            String.format("[table=%s] Thread was interrupted: %s", tableName, e.getMessage())));
  }

  static PException timeout(String tableName, int timeout, TimeoutException e) {
    return new PException(
        new ReplicationException(
            error_code.error_types.ERR_TIMEOUT,
            String.format(
                "[table=%s, timeout=%dms] Timeout on Future await: %s",
                tableName, timeout, e.getMessage())));
  }
}
