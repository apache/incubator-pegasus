// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc;

import com.xiaomi.infra.pegasus.base.error_code;

public class ReplicationException extends Exception {
  private static final long serialVersionUID = 4186015142427786503L;

  private error_code.error_types errType;

  public ReplicationException() {
    super();
  }

  public ReplicationException(error_code.error_types t) {
    super(t.name());
    errType = t;
  }

  public ReplicationException(error_code.error_types t, String message) {
    super(t.name() + (message.isEmpty() ? "" : (": " + message)));
    errType = t;
  }

  public ReplicationException(Throwable cause) {
    super(cause);
    errType = error_code.error_types.ERR_UNKNOWN;
  }

  public ReplicationException(String message, Throwable cause) {
    super(message, cause);
  }

  public error_code.error_types getErrorType() {
    return errType;
  }
}
