// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

/**
 * @author qinzuoyan
 *     <p>Pegasus exception.
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
}
