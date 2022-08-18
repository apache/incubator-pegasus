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
package org.apache.pegasus.client;

import java.util.concurrent.TimeoutException;
import org.apache.pegasus.base.error_code;
import org.apache.pegasus.client.PegasusTable.Request;
import org.apache.pegasus.rpc.ReplicationException;

/**
 * The generic type of exception thrown by all of the Pegasus APIs.
 *
 * <p>Common strategies of handling PException include retrying, or ignoring. We recommend you to
 * log the exception for future debugging.
 */
public class PException extends Exception {
  private static final long serialVersionUID = 4436491238550521203L;
  private static final String VERSION_PREFIX = loadVersion() + ": ";

  public PException() {
    super();
  }

  public PException(String message, Throwable cause) {
    super(VERSION_PREFIX + message, cause);
  }

  public PException(String message) {
    super(VERSION_PREFIX + message);
  }

  public PException(Throwable cause) {
    super(VERSION_PREFIX + cause.toString(), cause);
  }

  static PException threadInterrupted(String tableName, InterruptedException e) {
    return new PException(
        new ReplicationException(
            error_code.error_types.ERR_THREAD_INTERRUPTED,
            String.format("[table=%s] Thread was interrupted: %s", tableName, e.getMessage())));
  }

  static PException timeout(
      String metaList, String tableName, Request request, int timeout, TimeoutException e) {
    return new PException(
        new ReplicationException(
            error_code.error_types.ERR_TIMEOUT,
            String.format(
                "[metaServer=%s, table=%s, request=%s, timeout=%dms] Timeout on Future await: %s",
                metaList, tableName, request.toString(), timeout, e.getMessage())));
  }

  private static String loadVersion() {
    String ver = PException.class.getPackage().getImplementationVersion();
    if (ver == null) {
      return "{version}";
    }
    return ver;
  }
}
