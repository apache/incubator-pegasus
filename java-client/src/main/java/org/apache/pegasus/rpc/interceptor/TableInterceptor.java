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

import org.apache.pegasus.base.error_code;
import org.apache.pegasus.rpc.async.ClientRequestRound;
import org.apache.pegasus.rpc.async.TableHandler;

public interface TableInterceptor {
  // The behavior before sending the RPC to a table.
  void before(ClientRequestRound clientRequestRound, TableHandler tableHandler);
  // The behavior after getting reply or failure of the RPC.
  void after(
      ClientRequestRound clientRequestRound,
      error_code.error_types errno,
      TableHandler tableHandler);
}
