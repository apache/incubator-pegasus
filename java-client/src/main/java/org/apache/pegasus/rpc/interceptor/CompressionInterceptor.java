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

import java.util.List;
import org.apache.pegasus.apps.key_value;
import org.apache.pegasus.apps.mutate;
import org.apache.pegasus.base.error_code;
import org.apache.pegasus.operator.client_operator;
import org.apache.pegasus.operator.rrdb_check_and_mutate_operator;
import org.apache.pegasus.operator.rrdb_check_and_set_operator;
import org.apache.pegasus.operator.rrdb_get_operator;
import org.apache.pegasus.operator.rrdb_multi_get_operator;
import org.apache.pegasus.operator.rrdb_multi_put_operator;
import org.apache.pegasus.operator.rrdb_put_operator;
import org.apache.pegasus.operator.rrdb_scan_operator;
import org.apache.pegasus.rpc.async.ClientRequestRound;
import org.apache.pegasus.rpc.async.TableHandler;
import org.apache.pegasus.tools.ZstdWrapper;

public class CompressionInterceptor implements TableInterceptor {

  @Override
  public void before(ClientRequestRound clientRequestRound, TableHandler tableHandler) {
    tryCompress(clientRequestRound);
  }

  @Override
  public void after(
      ClientRequestRound clientRequestRound,
      error_code.error_types errno,
      TableHandler tableHandler) {
    if (errno != error_code.error_types.ERR_OK) {
      return;
    }
    tryDecompress(clientRequestRound);
  }

  private void tryCompress(ClientRequestRound clientRequestRound) {
    client_operator operator = clientRequestRound.getOperator();
    if (operator instanceof rrdb_put_operator) {
      rrdb_put_operator put = (rrdb_put_operator) operator;
      put.get_request().value.data = ZstdWrapper.compress(put.get_request().value.data);
      return;
    }

    if (operator instanceof rrdb_multi_put_operator) {
      List<key_value> kvs = ((rrdb_multi_put_operator) operator).get_request().kvs;
      for (key_value kv : kvs) {
        kv.value.data = ZstdWrapper.compress(kv.value.data);
      }
      return;
    }

    if (operator instanceof rrdb_check_and_set_operator) {
      rrdb_check_and_set_operator check_and_set = (rrdb_check_and_set_operator) operator;
      check_and_set.get_request().set_value.data =
          ZstdWrapper.compress(check_and_set.get_request().set_value.data);
      return;
    }

    if (operator instanceof rrdb_check_and_mutate_operator) {
      List<mutate> mutates = ((rrdb_check_and_mutate_operator) operator).get_request().mutate_list;
      for (mutate mu : mutates) {
        mu.value.data = ZstdWrapper.compress(mu.value.data);
      }
    }
  }

  private void tryDecompress(ClientRequestRound clientRequestRound) {
    client_operator operator = clientRequestRound.getOperator();

    if (operator instanceof rrdb_get_operator) {
      rrdb_get_operator get = (rrdb_get_operator) operator;
      get.get_response().value.data = ZstdWrapper.tryDecompress(get.get_response().value.data);
      return;
    }

    if (operator instanceof rrdb_multi_get_operator) {
      List<key_value> kvs = ((rrdb_multi_get_operator) operator).get_response().kvs;
      for (key_value kv : kvs) {
        kv.value.data = ZstdWrapper.tryDecompress(kv.value.data);
      }
      return;
    }

    if (operator instanceof rrdb_scan_operator) {
      List<key_value> kvs = ((rrdb_scan_operator) operator).get_response().kvs;
      for (key_value kv : kvs) {
        kv.value.data = ZstdWrapper.tryDecompress(kv.value.data);
      }
    }
  }
}
