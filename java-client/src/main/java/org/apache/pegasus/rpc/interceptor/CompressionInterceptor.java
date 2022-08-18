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
import org.apache.pegasus.operator.ClientOperator;
import org.apache.pegasus.operator.RRDBCheckAndMutateOperator;
import org.apache.pegasus.operator.RRDBCheckAndSetOperator;
import org.apache.pegasus.operator.RRDBGetOperator;
import org.apache.pegasus.operator.RRDBMultiGetOperator;
import org.apache.pegasus.operator.RRDBMultiPutOperator;
import org.apache.pegasus.operator.RRDBPutOperator;
import org.apache.pegasus.operator.RRDBScanOperator;
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
    ClientOperator operator = clientRequestRound.getOperator();
    if (operator instanceof RRDBPutOperator) {
      RRDBPutOperator put = (RRDBPutOperator) operator;
      put.get_request().value.data = ZstdWrapper.compress(put.get_request().value.data);
      return;
    }

    if (operator instanceof RRDBMultiPutOperator) {
      List<key_value> kvs = ((RRDBMultiPutOperator) operator).get_request().kvs;
      for (key_value kv : kvs) {
        kv.value.data = ZstdWrapper.compress(kv.value.data);
      }
      return;
    }

    if (operator instanceof RRDBCheckAndSetOperator) {
      RRDBCheckAndSetOperator check_and_set = (RRDBCheckAndSetOperator) operator;
      check_and_set.get_request().set_value.data =
          ZstdWrapper.compress(check_and_set.get_request().set_value.data);
      return;
    }

    if (operator instanceof RRDBCheckAndMutateOperator) {
      List<mutate> mutates = ((RRDBCheckAndMutateOperator) operator).get_request().mutate_list;
      for (mutate mu : mutates) {
        mu.value.data = ZstdWrapper.compress(mu.value.data);
      }
    }
  }

  private void tryDecompress(ClientRequestRound clientRequestRound) {
    ClientOperator operator = clientRequestRound.getOperator();

    if (operator instanceof RRDBGetOperator) {
      RRDBGetOperator get = (RRDBGetOperator) operator;
      get.get_response().value.data = ZstdWrapper.tryDecompress(get.get_response().value.data);
      return;
    }

    if (operator instanceof RRDBMultiGetOperator) {
      List<key_value> kvs = ((RRDBMultiGetOperator) operator).get_response().kvs;
      for (key_value kv : kvs) {
        kv.value.data = ZstdWrapper.tryDecompress(kv.value.data);
      }
      return;
    }

    if (operator instanceof RRDBScanOperator) {
      List<key_value> kvs = ((RRDBScanOperator) operator).get_response().kvs;
      for (key_value kv : kvs) {
        kv.value.data = ZstdWrapper.tryDecompress(kv.value.data);
      }
    }
  }
}
