package com.xiaomi.infra.pegasus.rpc.interceptor;

import com.xiaomi.infra.pegasus.apps.key_value;
import com.xiaomi.infra.pegasus.apps.mutate;
import com.xiaomi.infra.pegasus.base.error_code.error_types;
import com.xiaomi.infra.pegasus.operator.client_operator;
import com.xiaomi.infra.pegasus.operator.rrdb_check_and_mutate_operator;
import com.xiaomi.infra.pegasus.operator.rrdb_check_and_set_operator;
import com.xiaomi.infra.pegasus.operator.rrdb_get_operator;
import com.xiaomi.infra.pegasus.operator.rrdb_multi_get_operator;
import com.xiaomi.infra.pegasus.operator.rrdb_multi_put_operator;
import com.xiaomi.infra.pegasus.operator.rrdb_put_operator;
import com.xiaomi.infra.pegasus.operator.rrdb_scan_operator;
import com.xiaomi.infra.pegasus.rpc.async.ClientRequestRound;
import com.xiaomi.infra.pegasus.rpc.async.TableHandler;
import com.xiaomi.infra.pegasus.tools.ZstdWrapper;
import java.util.List;

public class CompressionInterceptor implements TableInterceptor {

  @Override
  public void before(ClientRequestRound clientRequestRound, TableHandler tableHandler) {
    tryCompress(clientRequestRound);
  }

  @Override
  public void after(
      ClientRequestRound clientRequestRound, error_types errno, TableHandler tableHandler) {
    if (errno != error_types.ERR_OK) {
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
