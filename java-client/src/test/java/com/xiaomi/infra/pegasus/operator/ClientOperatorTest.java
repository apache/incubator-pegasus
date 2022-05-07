package com.xiaomi.infra.pegasus.operator;

import com.xiaomi.infra.pegasus.apps.check_and_mutate_request;
import com.xiaomi.infra.pegasus.apps.multi_get_request;
import com.xiaomi.infra.pegasus.apps.update_request;
import com.xiaomi.infra.pegasus.base.blob;
import com.xiaomi.infra.pegasus.base.gpid;
import org.junit.Assert;
import org.junit.Test;

public class ClientOperatorTest {

  @Test
  public void testSupportBackupRequest() {
    client_operator op =
        new rrdb_multi_get_operator(new gpid(1, 1), "test", new multi_get_request(), 0);
    Assert.assertTrue(op.supportBackupRequest());

    op = new rrdb_get_operator(new gpid(1, 1), "test", new blob(), 0);
    Assert.assertTrue(op.supportBackupRequest());

    op = new rrdb_ttl_operator(new gpid(1, 1), "test", new blob(), 0);
    Assert.assertTrue(op.supportBackupRequest());

    op = new rrdb_put_operator(new gpid(1, 1), "test", new update_request(), 0);
    Assert.assertFalse(op.supportBackupRequest());

    op =
        new rrdb_check_and_mutate_operator(
            new gpid(1, 1), "test", new check_and_mutate_request(), 0);
    Assert.assertFalse(op.supportBackupRequest());
  }
}
