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

package org.apache.pegasus.operator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.pegasus.apps.check_and_mutate_request;
import org.apache.pegasus.apps.multi_get_request;
import org.apache.pegasus.apps.update_request;
import org.apache.pegasus.base.blob;
import org.apache.pegasus.base.gpid;
import org.junit.jupiter.api.Test;

public class ClientOperatorTest {

  @Test
  public void testSupportBackupRequest() {
    client_operator op =
        new rrdb_multi_get_operator(new gpid(1, 1), "test", new multi_get_request(), 0);
    assertTrue(op.supportBackupRequest());

    op = new rrdb_get_operator(new gpid(1, 1), "test", new blob(), 0);
    assertTrue(op.supportBackupRequest());

    op = new rrdb_ttl_operator(new gpid(1, 1), "test", new blob(), 0);
    assertTrue(op.supportBackupRequest());

    op = new rrdb_put_operator(new gpid(1, 1), "test", new update_request(), 0);
    assertFalse(op.supportBackupRequest());

    op =
        new rrdb_check_and_mutate_operator(
            new gpid(1, 1), "test", new check_and_mutate_request(), 0);
    assertFalse(op.supportBackupRequest());
  }
}
