// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

/**
 * @author qinzuoyan
 */

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by mi on 18-7-10.
 */
public class TestIncr {
    @Test
    public void testPing() throws PException {
        PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
        String tableName = "temp";

        byte[] hashKey = "incrkeyforjava".getBytes();
        byte[] sortKey = "0".getBytes();
        byte[] value = "".getBytes();

        try {
            System.out.println("set value ...");
            client.set(tableName, hashKey, sortKey, value, 0);
            System.out.println("set value ok");

            System.out.println("incr to empty value ...");
            long result = client.incr(tableName, hashKey, sortKey, 100);
            Assert.assertEquals(100, result);
            System.out.println("incr to empty value ok");

            System.out.println("incr zero ...");
            result = client.incr(tableName, hashKey, sortKey, 0);
            Assert.assertEquals(100, result);
            System.out.println("incr zero ok");

            System.out.println("incr negative ...");
            result = client.incr(tableName, hashKey, sortKey, -1);
            Assert.assertEquals(99, result);
            System.out.println("incr negative ok");

            System.out.println("del value ...");
            client.del(tableName, hashKey,sortKey);
            System.out.println("del value ok");

            System.out.println("incr to un-exist value ...");
            result = client.incr(tableName, hashKey, sortKey, 200);
            Assert.assertEquals(200, result);
            System.out.println("incr to un-exist value ok");

            System.out.println("del value ...");
            client.del(tableName, hashKey,sortKey);
            System.out.println("del value ok");
        }
        catch (PException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        PegasusClientFactory.closeSingletonClient();
    }
}
