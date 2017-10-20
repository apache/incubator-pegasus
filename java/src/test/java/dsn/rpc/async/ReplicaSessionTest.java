// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package dsn.rpc.async;

import dsn.base.error_code;
import dsn.tools.Toollet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import dsn.operator.*;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/** 
* ReplicaSession Tester. 
* 
* @author sunweijie@xiaomi.com
* @version 1.0 
*/ 
public class ReplicaSessionTest {
    private String[] metaList = {"127.0.0.1:34601", "127.0.0.1:34602", "127.0.0.1:34603"};
    private final Logger logger = org.slf4j.LoggerFactory.getLogger(ReplicaSessionTest.class);
    private ClusterManager manager;

    @Before
    public void before() throws Exception {
        manager = new ClusterManager(1000, 1, null, metaList);
    }
    
    @After
    public void after() throws Exception { 
    } 

    /**
     * Method: connect() 
     */ 
    @Test
    public void testConnect() throws Exception {
        //test1: connect to a invalid address
        dsn.base.rpc_address addr = new dsn.base.rpc_address();
        addr.fromString("127.0.0.1:12345");
        ReplicaSession rs = manager.getReplicaSession(addr);

        ArrayList<FutureTask<Void> > callbacks = new ArrayList<FutureTask<Void>>();

        for (int i=0; i<100; ++i) {
            final client_operator op = new rrdb_put_operator(new dsn.base.gpid(-1, -1), null);
            final FutureTask<Void> cb = new FutureTask<Void>(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    Assert.assertEquals(error_code.error_types.ERR_SESSION_RESET, op.rpc_error.errno);
                    return null;
                }
            });

            callbacks.add(cb);
            rs.asyncSend(op, cb, 1000);
        }

        for (FutureTask<Void> cb: callbacks) {
            try {
                dsn.utils.tools.waitUninterruptable(cb, Integer.MAX_VALUE);
            } catch (ExecutionException e) {
                Assert.fail();
            }
        }

        final ReplicaSession cp_rs = rs;
        Toollet.waitCondition(new Toollet.BoolCallable() {
            @Override
            public boolean call() {
                return ReplicaSession.ConnState.DISCONNECTED==cp_rs.getState();
            }
        }, 5);

        //test2: connect to an valid address, and then close the server
        addr.fromString("127.0.0.1:34801");
        callbacks.clear();

        rs = manager.getReplicaSession(addr);
        for (int i=0; i<20; ++i) {
            // we send query request to replica server. We expect it to discard it.
            final int index = i;
            dsn.apps.update_request req = new dsn.apps.update_request(
                    new dsn.base.blob("hello".getBytes()),
                    new dsn.base.blob("world".getBytes()),
                    0);

            final client_operator op = new Toollet.test_operator(new dsn.base.gpid(-1, -1), req);
            final dsn.base.rpc_address cp_addr = addr;
            final FutureTask<Void> cb = new FutureTask<Void>(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    Assert.assertEquals(error_code.error_types.ERR_TIMEOUT, op.rpc_error.errno);
                    // for the last request, we kill the server
                    if (index == 19) {
                        dsn.tools.Toollet.closeServer(cp_addr);
                    }
                    return null;
                }
            });

            callbacks.add(cb);
            rs.asyncSend(op, cb, 500);
        }

        for (int i=0; i<80; ++i) {
            // then we still send query request to replica server. But the timeout is longer.
            dsn.apps.update_request req = new dsn.apps.update_request(
                    new dsn.base.blob("hello".getBytes()),
                    new dsn.base.blob("world".getBytes()),
                    0);
            final client_operator op = new Toollet.test_operator(new dsn.base.gpid(-1, -1), req);
            final FutureTask<Void> cb = new FutureTask<Void>(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    Assert.assertEquals(error_code.error_types.ERR_SESSION_RESET, op.rpc_error.errno);
                    return null;
                }
            });

            callbacks.add(cb);
            //these requests have longer timeout, so they should be responsed later than the server is killed
            rs.asyncSend(op, cb, 2000);
        }

        for (FutureTask<Void> cb: callbacks) {
            try {
                dsn.utils.tools.waitUninterruptable(cb, Integer.MAX_VALUE);
            } catch (ExecutionException e) {
                e.printStackTrace();
                Assert.fail();
            }
        }

        dsn.tools.Toollet.tryStartServer(addr);
    }
}
