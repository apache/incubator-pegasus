// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package dsn.rpc.async; 

import dsn.api.KeyHasher;
import dsn.api.ReplicationException;

import dsn.base.error_code;
import dsn.base.error_code.error_types;
import dsn.base.rpc_address;
import dsn.operator.*;

import dsn.rpc.async.TableHandler.ReplicaConfiguration;
import dsn.tools.Toollet;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.slf4j.Logger;

import java.util.ArrayList;

/**
* TableHandler Tester.
*
* @author sunweijie@xiaomi.com
* @version 1.0
*/
public class TableHandlerTest {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(TableHandlerTest.class);

    private String[] addr_list = {"127.0.0.1:34601", "127.0.0.1:34602", "127.0.0.1:34603"};
    private String[] replica_servers = {"127.0.0.1:34801", "127.0.0.1:34802", "127.0.01:34803"};

    private ClusterManager testManager;

    @Before
    public void before() throws Exception {
        testManager = new ClusterManager(1000, 1, null, addr_list);
    }

    @After
    public void after() throws Exception {
    }

    private dsn.base.rpc_address getValidWrongServer(final dsn.base.rpc_address right_address) {
        ArrayList<dsn.base.rpc_address> replicas = new ArrayList<rpc_address>();
        for (int i=0; i<replica_servers.length; ++i) {
            dsn.base.rpc_address a = new dsn.base.rpc_address();
            boolean ans = a.fromString(replica_servers[i]);
            assert ans;

            if (a.get_port() != right_address.get_port())
                replicas.add(a);
        }

        int p = (int)(Math.random()*replicas.size());
        return replicas.get(p);
    }

    /**
     * Method: operate(client_operator op)
     */
    @Test
    public void testOperateOp() throws Exception {
        System.out.println("test synchronized opearate");
        TableHandler table = null;
        try {
            table = testManager.openTable("temp", KeyHasher.DEFAULT);
        } catch (ReplicationException e) {
            Assert.fail();
        }
        Assert.assertNotNull(table);

        dsn.apps.update_request request = new dsn.apps.update_request();
        request.key = new dsn.base.blob("hello".getBytes());
        request.value = new dsn.base.blob("value".getBytes());
        final dsn.base.gpid pid = table.getGpid(request.key.data);

        ReplicaConfiguration handle = table.getReplicaConfig(pid.get_pidx());

        //1. modify the replica handler to a not exist one
        final dsn.base.rpc_address old_addr = handle.session.getAddress();
        logger.info("the right primary for {} is {}", pid.toString(), old_addr.toString());

        dsn.base.rpc_address addr = new dsn.base.rpc_address();
        addr.fromString("127.0.0.1:123");
        handle.ballot--;
        handle.session = testManager.getReplicaSession(addr);

        client_operator op = new Toollet.test_operator(pid, request);

        try {
            table.operate(op, 0);
            Assert.fail();
        } catch (ReplicationException ex) {
            Assert.assertEquals(error_types.ERR_TIMEOUT, ex.err_type);
        }

        //we should try to query meta accordingly
        final TableHandler finalTableRef = table;
        boolean ans = Toollet.waitCondition(new Toollet.BoolCallable() {
            @Override
            public boolean call() {
                ReplicaSession session = finalTableRef.getReplicaConfig(pid.get_pidx()).session;
                if (session == null)
                    return false;
                return session.getAddress().equals(old_addr);
            }
        }, 10);
        Assert.assertTrue(ans);

        //2. set an invalid task code, server should not response
        op = new Toollet.test_operator(pid, request);
        try {
            table.operate(op, 0);
            Assert.fail();
        } catch (ReplicationException ex) {
            Assert.assertEquals(error_code.error_types.ERR_TIMEOUT, ex.err_type);
        }

        // 3. we should open a onebox cluster with three replica servers. thus every
        // server will server all the replicas. Then we can test query a request to secondary
        handle = table.getReplicaConfig(pid.get_pidx());
        addr = getValidWrongServer(old_addr);
        logger.info("the wrong valid server is {}", addr.toString());

        Assert.assertFalse(addr.equals(old_addr));
        handle.ballot--;
        handle.session = testManager.getReplicaSession(addr);

        op = new Toollet.test_operator(pid, request);
        try {
            table.operate(op, 0);
            Assert.fail();
        } catch (ReplicationException ex) {
            Assert.assertEquals(error_types.ERR_TIMEOUT, ex.err_type);
        }
    }

    /**
     * Method: tryQueryMeta(final dsn.base.gpid pid, final long signature)
     */
    @Test
    public void testTryQueryMeta() throws Exception {
        System.out.println("test try query meta");
        TableHandler table = null;

        try {
            table = testManager.openTable("temp", KeyHasher.DEFAULT);
        } catch (ReplicationException e) {
            Assert.fail();
        }
        Assert.assertNotNull(table);
        Assert.assertEquals(8, table.getPartitionCount());

        TableHandler.TableConfiguration tableConfig = table.tableConfig_.get();
        for (int i=0; i<tableConfig.replicas.size(); ++i) {
            ReplicaConfiguration handle = tableConfig.replicas.get(i);
            Assert.assertNotNull(handle);
            Assert.assertNotNull(handle.session);
        }

        //mark a handler to inactive
        ReplicaConfiguration handle = tableConfig.replicas.get(0);
        long oldBallot = handle.ballot - 1;
        handle.ballot = oldBallot;
        handle.session = null;

        boolean doTheQuerying = table.tryQueryMeta(tableConfig.updateVersion);
        Assert.assertTrue(doTheQuerying);

        final TableHandler finalRef = table;
        Assert.assertTrue( Toollet.waitCondition(
                new Toollet.BoolCallable() {
                    @Override
                    public boolean call() {
                        return finalRef.getReplicaConfig(0).session != null;
                    }
                },
                10
        ) );

        handle = table.getReplicaConfig(0);
        Assert.assertEquals(oldBallot+1, handle.ballot);
    }
}
