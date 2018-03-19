// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc.async;

import com.xiaomi.infra.pegasus.base.error_code.error_types;
import com.xiaomi.infra.pegasus.base.rpc_address;
import com.xiaomi.infra.pegasus.operator.client_operator;
import com.xiaomi.infra.pegasus.operator.query_cfg_operator;
import io.netty.channel.EventLoopGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/**
 * Created by weijiesun on 17-9-13.
 */
public class MetaSession {
    public MetaSession(ClusterManager manager, String addrList[],
                       int eachQueryTimeoutInMills, int defaultMaxQueryCount, EventLoopGroup g) throws IllegalArgumentException {
        metaList = new ArrayList<ReplicaSession>();
        for (String addr: addrList) {
            rpc_address rpc_addr = new rpc_address();
            if (rpc_addr.fromString(addr)) {
                logger.info("add {} as meta server", addr);
                metaList.add(manager.getReplicaSession(rpc_addr));
            }
            else {
                logger.error("invalid address {}", addr);
            }
        }
        if (metaList.isEmpty()) {
            throw new IllegalArgumentException("can't find valid meta server address " + addrList.toString());
        }
        curLeader = 0;

        this.eachQueryTimeoutInMills = eachQueryTimeoutInMills;
        this.defaultMaxQueryCount = defaultMaxQueryCount;
        this.group = g;
    }

    static public final error_types getMetaServiceError(client_operator metaQueryOp) {
        if (metaQueryOp.rpc_error.errno != error_types.ERR_OK)
            return metaQueryOp.rpc_error.errno;
        query_cfg_operator op = (query_cfg_operator) metaQueryOp;
        return op.get_response().getErr().errno;
    }

    public final void asyncQuery(client_operator op, Runnable callbackFunc, int maxQueryCount) {
        if (maxQueryCount == 0) {
            maxQueryCount = defaultMaxQueryCount;
        }
        MetaRequestRound round;
        synchronized (this) {
            round = new MetaRequestRound(op, callbackFunc, maxQueryCount, metaList.get(curLeader));
        }
        asyncCall(round);
    }

    public final void query(client_operator op, int maxQueryCount) {
        FutureTask<Void> v = new FutureTask<Void>(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                return null;
            }
        });
        asyncQuery(op, v, maxQueryCount);
        while (true) {
            try {
                v.get();
                return;
            } catch (InterruptedException e) {
                logger.info("operation {} got interrupt exception: ", op.get_gpid().toString(), e);
            } catch (ExecutionException e) {
                logger.warn("operation {} got execution exception, just return: ", op.get_gpid().toString(), e);
                return;
            }
        }
    }

    public final void closeSession() {
        for (ReplicaSession rs: metaList) {
            rs.closeSession();
        }
    }

    private final void asyncCall(final MetaRequestRound round) {
        round.lastSession.asyncSend(round.op, new Runnable() {
            @Override
            public void run() {
                onFinishQueryMeta(round);
            }
        }, eachQueryTimeoutInMills);
    }

    private final void onFinishQueryMeta(final MetaRequestRound round) {
        client_operator op = round.op;

        boolean needDelay = false;
        boolean needSwitchLeader = false;

        --round.maxQueryCount;
        if (round.maxQueryCount == 0) {
            round.callbackFunc.run();
            return;
        }

        error_types metaError = error_types.ERR_UNKNOWN;
        if (op.rpc_error.errno == error_types.ERR_OK) {
            metaError = getMetaServiceError(op);
            if (metaError == error_types.ERR_SERVICE_NOT_ACTIVE) {
                needDelay = true;
                needSwitchLeader = false;
            }
            else if (metaError == error_types.ERR_FORWARD_TO_OTHERS) {
                needDelay = false;
                needSwitchLeader = true;
            }
            else {
                round.callbackFunc.run();
                return;
            }
        }
        else if (op.rpc_error.errno == error_types.ERR_SESSION_RESET || op.rpc_error.errno == error_types.ERR_TIMEOUT) {
            needDelay = true;
            needSwitchLeader = true;
        }
        else {
            logger.error("unknown error: {}", op.rpc_error.errno.toString());
            round.callbackFunc.run();
            return;
        }

        logger.info("query meta got error, rpc({}), meta({}), connected leader({}), remain retry count({}), " +
                        "need switch leader({}), need delay({})",
                op.rpc_error.errno.toString(),
                metaError.toString(),
                round.lastSession.name(),
                round.maxQueryCount,
                needSwitchLeader,
                needDelay
                );
        synchronized (this) {
            if (needSwitchLeader && metaList.get(curLeader) == round.lastSession) {
                curLeader = (curLeader + 1) % metaList.size();
            }
            round.lastSession = metaList.get(curLeader);
        }

        group.schedule(new Runnable() {
            @Override
            public void run() {
                asyncCall(round);
            }
        }, needDelay ? 1 : 0, TimeUnit.SECONDS);
    }

    private static final class MetaRequestRound {
        public client_operator op;
        public Runnable callbackFunc;
        public int maxQueryCount;
        public ReplicaSession lastSession;

        public MetaRequestRound(client_operator o, Runnable r, int q, ReplicaSession l) {
            op = o;
            callbackFunc = r;
            maxQueryCount = q;
            lastSession = l;
        }
    }

    private List<ReplicaSession> metaList;
    private int curLeader;
    private int eachQueryTimeoutInMills;
    private int defaultMaxQueryCount;
    private EventLoopGroup group;

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetaSession.class);
}
