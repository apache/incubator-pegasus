package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.common.utils.gateway.Compaction;
import com.xiaomi.infra.pegasus.spark.common.utils.gateway.TableDupInfo;

public class StartBulkloadInfo {
    public String cluster;
    public String table;
    public String remoteFileSystem;
    public String remotePath;
    public Compaction compaction;

    public StartBulkloadInfo(String cluster,
                             String table,
                             String remoteFileSystem,
                             String remotePath,
                             Compaction compaction) throws PegasusSparkException {
        this.cluster = cluster;
        this.table = table;
        this.remoteFileSystem = remoteFileSystem;
        this.remotePath = remotePath;
        this.compaction = compaction;
    }

    public StartBulkloadInfo(StartBulkloadInfo bulkloadInfo) throws PegasusSparkException {
        this.cluster = bulkloadInfo.cluster;
        this.table = bulkloadInfo.table;
        this.remoteFileSystem = bulkloadInfo.remoteFileSystem;
        this.remotePath = bulkloadInfo.remotePath;
        this.compaction = new Compaction(bulkloadInfo.compaction);
    }

    @Override
    public String toString() {
        return "StartBulkloadInfo{" +
                "cluster='" + cluster + '\'' +
                ", table='" + table + '\'' +
                ", remoteFileSystem='" + remoteFileSystem + '\'' +
                ", remotePath='" + remotePath + '\'' +
                ", compaction=" + compaction +
                '}';
    }
}