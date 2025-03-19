package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.common.CommonConfig;
import com.xiaomi.infra.pegasus.spark.common.FDSConfig;
import com.xiaomi.infra.pegasus.spark.common.HDFSConfig;
import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.common.utils.FlowController.RateLimiterConfig;
import com.xiaomi.infra.pegasus.spark.common.utils.gateway.Cluster;
import com.xiaomi.infra.pegasus.spark.common.utils.gateway.TableInfo;
import java.io.Serializable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.CompressionType;

/**
 * The config used for generating the pegasus data which will be placed as follow":
 *
 * <p><DataPathRoot>/<ClusterName>/<TableName>
 * <DataPathRoot>/<ClusterName>/<TableName>/bulk_load_info => {JSON}
 * <DataPathRoot>/<ClusterName>/<TableName>/<PartitionIndex>/bulk_load_metadata => {JSON}
 * <DataPathRoot>/<ClusterName>/<TableName>/<PartitionIndex>/<FileIndex>.sst => RocksDB SST File
 */
public class BulkLoaderConfig extends CommonConfig {
  private static final Log LOG = LogFactory.getLog(BulkLoaderConfig.class);

  private boolean enableValidateAfterGenerate = true;
  private AdvancedConfig advancedConfig = new AdvancedConfig();
  private CompressionType compressionType = CompressionType.ZSTD_COMPRESSION;

  private DataVersion tableDataVersion;
  private int tableId;
  private int tablePartitionCount;

  public BulkLoaderConfig(HDFSConfig hdfsConfig, String clusterName, String tableName)
      throws PegasusSparkException {
    super(hdfsConfig, clusterName, tableName);
    initTableInfo(); // table id, partitionCount, version are fetched via gateway by default.
    // Pegasus Server Version 2.2.0 required
  }

  public BulkLoaderConfig(FDSConfig fdsConfig, String clusterName, String tableName)
      throws PegasusSparkException {
    super(fdsConfig, clusterName, tableName);
    initTableInfo(); // table id, partitionCount, version are fetched via gateway by default.
    // Pegasus Server Version  2.2.0 required
  }

  private void initTableInfo() throws PegasusSparkException {
    TableInfo tableInfo = Cluster.getTableInfo(getClusterName(), getTableName());
    setTableInfo(
        Integer.parseInt(tableInfo.general.app_id),
        Integer.parseInt(tableInfo.general.partition_count),
        Cluster.getTableVersion(tableInfo));
    LOG.info(
        "Init table info success:"
            + String.format(
                "cluster = %s, table = %s[%d(%d)], version = %s",
                getClusterName(),
                getTableName(),
                getTableId(),
                getTablePartitionCount(),
                getDataVersion().toString()));
  }

  private void setTableInfo(int tableId, int tablePartitionCount, int dataVersion)
      throws PegasusSparkException {
    this.tableId = tableId;
    this.tablePartitionCount = tablePartitionCount;
    switch (dataVersion) {
      case 0:
        this.tableDataVersion = new DataV0();
        break;
      case 1:
        this.tableDataVersion = new DataV1();
        break;
      default:
        throw new PegasusSparkException(
            String.format("Not support write data version: %d", dataVersion));
    }
  }

  /**
   * Set which compression method is used to compress data, support list: NO_COMPRESSION,
   * SNAPPY_COMPRESSION, ZLIB_COMPRESSION, BZLIB2_COMPRESSION, LZ4_COMPRESSION, LZ4HC_COMPRESSION,
   * XPRESS_COMPRESSION, ZSTD_COMPRESSION, DISABLE_COMPRESSION_OPTION;
   *
   * @param compressType default is ZSTD_COMPRESSION
   * @return this
   */
  public BulkLoaderConfig setDataCompressType(CompressionType compressType) {
    this.compressionType = compressType;
    return this;
  }

  /**
   * Set advanced configuration for bulk load job. See {@link AdvancedConfig} for more details.
   * (Optional)
   *
   * @param advancedConfig
   * @return this
   */
  public BulkLoaderConfig setAdvancedConfig(AdvancedConfig advancedConfig) {
    this.advancedConfig = advancedConfig;
    return this;
  }

  /**
   * set RateLimiter config to control request flow that include `qpsLimiter` and `bytesLimiter`,
   * detail see {@link com.xiaomi.infra.pegasus.spark.common.utils.FlowController} and {@link
   * RateLimiterConfig}
   *
   * @param rateLimiterConfig see {@link RateLimiterConfig}
   * @return this
   */
  @Override
  public BulkLoaderConfig setRateLimiterConfig(RateLimiterConfig rateLimiterConfig) {
    super.setRateLimiterConfig(rateLimiterConfig);
    return this;
  }

  public BulkLoaderConfig setEnableValidateAfterGenerate(boolean enableValidateAfterGenerate) {
    this.enableValidateAfterGenerate = enableValidateAfterGenerate;
    return this;
  }

  public DataVersion getDataVersion() {
    return tableDataVersion;
  }

  public int getTableId() {
    return tableId;
  }

  public int getTablePartitionCount() {
    return tablePartitionCount;
  }

  public AdvancedConfig getAdvancedConfig() {
    return advancedConfig;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  public boolean isEnableValidateAfterGenerate() {
    return enableValidateAfterGenerate;
  }

  /**
   * Advanced configuration for Pegasus BulkLoad. Leave it to default if you are uncertain for the
   * meaning of each item.
   */
  public static class AdvancedConfig implements Serializable {

    private boolean isDistinct = true;
    private boolean isSort = true;

    /**
     * Whether to remove duplicate pegasus records. Pegasus BulkLoad requires the data set contains
     * only distinct (different) records.
     *
     * <p>If the RDD given is already distinct (for example, when the RDD is loaded from a Pegasus
     * backup), you can set this option to false to complete the job faster.
     *
     * @param distinct default is true
     * @return this
     */
    public AdvancedConfig enableDistinct(boolean distinct) {
      isDistinct = distinct;
      return this;
    }

    /**
     * Whether to sort the pegasus records. Pegasus BulkLoad requires the data set to be fully
     * sorted.
     *
     * <p>If the RDD given is already sorted (for example, when the RDD is loaded from a Pegasus
     * backup), you can set this option to false to complete the job faster.
     *
     * @param sort default is true
     * @return this
     */
    public AdvancedConfig enableSort(boolean sort) {
      isSort = sort;
      return this;
    }

    public boolean enableDistinct() {
      return isDistinct;
    }

    public boolean enableSort() {
      return isSort;
    }
  }
}
