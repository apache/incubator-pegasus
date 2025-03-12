package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.common.CommonConfig;
import com.xiaomi.infra.pegasus.spark.common.FDSConfig;
import com.xiaomi.infra.pegasus.spark.common.HDFSConfig;
import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.common.utils.FlowController.RateLimiterConfig;
import com.xiaomi.infra.pegasus.spark.common.utils.gateway.Cluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ColdBackupConfig is used when you manipulate the cold-backup data. <br>
 * <br>
 * A pegasus cold-backup is a couple of well-organized files dumped from a pegasus table.<br>
 * It's a complete snapshot of the moment.
 */
public class ColdBackupConfig extends CommonConfig implements Config {
  private static final Log LOG = LogFactory.getLog(ColdBackupConfig.class);
  private static final DataType dataType = DataType.COLD_BACKUP;

  private static final long MB_UNIT = 1024 * 1024L;

  private static final int DEFAULT_FILE_OPEN_COUNT = 50;
  private static final long DEFAULT_READ_AHEAD_SIZE_MB = 1;

  private String policyName;
  private long readAheadSize;
  private int fileOpenCount;
  private String coldBackupTime;
  private DataVersion dataVersion;
  private boolean filterExpired;

  public ColdBackupConfig(HDFSConfig hdfsConfig, String clusterName, String tableName) {
    super(hdfsConfig, clusterName, tableName);
    initConfig();
  }

  public ColdBackupConfig(FDSConfig fdsConfig, String clusterName, String tableName) {
    super(fdsConfig, clusterName, tableName);
    initConfig();
  }

  private void initConfig() {
    filterExpired = true;
    setReadOptions(DEFAULT_FILE_OPEN_COUNT, DEFAULT_READ_AHEAD_SIZE_MB);
  }

  /**
   * auto set data version from gateway{@link Cluster}
   *
   * @throws PegasusSparkException
   */
  public ColdBackupConfig initDataVersion() throws PegasusSparkException {
    int version = Cluster.getTableVersion(getClusterName(), getTableName());
    setDataVersion(version);
    LOG.info(
        "Init table version success:"
            + String.format(
                "cluster = %s, table = %s, version = %s",
                getClusterName(), getTableName(), getDataVersion().toString()));
    return this;
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  /**
   * cold backup policy name is pegasus server(version < 2.2.0) cold backup concept which is set
   * when creating cold backup, see https://pegasus.apache.org/administration/cold-backup. if
   * pegasus server >=2.2.0, this param is set to "" and can be ignored
   *
   * @param policyName
   * @return this
   */
  public ColdBackupConfig setPolicyName(String policyName) {
    this.policyName = policyName;
    return this;
  }

  /**
   * cold backup creating time.
   *
   * @param coldBackupTime please </>creating time of cold backup data, accurate to day level. for
   *     example: 2019-09-11, default is null, means choose the latest data
   * @return this
   */
  public ColdBackupConfig setColdBackupTime(String coldBackupTime) {
    this.coldBackupTime = coldBackupTime;
    return this;
  }

  /**
   * pegasus table data version, 0 or 1, if Pegasus Server >= 2.2.0, you can auto-init use
   * {@linkplain ColdBackupConfig#initDataVersion()}
   *
   * @param dataVersion
   * @return this
   */
  public ColdBackupConfig setDataVersion(int dataVersion) throws PegasusSparkException {
    switch (dataVersion) {
      case 0:
        this.dataVersion = new DataV0();
        break;
      case 1:
        this.dataVersion = new DataV1();
        break;
      default:
        throw new PegasusSparkException(
            String.format("Not support read data version: %d", dataVersion));
    }
    return this;
  }

  /**
   * @param maxFileOpenCount maxFileOpenCount is rocksdb concept which can control the max file open
   *     count, default is 50. detail see
   *     https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#general-options
   * @param readAheadSize readAheadSize is rocksdb concept which can control the readAheadSize,
   *     default is 1MB, detail see https://github.com/facebook/rocksdb/wiki/Iterator#read-ahead
   */
  public ColdBackupConfig setReadOptions(int maxFileOpenCount, long readAheadSize) {
    this.readAheadSize = readAheadSize * MB_UNIT;
    this.fileOpenCount = maxFileOpenCount;
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
  public ColdBackupConfig setRateLimiterConfig(RateLimiterConfig rateLimiterConfig) {
    super.setRateLimiterConfig(rateLimiterConfig);
    return this;
  }

  public boolean isFilterExpired() {
    return filterExpired;
  }

  public ColdBackupConfig enableFilterExpired(boolean filterExpired) {
    this.filterExpired = filterExpired;
    return this;
  }

  public long getReadAheadSize() {
    return readAheadSize;
  }

  public int getFileOpenCount() {
    return fileOpenCount;
  }

  public String getPolicyName() {
    return policyName;
  }

  public String getColdBackupTime() {
    return coldBackupTime;
  }

  public DataVersion getDataVersion() {
    return dataVersion;
  }
}
