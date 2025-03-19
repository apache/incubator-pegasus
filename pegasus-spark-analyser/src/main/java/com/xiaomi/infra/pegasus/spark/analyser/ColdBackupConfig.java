package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.CommonConfig;
import com.xiaomi.infra.pegasus.spark.FDSConfig;
import com.xiaomi.infra.pegasus.spark.HDFSConfig;
import com.xiaomi.infra.pegasus.spark.utils.FlowController.RateLimiterConfig;

/**
 * ColdBackupConfig is used when you manipulate the cold-backup data. <br>
 * <br>
 * A pegasus cold-backup is a couple of well-organized files dumped from a pegasus table.<br>
 * It's a complete snapshot of the moment.
 */
public class ColdBackupConfig extends CommonConfig implements Config {
  private static final DataType dataType = DataType.COLD_BACKUP;

  private static final long MB_UNIT = 1024 * 1024L;

  private static final int DEFAULT_FILE_OPEN_COUNT = 50;
  private static final long DEFAULT_READ_AHEAD_SIZE_MB = 1;

  private long readAheadSize;
  private int fileOpenCount;
  private String policyName;
  private String coldBackupTime;
  private DataVersion dataVersion;

  public ColdBackupConfig(HDFSConfig hdfsConfig, String clusterName, String tableName) {
    super(hdfsConfig, clusterName, tableName);
    setReadOptions(DEFAULT_FILE_OPEN_COUNT, DEFAULT_READ_AHEAD_SIZE_MB);
    setPolicyName(tableName);
    setDataVersion(new DataVersion1());
  }

  public ColdBackupConfig(FDSConfig fdsConfig, String clusterName, String tableName) {
    super(fdsConfig, clusterName, tableName);
    setReadOptions(DEFAULT_FILE_OPEN_COUNT, DEFAULT_READ_AHEAD_SIZE_MB);
    setPolicyName(tableName);
    setDataVersion(new DataVersion1());
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  /**
   * cold backup policy name
   *
   * @param policyName policyName is pegasus server cold backup concept which is set when creating
   *     cold backup, see https://pegasus.apache.org/administration/cold-backup, here default is
   *     "every_day", you may need change it base your pegasus server config
   * @return this
   */
  public ColdBackupConfig setPolicyName(String policyName) {
    this.policyName = policyName;
    return this;
  }

  /**
   * cold backup creating time.
   *
   * @param coldBackupTime creating time of cold backup data, accurate to day level. for example:
   *     2019-09-11, default is null, means choose the latest data
   * @return this
   */
  public ColdBackupConfig setColdBackupTime(String coldBackupTime) {
    this.coldBackupTime = coldBackupTime;
    return this;
  }

  /**
   * pegasus data version
   *
   * @param dataVersion pegasus data has different data versions, default is {@linkplain
   *     DataVersion1}
   * @return this
   */
  // TODO(wutao1): we can support auto detection of the data version.
  public ColdBackupConfig setDataVersion(DataVersion dataVersion) {
    this.dataVersion = dataVersion;
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
   * detail see {@link com.xiaomi.infra.pegasus.spark.utils.FlowController} and {@link
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
