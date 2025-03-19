package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.bulkloader.DataMetaInfo.FileInfo;
import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.common.RemoteFileSystem;
import com.xiaomi.infra.pegasus.spark.common.RocksDBOptions;
import com.xiaomi.infra.pegasus.spark.common.utils.AutoRetryer;
import com.xiaomi.infra.pegasus.spark.common.utils.FlowController;
import com.xiaomi.infra.pegasus.spark.common.utils.JsonParser;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import scala.Tuple2;

class BulkLoader {
  private static final Log LOG = LogFactory.getLog(BulkLoader.class);

  public static final int SINGLE_FILE_SIZE_THRESHOLD = 64 * 1024 * 1024;
  public static final String BULK_LOAD_INFO = "bulk_load_info";
  public static final String BULK_LOAD_METADATA = "bulk_load_metadata";
  public static final String BULK_DATA_FILE_SUFFIX = ".sst";

  private final AtomicLong totalSize = new AtomicLong();
  private final int partitionId;
  private int curFileIndex = 1;
  private Long curFileSize = 0L;

  private BulkLoaderConfig config;

  private BulkLoadInfo bulkLoadInfo;
  private DataMetaInfo dataMetaInfo;

  private String partitionPath;
  private String bulkLoadInfoPath;
  private String bulkLoadMetaDataPath;

  private RemoteFileSystem remoteFileSystem;
  private SstFileWriterWrapper sstFileWriterWrapper;
  private FlowController flowController;

  private Iterator<Tuple2<PegasusKey, PegasusValue>> dataResourceIterator;

  BulkLoader(
      BulkLoaderConfig config, Iterator<Tuple2<PegasusKey, PegasusValue>> iterator, int partitionId)
      throws PegasusSparkException {

    this.config = config;

    String dataPathPrefix =
        config.getRemoteFileSystemURL()
            + "/"
            + config.getRemoteFileSystemPath()
            + "/"
            + config.getClusterName()
            + "/"
            + config.getTableName()
            + "/";

    this.dataResourceIterator = iterator;
    this.partitionId = partitionId;

    this.partitionPath = dataPathPrefix + "/" + partitionId + "/";
    this.bulkLoadInfoPath = dataPathPrefix + "/" + BULK_LOAD_INFO;
    this.bulkLoadMetaDataPath = partitionPath + "/" + BULK_LOAD_METADATA;

    this.bulkLoadInfo =
        new BulkLoadInfo(
            config.getClusterName(),
            config.getTableName(),
            config.getTableId(),
            config.getTablePartitionCount());
    this.dataMetaInfo = new DataMetaInfo();

    this.remoteFileSystem = config.getRemoteFileSystem();

    RocksDBOptions rocksDBOptions =
        new RocksDBOptions(config.getRemoteFileSystemURL(), config.getRemoteFileSystemPort());
    rocksDBOptions.options.setCompressionType(config.getCompressionType());
    this.sstFileWriterWrapper = new SstFileWriterWrapper(rocksDBOptions);
    this.flowController =
        new FlowController(config.getTablePartitionCount(), config.getRateLimiterConfig());
  }

  void start() throws PegasusSparkException {
    try {
      createBulkLoadInfoFile();
      createSstFile();
      AutoRetryer.getDefaultRetryer().call(this::createBulkLoadMetaDataFile);
    } catch (Exception e) {
      throw new PegasusSparkException(
          "generated bulkloader data failed, please check and retry!", e);
    }
  }

  public void validateGenerateFiles() throws PegasusSparkException {
    if (!remoteFileSystem.exist(bulkLoadMetaDataPath)) {
      throw new PegasusSparkException(
          "validate generated files failed: can't find " + bulkLoadMetaDataPath);
    }

    try (BufferedReader bufferedReader = remoteFileSystem.getReader(bulkLoadMetaDataPath)) {
      DataMetaInfo dataMetaInfo = JsonParser.getGson().fromJson(bufferedReader, DataMetaInfo.class);
      for (FileInfo fileInfo : dataMetaInfo.files) {
        String filePath = partitionPath + "/" + fileInfo.name;
        FileStatus[] filesStatus = remoteFileSystem.getFileStatus(filePath);
        if (filesStatus.length != 1) {
          throw new PegasusSparkException(
              String.format(
                  "get file(%s) status size should be one but is %d",
                  filePath, filesStatus.length));
        }

        String fileMD5 = remoteFileSystem.getFileMD5(filePath);
        if (filesStatus[0].getLen() != fileInfo.size && !fileMD5.equals(fileInfo.md5)) {
          throw new PegasusSparkException(
              String.format(
                  "validate file(%s) failed(actual vs expect): size(%d vs %d); md5(%s vs %s)",
                  filePath, filesStatus[0].getLen(), fileInfo.size, fileMD5, fileInfo.md5));
        }
      }
    } catch (IOException | PegasusSparkException e) {
      throw new PegasusSparkException("validate generated files failed!");
    }

    LOG.info(String.format("completed to validate partition %d data!", partitionId));
  }

  public void createBulkLoadInfoFile() throws PegasusSparkException {
    // all partitions share one bulkLoadInfo file, so just one partition create it, otherwise the
    // filesystem may throw exception
    if (partitionId == 0) {
      try (BufferedWriter bulkLoadInfoWriter = remoteFileSystem.getWriter(bulkLoadInfoPath)) {
        bulkLoadInfoWriter.write(bulkLoadInfo.toJsonString());
        LOG.info("The bulkLoadInfo file is created successful by partition 0.");
      } catch (IOException e) {
        throw new PegasusSparkException("create bulkLoadInfo failed!", e);
      }
    } else {
      LOG.info("The bulkLoadInfo file is created only by partition 0.");
    }
  }

  // TODO(jiashuo): write data may should keep trying if failed, but not only finite times
  public void createSstFile() throws PegasusSparkException {
    if (!dataResourceIterator.hasNext()) {
      throw new PegasusSparkException(
          "can't find any data in current partition! partition = " + this.partitionId);
    }

    if (remoteFileSystem.exist(partitionPath)) {
      LOG.warn(String.format("%s has existed, it will be deleted", partitionPath));
      remoteFileSystem.delete(partitionPath, true);
    }

    long start = System.currentTimeMillis();
    long count = 0;

    String curSSTFileName = curFileIndex + BULK_DATA_FILE_SUFFIX;
    sstFileWriterWrapper.openWithRetry(partitionPath + curSSTFileName);
    while (dataResourceIterator.hasNext()) {
      count++;
      Tuple2<PegasusKey, PegasusValue> record = dataResourceIterator.next();
      if (curFileSize > SINGLE_FILE_SIZE_THRESHOLD) {
        sstFileWriterWrapper.closeWithRetry();
        LOG.debug(curFileIndex + BULK_DATA_FILE_SUFFIX + " writes complete!");

        curFileIndex++;
        curFileSize = 0L;
        curSSTFileName = curFileIndex + BULK_DATA_FILE_SUFFIX;

        sstFileWriterWrapper.openWithRetry(partitionPath + curSSTFileName);
      }

      // `flowControl` initialized by `RateLimiterConfig` whose `qps` and `bytes` are both 0
      // default, which means if you don't set custom config value > 0 , it will not limit and
      // return immediately
      flowController.acquireQPS();
      flowController.acquireBytes(record._1.data().length + record._2.data().length);

      curFileSize += sstFileWriterWrapper.writeWithRetry(record._1.data(), record._2.data());
    }
    sstFileWriterWrapper.closeWithRetry();
    LOG.info(
        "create partition("
            + partitionId
            + ") sst file complete, time used is "
            + (System.currentTimeMillis() - start)
            + "ms, record counts = "
            + count
            + ", file counts = "
            + curFileIndex);
  }

  public boolean createBulkLoadMetaDataFile() throws PegasusSparkException {
    long start = System.currentTimeMillis();
    try {
      FileStatus[] fileStatuses = remoteFileSystem.getFileStatus(partitionPath);
      for (FileStatus fileStatus : fileStatuses) {
        generateFileMetaInfo(fileStatus);
      }

      dataMetaInfo.file_total_size = totalSize.get();
      BufferedWriter bulkLoadMetaDataWriter = remoteFileSystem.getWriter(bulkLoadMetaDataPath);
      bulkLoadMetaDataWriter.write(dataMetaInfo.toJsonString());
      bulkLoadMetaDataWriter.close();
    } catch (IOException e) {
      throw new PegasusSparkException(
          "create bulkload_meta_dada file failed, error=" + e.getMessage());
    }
    LOG.info("create meta info successfully, time used is " + (System.currentTimeMillis() - start));
    return true;
  }

  public void generateFileMetaInfo(FileStatus fileStatus) throws PegasusSparkException {
    String filePath = fileStatus.getPath().toString();
    if (!filePath.contains(".sst")) {
      LOG.warn(String.format("the file `%s` is not sst file, ignore it!", filePath));
      return;
    }

    String fileName = fileStatus.getPath().getName();
    long fileSize = fileStatus.getLen();
    String fileMD5 = remoteFileSystem.getFileMD5(filePath);

    if (fileSize <= 0) {
      dataMetaInfo.files.clear();
      totalSize.set(0);
      throw new PegasusSparkException(fileName + " get size failed, size=" + fileSize);
    }

    FileInfo fileInfo = dataMetaInfo.new FileInfo(fileName, fileSize, fileMD5);
    dataMetaInfo.files.add(fileInfo);

    totalSize.addAndGet(fileSize);
  }
}
