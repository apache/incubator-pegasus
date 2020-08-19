package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.RemoteFileSystem;
import com.xiaomi.infra.pegasus.spark.RocksDBOptions;
import java.io.BufferedReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.json.JSONException;
import org.json.JSONObject;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

class ColdBackupLoader implements PegasusLoader {

  private static final Log LOG = LogFactory.getLog(ColdBackupLoader.class);

  private ColdBackupConfig coldBackupConfig;
  private RemoteFileSystem remoteFileSystem;
  private Map<Integer, String> checkpointUrls = new HashMap<>();
  private int partitionCount;

  private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

  ColdBackupLoader(ColdBackupConfig config) throws PegasusSparkException {
    coldBackupConfig = config;
    remoteFileSystem = config.getRemoteFileSystem();

    String idPrefix =
        coldBackupConfig.getRemoteFileSystemURL()
            + "/"
            + coldBackupConfig.getClusterName()
            + "/"
            + coldBackupConfig.getPolicyName()
            + "/";

    String idPath =
        config.getColdBackupTime() == null
            ? getLatestPolicyId(idPrefix)
            : getPolicyId(idPrefix, config.getColdBackupTime());
    String tableNameAndId = getTableNameAndId(idPath, coldBackupConfig.getTableName());
    String metaPrefix = idPath + "/" + tableNameAndId;

    partitionCount = getCount(metaPrefix);
    initCheckpointUrls(metaPrefix, partitionCount);

    LOG.info("init fds default config and get the data urls");
  }

  @Override
  public int getPartitionCount() {
    return partitionCount;
  }

  @Override
  public ColdBackupConfig getConfig() {
    return coldBackupConfig;
  }

  @Override
  public PegasusScanner getScanner(int pid) throws PegasusSparkException, RocksDBException {
    RocksDBOptions rocksDBOptions =
        new RocksDBOptions(
            coldBackupConfig.getRemoteFileSystemURL(), coldBackupConfig.getRemoteFileSystemPort());
    rocksDBOptions.options.setMaxOpenFiles(coldBackupConfig.getFileOpenCount());
    rocksDBOptions.readOptions.setReadaheadSize(coldBackupConfig.getReadAheadSize());

    RocksDB rocksDB = RocksDB.openReadOnly(rocksDBOptions.options, checkpointUrls.get(pid));
    RocksIterator rocksIterator = rocksDB.newIterator(rocksDBOptions.readOptions);
    return new ColdBackupScanner(
        coldBackupConfig.getDataVersion(), rocksDBOptions, rocksDB, rocksIterator);
  }

  private void initCheckpointUrls(String prefix, int count) throws PegasusSparkException {
    String chkpt;
    count--;
    while (count >= 0) {
      String currentCheckpointUrl = prefix + "/" + count + "/" + "current_checkpoint";
      try (BufferedReader bufferedReader = remoteFileSystem.getReader(currentCheckpointUrl)) {
        while ((chkpt = bufferedReader.readLine()) != null) {
          String url =
              prefix.split(coldBackupConfig.getRemoteFileSystemURL())[1]
                  + "/"
                  + count
                  + "/"
                  + chkpt;
          checkpointUrls.put(count, url);
        }
        count--;
      } catch (IOException e) {
        throw new PegasusSparkException(
            "init checkPoint urls failed, [checkpointUrl:" + currentCheckpointUrl + "]", e);
      }
    }
  }

  private String getLatestPolicyId(String prefix) throws PegasusSparkException {
    LOG.info("get the " + prefix + " latest id");
    ArrayList<String> idList = getPolicyIdList(remoteFileSystem.getFileStatus(prefix));
    LOG.info("the policy list:" + idList);
    if (idList.size() != 0) {
      return idList.get(idList.size() - 1);
    }
    throw new PegasusSparkException(
        "get latest policy id from " + prefix + " failed, no policy id existed!");
  }

  private ArrayList<String> getPolicyIdList(FileStatus[] status) {
    ArrayList<String> idList = new ArrayList<>();
    for (FileStatus fileStatus : status) {
      idList.add(fileStatus.getPath().toString());
    }
    return idList;
  }

  private String getPolicyId(String prefix, String dateTime) throws PegasusSparkException {
    FileStatus[] fileStatuses = remoteFileSystem.getFileStatus(prefix);
    for (FileStatus s : fileStatuses) {
      String idPath = s.getPath().toString();
      long timestamp = Long.parseLong(idPath.substring(idPath.length() - 13));
      String date = simpleDateFormat.format(new Date(timestamp));
      if (date.equals(dateTime)) {
        return idPath;
      }
    }
    throw new PegasusSparkException("can't match the date time:+" + dateTime);
  }

  private String getTableNameAndId(String prefix, String tableName) throws PegasusSparkException {
    String backupInfo;
    String backupInfoUrl = prefix + "/" + "backup_info";
    try (BufferedReader bufferedReader = remoteFileSystem.getReader(backupInfoUrl)) {
      while ((backupInfo = bufferedReader.readLine()) != null) {
        JSONObject jsonObject = new JSONObject(backupInfo);
        JSONObject tables = jsonObject.getJSONObject("app_names");
        Iterator<String> iterator = tables.keys();
        while (iterator.hasNext()) {
          String tableId = iterator.next();
          if (tables.get(tableId).equals(tableName)) {
            return tableName + "_" + tableId;
          }
        }
      }
    } catch (IOException | JSONException e) {
      throw new PegasusSparkException("get table id failed, [url:" + prefix + "]", e);
    }
    throw new PegasusSparkException("can't find the table id, [url:" + prefix + "]");
  }

  private int getCount(String prefix) throws PegasusSparkException {
    String appMetaData;
    String appMetaDataUrl = prefix + "/" + "meta" + "/" + "app_metadata";
    try (BufferedReader bufferedReader = remoteFileSystem.getReader(appMetaDataUrl)) {
      if ((appMetaData = bufferedReader.readLine()) != null) {
        JSONObject jsonObject = new JSONObject(appMetaData);
        return jsonObject.getInt("partition_count");
      }
    } catch (IOException | JSONException e) {
      throw new PegasusSparkException(
          "get the partition count failed, [url: " + appMetaDataUrl + "]", e);
    }
    throw new PegasusSparkException(
        "can't find the partition count failed, [url: " + appMetaDataUrl + "]");
  }

  static class ColdBackupScanner implements PegasusScanner {
    DataVersion scannerVersion;
    RocksDBOptions rocksDBOptions;
    RocksDB rocksDB;
    RocksIterator rocksIterator;

    ColdBackupScanner(
        DataVersion scannerVersion,
        RocksDBOptions rocksDBOptions,
        RocksDB rocksDB,
        RocksIterator rocksIterator) {
      this.scannerVersion = scannerVersion;
      this.rocksDBOptions = rocksDBOptions;
      this.rocksDB = rocksDB;
      this.rocksIterator = rocksIterator;
    }

    @Override
    public boolean isValid() {
      return rocksIterator.isValid();
    }

    @Override
    public void seekToFirst() {
      rocksIterator.seekToFirst();
    }

    @Override
    public void next() {
      rocksIterator.next();
    }

    @Override
    public PegasusRecord restore() {
      return scannerVersion.getPegasusRecord(rocksIterator);
    }

    @Override
    public void close() {
      rocksIterator.close();
      rocksDB.close();
      rocksDBOptions.close();
    }
  }
}
