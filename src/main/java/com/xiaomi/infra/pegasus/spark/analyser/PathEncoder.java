package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import java.io.BufferedReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

public abstract class PathEncoder {
  private static final Log LOG = LogFactory.getLog(PathEncoder.class);

  private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

  /** V1: /clusterName/policyName/backupID/nameAndID/partition/chkpt_XXX/sst */
  public static class EncoderV1 extends PathEncoder {
    public EncoderV1(ColdBackupConfig config) {
      super(config);
    }

    @Override
    public String searchLatestBackupID(List<String> idPathList) throws PegasusSparkException {
      if (idPathList.size() != 0) {
        return idPathList.get(idPathList.size() - 1);
      }
      throw new PegasusSparkException(
          "get latest policy id from " + rootPath + " failed, no backup id existed!");
    }

    @Override
    public String searchBackupIDByTime(List<String> idPathList) throws PegasusSparkException {
      for (String path : idPathList) {
        long timestamp = Long.parseLong(path.substring(path.length() - 13));
        String date = simpleDateFormat.format(new Date(timestamp));
        if (date.equals(coldBackupConfig.getColdBackupTime())) {
          return path;
        }
      }
      throw new PegasusSparkException(
          "can't match the date time:+" + coldBackupConfig.getColdBackupTime());
    }
  }

  /** V2: /clusterName/backupID/nameAndID/partition/chkpt_XXX/sst */
  public static class EncoderV2 extends PathEncoder {
    public EncoderV2(ColdBackupConfig config) {
      super(config);
    }

    @Override
    public String searchLatestBackupID(List<String> idPathList) throws PegasusSparkException {
      for (int i = idPathList.size() - 1; i >= 0; i--) {
        String nameWithId;
        try {
          nameWithId = encodeTableNameAndId(idPathList.get(i), coldBackupConfig.getTableName());
        } catch (Exception e) {
          LOG.warn(
              "try next timestamp folder for table "
                  + coldBackupConfig.getTableName()
                  + ", because "
                  + e.getMessage());
          continue;
        }
        if (nameWithId.equals("")) {
          throw new PegasusSparkException(
              "get latest policy id from "
                  + rootPath
                  + " failed, no backup id existed for table:"
                  + coldBackupConfig.getTableName());
        }
        List<String> tablePathList =
            coldBackupConfig.getRemoteFileSystem().listSubPath(idPathList.get(i));
        for (String tablePath : tablePathList) {
          if (tablePath.contains(nameWithId)) {
            return idPathList.get(i);
          }
        }
      }
      throw new PegasusSparkException(
          "get latest policy id from "
              + rootPath
              + " failed, no backup id existed for table:"
              + coldBackupConfig.getTableName());
    }

    @Override
    public String searchBackupIDByTime(List<String> idPathList) throws PegasusSparkException {
      List<String> matchIDPathListByTime = new ArrayList<>();
      for (String path : idPathList) {
        long timestamp = Long.parseLong(path.substring(path.length() - 13));
        String date = simpleDateFormat.format(new Date(timestamp));
        if (date.equals(coldBackupConfig.getColdBackupTime())) {
          matchIDPathListByTime.add(path);
        }
      }
      return searchLatestBackupID(matchIDPathListByTime);
    }
  }

  public ColdBackupConfig coldBackupConfig;
  public String rootPath;

  private PathEncoder(ColdBackupConfig config) {
    this.coldBackupConfig = config;
    this.rootPath =
        String.format(
            "%s/%s/%s/%s/",
            coldBackupConfig.getRemoteFileSystemURL(),
            coldBackupConfig.getRemoteFileSystemPath(),
            coldBackupConfig.getClusterName(),
            coldBackupConfig.getPolicyName() == null ? "" : coldBackupConfig.getPolicyName());
  }

  public static PathEncoder create(ColdBackupConfig config) {
    if (config.getPolicyName() != null) {
      return new EncoderV1(config);
    } else {
      return new EncoderV2(config);
    }
  }

  public Map<Integer, String> getCheckPointUrls() throws PegasusSparkException {
    String backupIDPath =
        searchMatchBackupIDPath(
            filterValidIDList(coldBackupConfig.getRemoteFileSystem().listSubPath(rootPath)));
    String tableFolderPath =
        backupIDPath + "/" + encodeTableNameAndId(backupIDPath, coldBackupConfig.getTableName());
    return getCheckpointUrls(tableFolderPath, getPartitionCount(tableFolderPath));
  }

  private List<String> filterValidIDList(List<String> ids) {
    List<String> validIDList = new ArrayList<>();
    for (String idPath : ids) {
      try {
        Long.parseLong(idPath.substring(idPath.length() - 13));
        validIDList.add(idPath);
      } catch (NumberFormatException e) {
        LOG.warn("Ignore the invalid path: " + idPath);
      }
    }
    return validIDList;
  }

  private Map<Integer, String> getCheckpointUrls(String tableFolderPath, int partitionCount)
      throws PegasusSparkException {
    Map<Integer, String> checkpointUrls = new HashMap<>();

    String chkpt;
    partitionCount--;
    while (partitionCount >= 0) {
      String currentCheckpointUrl =
          tableFolderPath + "/" + partitionCount + "/" + "current_checkpoint";
      try (BufferedReader bufferedReader =
          coldBackupConfig.getRemoteFileSystem().getReader(currentCheckpointUrl)) {
        while ((chkpt = bufferedReader.readLine()) != null) {
          String url =
              tableFolderPath.split(coldBackupConfig.getRemoteFileSystemURL())[1]
                  + "/"
                  + partitionCount
                  + "/"
                  + chkpt;
          checkpointUrls.put(partitionCount, url);
        }
        partitionCount--;
      } catch (IOException e) {
        throw new PegasusSparkException(
            "init checkPoint urls failed, [checkpointUrl:" + currentCheckpointUrl + "]", e);
      }
    }
    return checkpointUrls;
  }

  private int getPartitionCount(String tableFolderPath) throws PegasusSparkException {
    String appMetaData;
    String appMetaDataUrl = tableFolderPath + "/" + "meta" + "/" + "app_metadata";
    try (BufferedReader bufferedReader =
        coldBackupConfig.getRemoteFileSystem().getReader(appMetaDataUrl)) {
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

  public String searchMatchBackupIDPath(List<String> idPathList) throws PegasusSparkException {
    if (coldBackupConfig.getColdBackupTime() == null) {
      return searchLatestBackupID(idPathList);
    } else {
      return searchBackupIDByTime(idPathList);
    }
  }

  public String encodeTableNameAndId(String backupIDPath, String tableName)
      throws PegasusSparkException {
    String infoMessage = null;
    String backupInfo;
    String backupInfoUrl = backupIDPath + "/" + "backup_info";
    try (BufferedReader bufferedReader =
        coldBackupConfig.getRemoteFileSystem().getReader(backupInfoUrl)) {
      while ((backupInfo = bufferedReader.readLine()) != null) {
        JSONObject jsonObject = new JSONObject(backupInfo);
        if (backupInfo.contains("app_names")) {
          infoMessage = "[app_names]";
          JSONObject tables = jsonObject.getJSONObject("app_names");
          Iterator<String> iterator = tables.keys();
          while (iterator.hasNext()) {
            String tableId = iterator.next();
            if (tables.get(tableId).equals(tableName)) {
              return tableName + "_" + tableId;
            }
          }
        } else {
          infoMessage = "[app_name]";
          String name = jsonObject.getString("app_name");
          int id = jsonObject.getInt("app_id");
          if (name.equals(tableName)) {
            return name + "_" + id;
          }
        }
      }
    } catch (IOException | JSONException e) {
      throw new PegasusSparkException(
          "get table[" + tableName + "] id failed, [url:" + backupIDPath + "]" + infoMessage, e);
    }
    throw new PegasusSparkException(
        "can't find the table[" + tableName + "] id, [url:" + backupIDPath + "]" + infoMessage);
  }

  protected abstract String searchLatestBackupID(List<String> idPathList)
      throws PegasusSparkException;

  protected abstract String searchBackupIDByTime(List<String> idPathList)
      throws PegasusSparkException;
}
