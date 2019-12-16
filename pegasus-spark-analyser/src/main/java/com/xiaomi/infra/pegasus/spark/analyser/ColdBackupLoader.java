package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.FDSException;
import com.xiaomi.infra.pegasus.spark.FDSService;
import java.io.BufferedReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.json.JSONException;
import org.json.JSONObject;

public class ColdBackupLoader implements PegasusLoader {

  private static final Log LOG = LogFactory.getLog(ColdBackupLoader.class);

  private ColdBackupConfig globalConfig;
  private FDSService fdsService = new FDSService();
  private Map<Integer, String> checkpointUrls = new HashMap<>();
  private int partitionCount;

  private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

  public ColdBackupLoader(ColdBackupConfig config) throws FDSException {
    globalConfig = config;
    String idPrefix =
        globalConfig.remoteFsUrl
            + "/"
            + globalConfig.clusterName
            + "/"
            + globalConfig.policyName
            + "/";

    String idPath;
    if (!config.coldBackupTime.equals("")) {
      idPath = getPolicyId(idPrefix, config.coldBackupTime);
    } else {
      idPath = getLatestPolicyId(idPrefix);
    }

    String tableNameAndId = getTableNameAndId(idPath, globalConfig.tableName);
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
  public Map<Integer, String> getCheckpointUrls() {
    return checkpointUrls;
  }

  private void initCheckpointUrls(String prefix, int counter) throws FDSException {
    String chkpt;
    counter--;
    while (counter >= 0) {
      String currentCheckpointUrl = prefix + "/" + counter + "/" + "current_checkpoint";
      try (BufferedReader bufferedReader = fdsService.getReader(currentCheckpointUrl)) {
        while ((chkpt = bufferedReader.readLine()) != null) {
          String url = prefix.split(globalConfig.remoteFsUrl)[1] + "/" + counter + "/" + chkpt;
          checkpointUrls.put(counter, url);
        }
        counter--;
      } catch (IOException e) {
        LOG.error("init checkPoint urls failed from " + currentCheckpointUrl);
        throw new FDSException(
            "init checkPoint urls failed, [checkpointUrl:" + currentCheckpointUrl + "]" + e);
      }
    }
  }

  private String getLatestPolicyId(String prefix) throws FDSException {
    try {
      LOG.info("get the " + prefix + " latest id");
      ArrayList<String> idList = getPolicyIdList(fdsService.getFileStatus(prefix));
      LOG.info("the policy list:" + idList);
      if (idList.size() != 0) {
        return idList.get(idList.size() - 1);
      }
    } catch (IOException e) {
      LOG.error("get latest policy id from " + prefix + "failed!");
      throw new FDSException("get latest policy id failed, [url:" + prefix + "]", e);
    }
    LOG.error("get latest policy id from " + prefix + " failed, no policy id existed!");
    throw new FDSException(
        "get latest policy id from " + prefix + " failed, no policy id existed!");
  }

  private ArrayList<String> getPolicyIdList(FileStatus[] status) {
    ArrayList<String> idList = new ArrayList<>();
    for (FileStatus fileStatus : status) {
      idList.add(fileStatus.getPath().toString());
    }
    return idList;
  }

  private String getPolicyId(String prefix, String dateTime) throws FDSException {
    try {
      FileStatus[] status = fdsService.getFileStatus(prefix);
      String id = parseId(dateTime);
      for (FileStatus s : status) {
        String idPath = s.getPath().toString();
        if (idPath.contains(id)) {
          return idPath;
        }
      }
    } catch (IOException | ParseException e) {
      LOG.error("get latest policy id from " + prefix + "failed!");
      throw new FDSException("get latest policy id failed, [url:" + prefix + "]", e);
    }
    throw new FDSException("can't match the date time:+" + dateTime);
  }

  private String getTableNameAndId(String prefix, String tableName) throws FDSException {
    String backupInfo;
    String backupInfoUrl = prefix + "/" + "backup_info";
    try (BufferedReader bufferedReader = fdsService.getReader(backupInfoUrl)) {
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
      LOG.error("get table id from " + prefix + "failed!");
      throw new FDSException("get table id failed, [url:" + prefix + "]", e);
    }
    throw new FDSException("can't get the table id");
  }

  private int getCount(String prefix) throws FDSException {
    String appMetaData;
    String appMetaDataUrl = prefix + "/" + "meta" + "/" + "app_metadata";
    try (BufferedReader bufferedReader = fdsService.getReader(appMetaDataUrl)) {
      if ((appMetaData = bufferedReader.readLine()) != null) {
        JSONObject jsonObject = new JSONObject(appMetaData);
        String partitionCount = jsonObject.getString("partition_count");
        return Integer.valueOf(partitionCount);
      }
    } catch (IOException | JSONException e) {
      LOG.error("get the partition count failed from " + appMetaDataUrl, e);
      throw new FDSException("get the partition count failed, [url: " + appMetaDataUrl + "]" + e);
    }
    throw new FDSException("get the partition count failed, [url: " + appMetaDataUrl + "]");
  }

  private String parseId(String dateStr) throws ParseException {
    Date date = simpleDateFormat.parse(dateStr);
    long dateTime = date.getTime();
    return String.valueOf(dateTime).substring(0, 5);
  }
}
