package com.xiaomi.infra.pegasus.analyser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONException;
import org.json.JSONObject;

public class FDSService implements Serializable {

  private static final Log LOG = LogFactory.getLog(FDSService.class);

  private Config globalConfig;
  private transient Configuration conf = new Configuration();
  private Map<Integer, String> checkpointUrls = new HashMap<>();
  private int partitionCount;

  private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

  public FDSService() throws ConfigurationException {
    globalConfig = new Config();
    LOG.info("init fds default config");
  }

  public FDSService(Config config) {
    globalConfig = config;
    LOG.info("init fds config");
  }

  public FDSService(Config config, String prefix, int counter) throws FDSException {
    globalConfig = config;
    String urlPrefix = globalConfig.DATA_URL + "/" + prefix;
    initCheckpointUrls(urlPrefix, counter);
    LOG.info("init fds default config and get the data urls");
  }

  public FDSService(Config config, String cluster, String table) throws FDSException {
    globalConfig = config;
    String idPrefix =
        globalConfig.DATA_URL + "/" + cluster + "/" + globalConfig.COLDBK_POLICY + "/";
    String latestIdPath = getLatestPolicyId(idPrefix);
    String tableNameAndId = getTableNameAndId(latestIdPath, table);
    String metaPrefix = latestIdPath + "/" + tableNameAndId;

    partitionCount = getCount(metaPrefix);
    initCheckpointUrls(metaPrefix, partitionCount);

    LOG.info("init fds default config and get the latest data urls");
  }

  public FDSService(Config config, String cluster, String table, String dataTime)
      throws FDSException {
    globalConfig = config;
    String idPrefix =
        globalConfig.DATA_URL + "/" + cluster + "/" + globalConfig.COLDBK_POLICY + "/";
    String idPath = getPolicyId(idPrefix, dataTime);
    String tableNameAndId = getTableNameAndId(idPath, table);
    String metaPrefix = idPath + "/" + tableNameAndId;

    partitionCount = getCount(metaPrefix);
    initCheckpointUrls(metaPrefix, partitionCount);

    LOG.info("init fds default config and get the " + dataTime + " data urls");
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public Map<Integer, String> getCheckpointUrls() {
    return checkpointUrls;
  }

  private void initCheckpointUrls(String prefix, int counter) throws FDSException {
    String chkpt;
    counter--;
    while (counter >= 0) {
      String currentCheckpointUrl = prefix + "/" + counter + "/" + "current_checkpoint";
      try (InputStream inputStream =
              FileSystem.get(new URI(currentCheckpointUrl), conf)
                  .open(new Path(currentCheckpointUrl));
          BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
        while ((chkpt = bufferedReader.readLine()) != null) {
          String url = prefix.split(globalConfig.DATA_URL)[1] + "/" + counter + "/" + chkpt;
          checkpointUrls.put(counter, url);
        }
        counter--;
      } catch (IOException | URISyntaxException e) {
        LOG.error("init checkPoint urls failed from " + currentCheckpointUrl);
        throw new FDSException(
            "init checkPoint urls failed, [checkpointUrl:" + currentCheckpointUrl + "]" + e);
      }
    }
  }

  private String getLatestPolicyId(String prefix) throws FDSException {
    try {
      LOG.info("get the " + prefix + " latest id");
      FileSystem fs = FileSystem.get(URI.create(prefix), conf);
      Path Path = new Path(prefix);
      FileStatus[] status = fs.listStatus(Path);
      ArrayList<String> idList = getPolicyIdList(status);
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
      FileSystem fs = FileSystem.get(URI.create(prefix), conf);
      Path Path = new Path(prefix);
      FileStatus[] status = fs.listStatus(Path);
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
    try (InputStream inputStream =
            FileSystem.get(new URI(backupInfoUrl), conf).open(new Path(backupInfoUrl));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
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
    } catch (IOException | URISyntaxException | JSONException e) {
      LOG.error("get latest policy id from " + prefix + "failed!");
      throw new FDSException("get latest policy id failed, [url:" + prefix + "]", e);
    }
    throw new FDSException("can't get the table id");
  }

  private int getCount(String prefix) throws FDSException {
    String appMetaData;
    String appMetaDataUrl = prefix + "/" + "meta" + "/" + "app_metadata";
    try (InputStream inputStream =
            FileSystem.get(new URI(appMetaDataUrl), conf).open(new Path(appMetaDataUrl));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
      while ((appMetaData = bufferedReader.readLine()) != null) {
        JSONObject jsonObject = new JSONObject(appMetaData);
        String partitionCount = jsonObject.getString("partition_count");
        return Integer.valueOf(partitionCount);
      }
    } catch (IOException | JSONException | URISyntaxException e) {
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
