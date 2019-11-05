package com.xiaomi.infra.pegasus.analyser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
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
import org.json.JSONObject;

public class FdsService implements Serializable {

  private static final Log LOG = LogFactory.getLog(FdsService.class);

  private Config globalConfig;
  private transient Configuration conf = new Configuration();
  private Map<Integer, String> checkpointUrls = new HashMap<>();
  private int partitionCount;

  private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

  public FdsService() throws ConfigurationException {
    globalConfig = new Config();
    LOG.info("Init fds default config");
  }

  public FdsService(Config config) {
    globalConfig = config;
    LOG.info("Init fds config");
  }

  public FdsService(Config config, String prefix, int counter) {
    globalConfig = config;
    String urlPrefix = globalConfig.DATA_URL + "/" + prefix;
    initCheckpointUrls(urlPrefix, counter);
    LOG.info("Init fds default config and get the data urls");
  }

  public FdsService(Config config, String cluster, String table) throws IOException {
    globalConfig = config;
    String idPrefix =
        globalConfig.DATA_URL + "/" + cluster + "/" + globalConfig.COLDBK_POLICY + "/";
    String latestId = getLatestPolicyId(idPrefix);

    String tablePrefix = latestId + "/";
    String tableNameAndId = getTableNameAndId(tablePrefix, table);

    String metaPrefix = tablePrefix + "/" + tableNameAndId + "/";
    partitionCount = getCounter(metaPrefix);

    initCheckpointUrls(metaPrefix, partitionCount);

    LOG.info("Init fds default config and get the latest data urls");
  }

  public FdsService(Config config, String cluster, String table, String dataTime)
      throws IOException, ParseException {
    LOG.info("The default URL/FileSystem:" + FileSystem.getDefaultUri(new Configuration()));
    globalConfig = config;
    String idPrefix =
        globalConfig.DATA_URL + "/" + cluster + "/" + globalConfig.COLDBK_POLICY + "/";
    String id = getPolicyId(idPrefix, dataTime);

    String tablePrefix = id + "/";
    String tableNameAndId = getTableNameAndId(tablePrefix, table);

    String metaPrefix = tablePrefix + "/" + tableNameAndId + "/";
    partitionCount = getCounter(metaPrefix);

    initCheckpointUrls(metaPrefix, partitionCount);

    LOG.info("Init fds default config and get the " + dataTime + " data urls");
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public Map<Integer, String> getCheckpointUrls() {
    return checkpointUrls;
  }

  private void initCheckpointUrls(String prefix, int counter) {
    String chkpt;
    counter--;
    while (counter >= 0) {
      String currentCheckpointUrl = prefix + "/" + counter + "/" + "current_checkpoint";
      try (InputStream inputStream =
              FileSystem.get(new URI(currentCheckpointUrl), conf)
                  .open(new Path(currentCheckpointUrl));
          BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
        while ((chkpt = bufferedReader.readLine()) != null) {
          String url = prefix.split(globalConfig.DATA_URL)[1] + "/" + counter + "/" + chkpt + "/";
          checkpointUrls.put(counter, url);
        }
        counter--;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private String getLatestPolicyId(String prefix) throws IOException {
    LOG.info("Get the " + prefix + " latest id");
    FileSystem fs = FileSystem.get(URI.create(prefix), conf);
    Path Path = new Path(prefix);
    FileStatus[] status = fs.listStatus(Path);
    ArrayList<String> idList = getPolicyIdList(status);
    LOG.info("The policy list:" + idList);
    return idList.get(idList.size() - 1);
  }

  private ArrayList<String> getPolicyIdList(FileStatus[] status) {
    ArrayList<String> idList = new ArrayList<>();
    for (FileStatus fileStatus : status) {
      idList.add(fileStatus.getPath().toString());
    }
    return idList;
  }

  private String getPolicyId(String prefix, String dateTime) throws IOException, ParseException {
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
    return null;
  }

  private String getTableNameAndId(String prefix, String tableName) {
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
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  private int getCounter(String prefix) {
    String appMetaData = null;
    String appMetaDataUrl = prefix + "/" + "meta" + "/" + "app_metadata";
    try (InputStream inputStream =
            FileSystem.get(new URI(appMetaDataUrl), conf).open(new Path(appMetaDataUrl));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
      while ((appMetaData = bufferedReader.readLine()) != null) {
        JSONObject jsonObject = new JSONObject(appMetaData);
        String partitionCount = jsonObject.getString("partition_count");
        return Integer.valueOf(partitionCount);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return -1;
  }

  private String parseId(String dateStr) throws ParseException {
    Date date = simpleDateFormat.parse(dateStr);
    long dateTime = date.getTime();
    return String.valueOf(dateTime).substring(0, 6);
  }
}
