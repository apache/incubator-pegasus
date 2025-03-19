package com.xiaomi.infra.pegasus.spark.common.utils.gateway;

import com.google.gson.reflect.TypeToken;
import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.common.utils.HttpClient;
import com.xiaomi.infra.pegasus.spark.common.utils.JsonParser;
import com.xiaomi.infra.pegasus.spark.common.utils.metaproxy.ClusterStateInfo;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

public class Cluster {
  private static final Log LOG = LogFactory.getLog(Cluster.class);

  public static String metaGateWay = "http://pegasus-gateway.hadoop.srv/";

  public static ClusterStateInfo getMetaList(String cluster) throws PegasusSparkException {
    String path = String.format("%s/v1/%s/meta/cluster", metaGateWay, cluster);

    ClusterStateInfo clusterStateInfo;
    String respString = "";
    HttpResponse httpResponse = HttpClient.get(path, new HashMap<>());
    try {
      int code = httpResponse.getStatusLine().getStatusCode();
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      if (code != 200) {
        throw new PegasusSparkException(
            String.format(
                "get meta[%s] list from gateway failed, ErrCode = %d, err = %s",
                cluster, code, respString));
      }
      clusterStateInfo = JsonParser.getGson().fromJson(respString, ClusterStateInfo.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to tableInfo failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to tableInfo failed: %s\n%s", e.getMessage(), respString));
    }

    return clusterStateInfo;
  }

  public static TableInfo getTableInfo(String cluster, String table) throws PegasusSparkException {
    String path = String.format(metaGateWay + "/%s/meta/app", cluster);
    Map<String, String> params = new HashMap<>();
    params.put("name", table);
    params.put("detail", "");

    TableInfo tableInfo;
    String respString = "";
    HttpResponse httpResponse = HttpClient.get(path, params);
    try {
      int code = httpResponse.getStatusLine().getStatusCode();
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      if (code != 200) {
        throw new PegasusSparkException(
            String.format(
                "get tableInfo[%s(%s)] from gateway failed, ErrCode = %d, err = %s",
                cluster, table, code, respString));
      }

      tableInfo = JsonParser.getGson().fromJson(respString, TableInfo.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to tableInfo failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to tableInfo failed: %s\n%s", e.getMessage(), respString));
    }
    return tableInfo;
  }

  public static int getTableVersion(String cluster, String table) throws PegasusSparkException {
    return getTableVersion(getTableInfo(cluster, table));
  }

  public static int getTableVersion(TableInfo tableInfo) throws PegasusSparkException {
    AtomicInteger replicaCount = new AtomicInteger();
    ConcurrentHashMap<String, String> version = new ConcurrentHashMap<>();
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    Map<String, String> params = new HashMap<>();
    params.put("app_id", tableInfo.general.app_id);
    for (String node : tableInfo.nodes.keySet()) {
      if (node.equals("total")) {
        continue;
      }
      String path = String.format("http://%s/replica/data_version", node);
      futures.add(
          CompletableFuture.runAsync(
              () -> {
                try {
                  HttpResponse httpResponse = HttpClient.get(path, params);
                  int code = httpResponse.getStatusLine().getStatusCode();
                  if (code != 200) {
                    throw new PegasusSparkException(
                        String.format(
                            "get table[%s(%s)] version from replica[%s] failed, ErrCode = %d",
                            tableInfo.general.app_name, tableInfo.general.app_id, node, code));
                  }
                  String resp = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
                  Type type = new TypeToken<HashMap<String, ReplicaVersion>>() {}.getType();
                  Map<String, ReplicaVersion> replicaVersionMap =
                      JsonParser.getGson().fromJson(resp, type);
                  for (Map.Entry<String, ReplicaVersion> replica : replicaVersionMap.entrySet()) {
                    replicaCount.incrementAndGet();
                    version.putIfAbsent(
                        replica.getValue().data_version, replica.getValue().data_version);
                  }
                } catch (PegasusSparkException | IOException e) {
                  throw new RuntimeException(String.format("get table version failed:%s", e));
                }
              }));
    }

    CompletableFuture<Void> futureAll =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    futureAll.join();

    if (version.size() == 0) {
      throw new PegasusSparkException(
          String.format(
              "table[%s] version init failed, not allow to use bulkload!",
              tableInfo.general.app_name));
    }

    if (version.size() != 1) {
      throw new PegasusSparkException(
          String.format(
              "table[%s] has multi version, not allow to use " + "bulkload!",
              tableInfo.general.app_name));
    }

    if (replicaCount.get()
        != tableInfo.replicas.size() * Integer.parseInt(tableInfo.general.max_replica_count)) {
      throw new PegasusSparkException(
          String.format(
              "table[%s] has not enough replica(expect=%d, actual=%d), not allow to use bulkload!",
              tableInfo.general.app_name, tableInfo.replicas.size() * 3, replicaCount.get()));
    }

    return Integer.parseInt(version.keys().nextElement());
  }

  public static void startBackup(
      String cluster, String table, String remoteFileSystem, String remotePath)
      throws PegasusSparkException, InterruptedException {
    LOG.info(
        String.format(
            "start export %s.%s to hdfs %s/%s", cluster, table, remoteFileSystem, remotePath));
    BackupInfo.ExecuteResponse executeResponse =
        Cluster.sendBackupRequest(cluster, table, remoteFileSystem, remotePath);
    if (!executeResponse.err.Errno.equals("ERR_OK")) {
      throw new PegasusSparkException(
          executeResponse.err.Errno + " : " + executeResponse.hint_message);
    }
    BackupInfo.QueryResponse queryResponse =
        Cluster.queryBackupResult(cluster, table, String.valueOf(executeResponse.backup_id));
    while (queryResponse.err.Errno.equals("ERR_OK")
        && queryResponse.backup_items.length == 1
        && queryResponse.backup_items[0].end_time_ms == 0) {
      if (queryResponse.backup_items[0].is_backup_failed) {
        throw new PegasusSparkException(
            String.format(
                "export %s.%s to %s %s failed, please check the pegasus server log",
                cluster, table, remoteFileSystem, remotePath));
      }

      LOG.warn(
          String.format(
              "export %s.%s to %s %s is running", cluster, table, remoteFileSystem, remotePath));
      Thread.sleep(10000);
      queryResponse =
          Cluster.queryBackupResult(cluster, table, String.valueOf(executeResponse.backup_id));
    }

    if (queryResponse.backup_items[0].end_time_ms == 0) {
      throw new PegasusSparkException(
          String.format(
              "export %s.%s to %s %s failed = [%s]%s, please check the pegasus server log",
              queryResponse.err.Errno,
              queryResponse.hint_message,
              cluster,
              table,
              remoteFileSystem,
              remotePath));
    }

    LOG.info(
        String.format(
            "export %s.%s to %s %s is completed", cluster, table, remoteFileSystem, remotePath));
  }

  private static BackupInfo.ExecuteResponse sendBackupRequest(
      String cluster, String table, String remoteFileSystem, String remotePath)
      throws PegasusSparkException {
    String path = String.format("%s/v1/backupManager/%s/backup", metaGateWay, cluster);

    BackupInfo.ExecuteRequest executeRequest = new BackupInfo.ExecuteRequest();
    executeRequest.TableName = table;
    executeRequest.BackupProvider = remoteFileSystem;
    executeRequest.BackupPath = remotePath;
    HttpResponse httpResponse = HttpClient.post(path, JsonParser.getGson().toJson(executeRequest));

    BackupInfo.ExecuteResponse backupExecuteResponse;
    String respString = "";
    try {
      int code = httpResponse.getStatusLine().getStatusCode();
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      if (code != 200) {
        throw new PegasusSparkException(
            String.format(
                "start backup[%s(%s)] via gateway failed, ErrCode = %d, err = %s",
                cluster, table, code, respString));
      }

      backupExecuteResponse =
          JsonParser.getGson().fromJson(respString, BackupInfo.ExecuteResponse.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to tableInfo failed: %s\n%s", e.getMessage(), respString));
    }
    return backupExecuteResponse;
  }

  private static BackupInfo.QueryResponse queryBackupResult(String cluster, String table, String id)
      throws PegasusSparkException {
    String path = String.format("%s/v1/backupManager/%s/%s/%s", metaGateWay, cluster, table, id);
    Map<String, String> params = new HashMap<>();
    HttpResponse httpResponse = HttpClient.get(path, params);

    BackupInfo.QueryResponse queryResponse;
    String respString = "";
    try {
      int code = httpResponse.getStatusLine().getStatusCode();
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      if (code != 200) {
        throw new PegasusSparkException(
            String.format(
                "query backup[%s(%s)] via gateway failed, ErrCode = %d", cluster, table, code));
      }
      queryResponse = JsonParser.getGson().fromJson(respString, BackupInfo.QueryResponse.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to queryResponse failed: %s\n%s", e.getMessage(), respString));
    }
    return queryResponse;
  }

  public static void startBulkLoad(
      String cluster, String table, String remoteFileSystem, String remotePath)
      throws PegasusSparkException, InterruptedException {
    startBulkLoad(cluster, table, remoteFileSystem, remotePath, new Compaction("03:00", 1, true));
  }

  public static void startBulkLoad(
      String cluster,
      String table,
      String remoteFileSystem,
      String remotePath,
      Compaction compaction)
      throws InterruptedException, PegasusSparkException {
    LOG.info(
        String.format(
            "start import hdfs %s/%s to pegasus %s.%s",
            remoteFileSystem, remotePath, cluster, table));

    BulkLoadInfo.QueryResponse queryResponse = queryBulkLoadResult(cluster, table);
    if (queryResponse.app_status.contains("BLS_CANCEL")) {
      throw new PegasusSparkException("the last bulkload is " + queryResponse.app_status);
    }

    if (queryResponse.app_status.contains("BLS_FAILED")) {
      throw new PegasusSparkException(
          String.format(
              "the last bulkload is %s,%s: %s",
              queryResponse.app_status, queryResponse.err.Errno, queryResponse.hint_msg));
    }

    Compaction.Response resp = queryTableCompaction(cluster, table);
    if (!resp.err.Errno.equals("ERR_INVALID_STATE") && resp.progress != 100) {
      throw new PegasusSparkException(
          String.format(
              "the current table[%s.%s] is running compaction[err=%s, process=%d], not allow start bulkload",
              cluster, table, resp.err.Errno, resp.progress));
    }

    BulkLoadInfo.ExecuteResponse executeResponse =
        sendBulkLoadRequest(cluster, table, remoteFileSystem, remotePath);
    while (!executeResponse.err.Errno.equals("ERR_OK")) {
      if (executeResponse.err.Errno.equals("ERR_BUSY")) {
        queryResponse = queryBulkLoadResult(cluster, table);
        if (queryResponse.app_status.contains("BLS_CANCEL")) {
          throw new PegasusSparkException(
              String.format(
                  "%s : last %s.%s bulkload is %s",
                  executeResponse.err.Errno, cluster, table, queryResponse.app_status));
        }
        LOG.info(
            String.format(
                "%s : last bulkload[%s.%s] is running, process %s",
                executeResponse.err.Errno, cluster, table, queryResponse.app_status));
        Thread.sleep(10000);
        executeResponse = sendBulkLoadRequest(cluster, table, remoteFileSystem, remotePath);
        continue;
      }
      throw new PegasusSparkException(executeResponse.err.Errno + " : " + executeResponse.hint_msg);
    }

    try {
      Thread.sleep(10000);
      setBulkLoadMod(cluster, table);
      queryResponse = queryBulkLoadResult(cluster, table);
      if (!queryResponse.app_status.equals("BLS_DOWNLOADING")) {
        LOG.warn(
            queryResponse.err.Errno
                + " : the first stage should be BLS_DOWNLOADING, but now is "
                + queryResponse.app_status);
      }

      while (queryResponse.err.Errno.equals("ERR_OK")) {
        if (queryResponse.app_status.contains("BLS_CANCEL")) {
          throw new PegasusSparkException(
              String.format("%s.%s bulkload is %s", cluster, table, queryResponse.app_status));
        }

        if (queryResponse.app_status.contains("BLS_FAILED")) {
          throw new PegasusSparkException(
              String.format(
                  "bulkload[%s.%s] failed. message = %s", cluster, table, queryResponse.hint_msg));
        }

        if (queryResponse.app_status.contains("BLS_SUCCEED")) {
          LOG.info(
              String.format(
                  "bulkload[%s.%s] is completed, process %s",
                  cluster, table, queryResponse.app_status));
          createManualCompaction(cluster, table, compaction);
          return;
        }

        LOG.info(
            String.format(
                "bulkload[%s.%s] is running, process %s",
                cluster, table, queryResponse.app_status));
        Thread.sleep(10000);
        queryResponse = queryBulkLoadResult(cluster, table);
      }

      throw new PegasusSparkException(
          String.format(
              "bulkload[%s.%s] failed. err = %s,  message = %s",
              cluster, table, queryResponse.err.Errno, queryResponse.hint_msg));
    } catch (PegasusSparkException e) {
      LOG.error(
          "bulkload failed and force create periodic manual compaction: " + compaction.toString());
      createManualCompaction(cluster, table, new Compaction("00:05", 1, false));
      throw new PegasusSparkException("bulkload failed: " + e.getMessage());
    }
  }

  private static BulkLoadInfo.ExecuteResponse sendBulkLoadRequest(
      String cluster, String table, String remoteFileSystem, String remotePath)
      throws PegasusSparkException {
    String path = String.format("%s/v1/bulkloadManager/start", metaGateWay);

    BulkLoadInfo.ExecuteRequest executeRequest = new BulkLoadInfo.ExecuteRequest();
    executeRequest.ClusterName = cluster;
    executeRequest.TableName = table;
    executeRequest.RemoteProvider = remoteFileSystem;
    executeRequest.RemotePath = remotePath;
    HttpResponse httpResponse = HttpClient.post(path, JsonParser.getGson().toJson(executeRequest));

    BulkLoadInfo.ExecuteResponse bulkloadExecuteResponse;
    String respString = "";
    try {
      int code = httpResponse.getStatusLine().getStatusCode();
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      if (code != 200) {
        throw new PegasusSparkException(
            String.format(
                "start bulkload[%s(%s)] via gateway[%s] failed, ErrCode = %d, error = %s",
                cluster, table, path, code, respString));
      }
      bulkloadExecuteResponse =
          JsonParser.getGson().fromJson(respString, BulkLoadInfo.ExecuteResponse.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to tableInfo failed: %s\n%s", e.getMessage(), respString));
    }
    return bulkloadExecuteResponse;
  }

  private static BulkLoadInfo.QueryResponse queryBulkLoadResult(String cluster, String table)
      throws PegasusSparkException {
    String path = String.format("%s/v1/bulkloadManager/%s/%s", metaGateWay, cluster, table);
    Map<String, String> params = new HashMap<>();
    HttpResponse httpResponse = HttpClient.get(path, params);

    BulkLoadInfo.QueryResponse queryResponse;
    String respString = "";
    try {
      int code = httpResponse.getStatusLine().getStatusCode();
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      if (code != 200) {
        throw new PegasusSparkException(
            String.format(
                "query bulkload[%s(%s)] via gateway[%s] failed, ErrCode = %d, error = %s",
                cluster, table, path, code, respString));
      }
      queryResponse = JsonParser.getGson().fromJson(respString, BulkLoadInfo.QueryResponse.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      if (respString.contains("is not during bulk load")) {
        LOG.warn(String.format("%s.%s is not during bulk load", cluster, table));
        BulkLoadInfo.QueryResponse response = new BulkLoadInfo.QueryResponse();
        response.err = new BulkLoadInfo.Error("ERR_OK");
        response.app_status = "BLS_INVALID";
        response.hint_msg = e.getMessage();
        return response;
      } else {
        throw new PegasusSparkException(
            String.format(
                "parser the response to queryResponse failed: %s\n%s", e.getMessage(), respString));
      }
    }
    return queryResponse;
  }

  public static BulkLoadInfo.CancelResponse cancelBulkLoad(String cluster, String table)
      throws PegasusSparkException {
    LOG.info(String.format("cancel import pegasus %s.%s", cluster, table));
    String path = String.format("%s/v1/bulkloadManager/cancel/", metaGateWay);
    BulkLoadInfo.CancelRequest cancelRequest = new BulkLoadInfo.CancelRequest();
    cancelRequest.ClusterName = cluster;
    cancelRequest.TableName = table;
    HttpResponse httpResponse = HttpClient.post(path, JsonParser.getGson().toJson(cancelRequest));

    BulkLoadInfo.CancelResponse cancelResponse;
    String respString = "";
    try {
      int code = httpResponse.getStatusLine().getStatusCode();
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      if (code != 200) {
        throw new PegasusSparkException(
            String.format(
                "cancel bulkload[%s(%s)] via gateway[%s] failed, ErrCode = %d, err = %s",
                cluster, table, path, code, respString));
      }
      cancelResponse = JsonParser.getGson().fromJson(respString, BulkLoadInfo.CancelResponse.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to queryResponse failed: %s\n%s", e.getMessage(), respString));
    }
    return cancelResponse;
  }

  public static void createManualCompaction(String cluster, String table, Compaction compaction)
      throws PegasusSparkException, InterruptedException {
    sendCompactionRequest(cluster, table, compaction);
    if (!compaction.triggerAfterLoaded) {
      LOG.warn(
          "disable compaction after this data load completed, it will be triggered at "
              + compaction.periodicTriggerTime
              + " every day");
      return;
    }
    LOG.info(
        String.format(
            "start compact %s.%s and it will be trigger at %s every day",
            cluster, table, compaction.periodicTriggerTime));
    Thread.sleep(60000); // wait to the perf is updated
    waitTableCompactionCompleted(cluster, table);
    LOG.warn(String.format("%s.%s compaction is completed, set env as normal mod", cluster, table));
    setNormalMod(cluster, table);
  }

  public static void createManualCompaction(
      String cluster, List<String> tables, Compaction compaction)
      throws PegasusSparkException, InterruptedException {
    for (String table : tables) {
      sendCompactionRequest(cluster, table, compaction);
      if (!compaction.triggerAfterLoaded) {
        LOG.warn(
            "disable compaction after this data load completed, it will be triggered at "
                + compaction.periodicTriggerTime);
      } else {
        LOG.info(
            String.format(
                "start compact %s.%s and it will be trigger at %s",
                cluster, table, compaction.periodicTriggerTime));
      }
    }

    if (!compaction.triggerAfterLoaded) {
      return;
    }

    Thread.sleep(60000); // wait to the perf is updated
    for (String table : tables) {
      waitTableCompactionCompleted(cluster, table);
      LOG.warn(
          String.format("%s.%s compaction is completed, set env as normal mod", cluster, table));
      setNormalMod(cluster, table);
    }
  }

  public static void disableManualCompaction(String cluster, String table)
      throws PegasusSparkException {
    Map<String, String> envs = new HashMap<>();
    envs.put("manual_compact.disabled", "true");
    setTableEnv(cluster, table, envs);
  }

  public static void disableManualCompaction(String cluster, List<String> tables)
      throws PegasusSparkException {
    Map<String, String> envs = new HashMap<>();
    envs.put("manual_compact.disabled", "true");
    for (String tb : tables) {
      setTableEnv(cluster, tb, envs);
    }
  }

  private static void setBulkLoadMod(String cluster, String table) throws PegasusSparkException {
    Map<String, String> envs = new HashMap<>();
    envs.put("rocksdb.usage_scenario", "bulk_load");
    setTableEnv(cluster, table, envs);
  }

  private static void setNormalMod(String cluster, String table) throws PegasusSparkException {
    Map<String, String> envs = new HashMap<>();
    envs.put("rocksdb.usage_scenario", "normal");
    setTableEnv(cluster, table, envs);
  }

  private static void sendCompactionRequest(String cluster, String table, Compaction compaction)
      throws PegasusSparkException {
    Map<String, String> envs = new HashMap<>();
    envs.put("manual_compact.max_concurrent_running_count", compaction.concurrent);
    envs.put("manual_compact.disabled", "false");
    if (compaction.triggerAfterLoaded) {
      envs.put("manual_compact.once.bottommost_level_compaction", "skip");
      envs.put("manual_compact.once.target_level", "-1");
      envs.put(
          "manual_compact.once.trigger_time",
          String.valueOf(System.currentTimeMillis() / 1000 + 10));
    }
    envs.put("manual_compact.periodic.bottommost_level_compaction", "skip");
    envs.put("manual_compact.periodic.target_level", "-1");
    envs.put("manual_compact.periodic.trigger_time", compaction.periodicTriggerTime);
    setTableEnv(cluster, table, envs);
  }

  private static void waitTableCompactionCompleted(String cluster, String table)
      throws PegasusSparkException, InterruptedException {
    while (true) {
      Compaction.Response resp = queryTableCompaction(cluster, table);
      if (resp.err.Errno.equals("ERR_INVALID_STATE")) {
        LOG.info(String.format("%s.%s compaction has not running", cluster, table));
      } else if (!resp.err.Errno.equals("ERR_OK")) {
        LOG.info(
            String.format("%s.%s query compaction failed, err = %s", cluster, table, resp.err));
      } else {
        if (resp.progress == 100) {
          LOG.info(String.format("%s.%s compaction is completed", cluster, table));
          return;
        } else {
          LOG.info(
              String.format(
                  "%s.%s is running compaction, process = %d", cluster, table, resp.progress));
        }
      }
      Thread.sleep(30000);
    }
  }

  private static Compaction.Response queryTableCompaction(String cluster, String table)
      throws PegasusSparkException {
    String path = String.format("%s/v1/manualCompaction/%s/%s", metaGateWay, cluster, table);
    Map<String, String> params = new HashMap<>();
    HttpResponse httpResponse = HttpClient.get(path, params);

    Compaction.Response queryResponse;
    String respString = "";
    try {
      int code = httpResponse.getStatusLine().getStatusCode();
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      if (code != 200) {
        throw new PegasusSparkException(
            String.format(
                "query compaction[%s(%s)] via gateway[%s] failed, ErrCode = %d, error = %s",
                cluster, table, path, code, respString));
      }
      queryResponse = JsonParser.getGson().fromJson(respString, Compaction.Response.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to tableInfo failed: %s\n%s", e.getMessage(), respString));
    }
    return queryResponse;
  }

  private static boolean queryCompactionIfCompleted(String cluster)
      throws PegasusSparkException, InterruptedException {
    Map<String, Double> result = queryCompactionResult(cluster);
    for (Double value : result.values()) {
      if (value > 0) {
        return false;
      }
    }
    return true;
  }

  private static Map<String, Double> queryCompactionResult(String cluster)
      throws PegasusSparkException, InterruptedException {
    String counterName = "replica*app.pegasus*manual.compact.running.count";
    return queryPerfCounter(cluster, counterName);
  }

  private static void setTableEnv(String cluster, String table, Map<String, String> envs)
      throws PegasusSparkException {
    String path = String.format("%s/v1/%s/%s/envs", metaGateWay, cluster, table);
    String envStr = JsonParser.getGson().toJson(envs);
    LOG.info(String.format("%s.%s update envs to %s", cluster, table, envStr));
    HttpResponse httpResponse = HttpClient.post(path, envStr);
    String respString = "";
    try {
      int code = httpResponse.getStatusLine().getStatusCode();
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      if (code != 200) {
        throw new PegasusSparkException(
            String.format(
                "set envs[%s(%s)]=>%s via gateway[%s] failed, ErrCode = %d, err = %s",
                cluster, table, envStr, path, code, respString));
      }
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to queryResponse failed: %s\n%s", e.getMessage(), respString));
    }
  }

  private static Map<String, Double> queryPerfCounter(String cluster, String counterName)
      throws PegasusSparkException {
    String path = String.format("%s/v1/tableManager/%s/perf", metaGateWay, cluster);
    HttpResponse httpResponse = HttpClient.get(path, new HashMap<>());

    String respString = "";
    Map<String, Double> results = new HashMap<>();
    try {
      int code = httpResponse.getStatusLine().getStatusCode();
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      if (code != 200) {
        throw new PegasusSparkException(
            String.format(
                "get perf[%s] via gateway[%s] failed, ErrCode = %d, err = %s",
                cluster, path, code, respString));
      }
      // addr=>{addr=>{counter=>value}}
      Map<String, Map<String, Map<String, Double>>> stats =
          JsonParser.getGson().fromJson(respString, Map.class);
      for (Map.Entry<String, Map<String, Map<String, Double>>> stat : stats.entrySet()) {
        String addr = stat.getKey();
        Map<String, Double> counters = stat.getValue().get("Stats");
        Double value = counters.get(counterName);
        results.put(addr, value);
      }
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to queryResponse failed: %s\n%s",
              e.getMessage(),
              respString.length() < 100 ? respString : respString.substring(0, 100)));
    }
    return results;
  }
}
