package com.xiaomi.infra.pegasus.spark.common.utils.metaproxy;

import com.xiaomi.infra.pegasus.client.ClientOptions;
import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.client.PegasusClientFactory;
import com.xiaomi.infra.pegasus.client.PegasusClientInterface;
import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.common.utils.HttpClient;
import com.xiaomi.infra.pegasus.spark.common.utils.JsonParser;
import com.xiaomi.infra.pegasus.spark.common.utils.gateway.Cluster;
import java.io.IOException;
import java.util.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

public class ClusterSwitcher {
  private static final Log LOG = LogFactory.getLog(ClusterSwitcher.class);

  public static void switchTableCluster(
      String metaProxyAddr, String table, String originCluster, String targetCluster)
      throws PegasusSparkException {
    switchTableCluster(metaProxyAddr, table, originCluster, targetCluster, false);
  }

  public static void switchTableCluster(
      String metaProxyAddr,
      String table,
      String originCluster,
      String targetCluster,
      boolean allowRegionSame)
      throws PegasusSparkException {

    validateCluster(originCluster, targetCluster, allowRegionSame);

    Pair<String, String> zkInfo = getZkInfo(metaProxyAddr);
    String zkAddr = zkInfo.getKey();
    String zkRoot = zkInfo.getValue();
    ZKClient client = new ZKClient(zkAddr, zkRoot);

    ZkTableInfo origin = validateZKTableInfo(client, originCluster, table);
    ZkTableInfo target =
        new ZkTableInfo(targetCluster, Cluster.getMetaList(targetCluster).meta_servers);

    try {
      if (validateLocalTableInfo(target.meta_addrs, table)) {
        client.writeTableInfo(table, target);
        LOG.info(
            String.format(
                "switch table(%s) cluster from(%s) to(%s) completed",
                table, origin.toString(), target.toString()));
      }
    } catch (Exception e) {
      throw new PegasusSparkException(
          String.format(
              "switch table(%s) cluster from(%s) to(%s) failed: \n%s",
              table, origin.toString(), target.toString(), e.getMessage()));
    }
  }

  private static void validateCluster(String origin, String target, boolean allowRegionSame)
      throws PegasusSparkException {
    String originRegion = getRegion(origin);
    String targetRegion = getRegion(target);
    if (!allowRegionSame && originRegion.equals(targetRegion)) {
      throw new PegasusSparkException(
          String.format(
              "region of `origin` cluster(%s) and `target` cluster(%s) shouldn't be be same!",
              origin, target));
    }
    validateClusterIfContain(originRegion, origin);
    validateClusterIfContain(targetRegion, target);
  }

  private static ZkTableInfo validateZKTableInfo(ZKClient client, String cluster, String table)
      throws PegasusSparkException {
    if (!client.existTableInfo(table)) {
      throw new PegasusSparkException(
          String.format("table(%s) is not existed on %s", table, client.toString()));
    }

    ZkTableInfo remoteTableInfo = client.readTableInfo(table);
    ZkTableInfo localTableInfo =
        new ZkTableInfo(cluster, Cluster.getMetaList(cluster).meta_servers);
    if (!localTableInfo.equals(remoteTableInfo)) {
      throw new PegasusSparkException(
          String.format(
              "table(%s) local cluster(%s) that will be updated is not equal with remote(%s) on %s",
              table, localTableInfo.toString(), remoteTableInfo.toString(), client.toString()));
    }
    return remoteTableInfo;
  }

  private static boolean validateLocalTableInfo(String metas, String table) throws PException {
    // try open target cluster table to validate table if exit
    PegasusClientInterface pegasus =
        PegasusClientFactory.createClient(ClientOptions.builder().metaServers(metas).build());
    pegasus.openTable(table);
    pegasus.close();
    return true;
  }

  private static String getRegion(String name) throws PegasusSparkException {
    String[] split;
    if ((split = name.split("-")).length < 2) {
      throw new PegasusSparkException(String.format("cluster(%s) is invalid!", name));
    }
    return split[0];
  }

  private static void validateClusterIfContain(String region, String cluster)
      throws PegasusSparkException {
    String tankUrl;
    ZkRegionInfo.Region enumRegion = ZkRegionInfo.Region.valueOf(region);
    if (ZkRegionInfo.minos1.containsKey(enumRegion)) {
      tankUrl = "http://tank.d.xiaomi.net/api/get_cluster_info/";
    } else if (ZkRegionInfo.minos2.containsKey(enumRegion)) {
      tankUrl = "http://fusion-tank2.api.xiaomi.net/api/get_cluster_info/";
    } else {
      throw new PegasusSparkException(
          String.format("can't find cluster(%s) region in all regions", cluster));
    }

    Map<String, String> params = new HashMap<>();
    params.put("service", "pegasus");
    params.put("region", enumRegion.toString());
    List<String> clusters = queryClusters(tankUrl, params);
    if (!clusters.contains(String.format("pegasus-%s", cluster))) {
      throw new PegasusSparkException(
          String.format("can't find cluster(%s) from region(%s)", cluster, region));
    }
  }

  private static List<String> queryClusters(String tankUrl, Map<String, String> params)
      throws PegasusSparkException {
    HttpResponse httpResponse = HttpClient.get(tankUrl, params);

    ClusterListInfo clusters;
    String respString = "";
    try {
      int code = httpResponse.getStatusLine().getStatusCode();
      respString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      if (code != 200) {
        throw new PegasusSparkException(
            String.format("query cluster list via %s failed, ErrCode = %d", tankUrl, code));
      }
      clusters = JsonParser.getGson().fromJson(respString, ClusterListInfo.class);
    } catch (IOException e) {
      throw new PegasusSparkException(
          String.format("format the response to string failed: %s", e.getMessage()));
    } catch (RuntimeException e) {
      throw new PegasusSparkException(
          String.format(
              "parser the response to queryResponse failed: %s\n%s", e.getMessage(), respString));
    }

    return new ArrayList<>(Arrays.asList(clusters.retval));
  }

  private static Pair<String, String> getZkInfo(String metaProxyAddr) throws PegasusSparkException {
    ZkRegionInfo.Region zkRegion = ZkRegionInfo.Region.valueOf(getRegion(metaProxyAddr));

    Pair<String, String> zkInfo;
    if ((zkInfo = ZkRegionInfo.minos1.get(zkRegion)) != null) {
      return zkInfo;
    } else if ((zkInfo = ZkRegionInfo.minos2.get(zkRegion)) != null) {
      return zkInfo;
    }
    throw new PegasusSparkException(
        String.format("can't find zk info base meta proxy addrs(%s)", metaProxyAddr));
  }
}
