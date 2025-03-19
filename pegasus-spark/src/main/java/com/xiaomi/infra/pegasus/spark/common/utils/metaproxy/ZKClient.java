package com.xiaomi.infra.pegasus.spark.common.utils.metaproxy;

import com.xiaomi.infra.pegasus.spark.common.utils.JsonParser;
import com.xiaomi.infra.pegasus.thirdparty.org.I0Itec.zkclient.ZkClient;
import com.xiaomi.infra.pegasus.thirdparty.org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.zookeeper.data.Stat;

public class ZKClient {
  public final ZkClient client;

  public final String addr;
  public final String root;

  public ZKClient(String addr, String root) {
    this.addr = addr;
    this.root = root;

    this.client = new ZkClient(addr, 30000, 30000, new BytesPushThroughSerializer());
  }

  public ZkTableInfo readTableInfo(String table) {
    String tablePath = String.format("%s/%s", root, table);
    String tableInfoStr = new String((byte[]) client.readData(tablePath));
    return JsonParser.getGson().fromJson(tableInfoStr, ZkTableInfo.class);
  }

  public boolean existTableInfo(String table) {
    String tablePath = String.format("%s/%s", root, table);
    return client.exists(tablePath);
  }

  public Stat writeTableInfo(String table, ZkTableInfo tableInfo) {
    String tablePath = String.format("%s/%s", root, table);
    String info =
        String.format(
            "{\"cluster_name\": \"%s\", \"meta_addrs\": \"%s\"}",
            tableInfo.cluster_name, tableInfo.meta_addrs);
    return client.writeData(tablePath, info.getBytes());
  }

  @Override
  public String toString() {
    return "ZKServerPath{" + "addr='" + addr + '\'' + ", root='" + root + '\'' + '}';
  }
}
