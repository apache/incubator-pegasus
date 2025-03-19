package com.xiaomi.infra.pegasus.spark.common.utils.metaproxy;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

public class ZkRegionInfo {
  enum Region {
    tjwqtst("tjwq"),
    c3srv("c3"),
    c3tst("c3"),
    c4srv("c4"),
    c4tst("c4"),
    zjysrv("zjy"),
    c6cloudsrv("c6"),
    alsgsrv("alsg"),
    azdesrv("azde"),
    azmbcloudsrv("azmb"),
    ksmoscloudsrv("ksmos"),
    azorsrv("azor"),
    azamssrv("azmas");

    private final String name;

    Region(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public static final Map<Region, Pair<String, String>> minos1 =
      new HashMap<Region, Pair<String, String>>() {
        {
          put(Region.tjwqtst, Pair.of("tjwqtst.zk.hadoop.srv:21000", "/pegasus/tjwqtst-metaproxy"));
          put(Region.c3srv, Pair.of("c3cloudsrv.zk.hadoop.srv:11000", "/pegasus/c3srv-metaproxy"));
          put(Region.c3tst, Pair.of("c3cloudsrv.zk.hadoop.srv:11000", "/pegasus/c3srv-metaproxy"));
          put(Region.c4srv, Pair.of("c4cloudsrv.zk.hadoop.srv:11000", "/pegasus/c4srv-metaproxy"));
          put(Region.c4tst, Pair.of("c4cloudsrv.zk.hadoop.srv:11000", "/pegasus/c4srv-metaproxy"));
          put(Region.zjysrv, Pair.of("zjysrv.zk.hadoop.srv:21000", "/pegasus/zjysrv-metaproxy"));
          put(
              Region.alsgsrv,
              Pair.of("alsgcloudsrv.zk.hadoop.srv:11000", "/pegasus/alsgcloudsrv-metaproxy"));
          put(Region.azorsrv, Pair.of("azorsrv.zk.hadoop.srv:11000", "/pegasus/azorsrv-metaproxy"));
        }
      };

  public static final Map<Region, Pair<String, String>> minos2 =
      new HashMap<Region, Pair<String, String>>() {
        {
          put(Region.azdesrv, Pair.of("azdesrv.zk.hadoop.srv:11000", "/pegasus/azdesrv-metaproxy"));
          put(
              Region.azmbcloudsrv,
              Pair.of("azmbcloudsrv.zk.hadoop.srv:11000", "/pegasus/azmbcloudsrv-metaproxy"));
          put(
              Region.c6cloudsrv,
              Pair.of("c6cloudsrv.zk.hadoop.srv:11000", "/pegasus/c6cloudsrv-metaproxy"));
          put(
              Region.ksmoscloudsrv,
              Pair.of("ksmoscloudsrv.zk.hadoop.srv:11000", "/pegasus/ksmoscloudsrv-metaproxy"));
          put(Region.azamssrv, Pair.of("azamssrv.zk.hadoop.srv:11000", "/pegasus/azams-metaproxy"));
        }
      };
}
