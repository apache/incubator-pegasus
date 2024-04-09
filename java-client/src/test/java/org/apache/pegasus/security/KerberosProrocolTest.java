package org.apache.pegasus.security;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.awaitility.Awaitility.await;

import org.apache.hadoop.minikdc.MiniKdc;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pegasus.client.PException;
import org.apache.pegasus.client.PegasusClientFactory;
import org.apache.pegasus.client.PegasusClientInterface;
import org.apache.pegasus.security.KerberosProtocol;
import org.apache.pegasus.rpc.async.ReplicaSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosProrocolTest {
  private static final Logger logger = LoggerFactory.getLogger(KerberosProrocolTest.class);

  private static File baseDir;
  private static String Principal;
  private static MiniKdc kdc;
  private static String keytab;
  private static String spnegoPrincipal;

  private static PegasusClientInterface client;

  private static File confDir;
  private static final String USER_NAME = "root";
  public static void initKdc() throws Exception {
    baseDir =
        new File(
            System.getProperty("test.build.dir", "target/test-dir"),
            KerberosProrocolTest.class.getSimpleName());
    FileUtil.fullyDelete(baseDir);
    assertTrue(baseDir.mkdirs());
    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(USER_NAME);
    UserGroupInformation.setLoginUser(ugi);
    String userName = UserGroupInformation.getLoginUser().getShortUserName();
    File keytabFile = new File(baseDir, userName + ".keytab");
    keytab = keytabFile.getAbsolutePath();
    kdc.createPrincipal(keytabFile, userName + "/localhost", "HTTP/localhost");
    Principal = userName + "/localhost@" + kdc.getRealm();
    spnegoPrincipal = "HTTP/localhost@" + kdc.getRealm();
    logger.info(
        "keytab "
            + keytab
            + " Principal "
            + Principal
            + " spnegoPrincipal "
            + spnegoPrincipal);
    logger.info("krb_conf = {}", kdc.getKrb5conf().toString());
  }


  @BeforeAll
  static void setUp() throws Exception {
    // 1.启动 pegasus onebox 集群
    // 2.启动 kdc
    // 3.启动客户端，去访问不存在的表
    confDir = new File("target", KerberosProrocolTest.class.getSimpleName());
    initKdc();
    client = PegasusClientFactory.getSingletonClient();
  }

  @AfterAll
  static void shutDown() throws Exception {
    FileUtils.deleteDirectory(confDir);
    if (kdc != null) {
      kdc.stop();
    }
    FileUtil.fullyDelete(baseDir);
    client.close();
  }

  @Test
  public void testKerberosProrocolTest() {
    // 3.客户端去访问不存在的表
    for(int i = 0; i < 100; i++) {
      try {
        client.openTable("qweabcdef");
      } catch (PException e) {
        logger.info("Make client to open a not exist table to simulation TGT renew thread leak");
      }
    }

    // 4.检查是否有多的 TGT 线程存在
    logger.info("serviceName = {}, serviceFqdn = {}, keytab = {}, principal = {}", USER_NAME, kdc.getHost(), keytab, Principal);
    KerberosProtocol kerberosProtocol = new KerberosProtocol(USER_NAME, kdc.getHost(), keytab, Principal);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e){
      Thread.currentThread().interrupt();
    }

    logger.info("active thread count = {}", Thread.activeCount());
    logger.info("Thread name = {}",Thread.currentThread().getName());
    kdc.stop();
    logger.info("active thread count = {}", Thread.activeCount());
//    logger.info(" {}", KerberosProtocol.service.isTerminated());
    await().atMost(60, TimeUnit.SECONDS).until(this::isThreadLeak);

  }

  private boolean isThreadLeak() throws IOException {
    return Thread.activeCount() == 1;
  }
}
