package com.xiaomi.infra.pegasus.rpc.async;

import com.xiaomi.infra.pegasus.client.ClientOptions;
import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.client.PegasusClientFactory;
import com.xiaomi.infra.pegasus.client.PegasusTableInterface;
import com.xiaomi.infra.pegasus.client.TableOptions;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class InterceptorTest {
  @Test
  public void testCompressionInterceptor() throws PException {
    PegasusTableInterface commonTable =
        PegasusClientFactory.createClient(ClientOptions.create()).openTable("temp");
    PegasusTableInterface compressTable =
        PegasusClientFactory.createClient(ClientOptions.create())
            .openTable("temp", new TableOptions().withCompression(true));

    byte[] hashKey = "hashKey".getBytes();
    byte[] sortKey = "sortKey".getBytes();
    byte[] commonValue = "commonValue".getBytes();
    byte[] compressionValue = "compressionValue".getBytes();

    // if origin value was not compressed, both commonTable and compressTable can read origin value
    commonTable.set(hashKey, sortKey, commonValue, 10000);
    Assertions.assertEquals(
        new String(commonTable.get(hashKey, sortKey, 10000)), new String(commonValue));
    Assertions.assertEquals(
        new String(compressTable.get(hashKey, sortKey, 10000)), new String(commonValue));

    // if origin value was compressed, only compressTable can read successfully
    compressTable.set(hashKey, sortKey, compressionValue, 10000);
    Assertions.assertNotEquals(
        new String(commonTable.get(hashKey, sortKey, 10000)), new String(compressionValue));
    Assertions.assertEquals(
        new String(compressTable.get(hashKey, sortKey, 10000)), new String(compressionValue));
  }
}
