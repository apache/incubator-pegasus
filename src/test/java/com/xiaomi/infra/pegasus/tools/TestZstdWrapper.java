package com.xiaomi.infra.pegasus.tools;


import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.client.PegasusClientFactory;
import com.xiaomi.infra.pegasus.client.PegasusClientInterface;
import com.xiaomi.infra.pegasus.client.PegasusTableInterface;
import org.junit.Assert;
import org.junit.Test;

public class TestZstdWrapper {
    @Test
    public void testCompression() throws Exception {
        PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
        PegasusTableInterface table = client.openTable("temp");

        for (int t = 0; t < 4; t++) {
            // generate a 10KB value
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < 10000; i++) {
                builder.append('a' + t);
            }
            byte[] value = builder.toString().getBytes();

            // write the record into pegasus
            table.set("h".getBytes(), "s".getBytes(), ZstdWrapper.compress(value), 1000);

            // read the record from pegasus
            byte[] compressedBuf = table.get("h".getBytes(), "s".getBytes(), 1000);

            // decompress the value
            Assert.assertArrayEquals(ZstdWrapper.decompress(compressedBuf), value);
        }

        // ensure empty value won't break the program
        {
            try {
                ZstdWrapper.decompress("".getBytes());
                Assert.fail("expecting a IllegalArgumentException");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof IllegalArgumentException);
            }
            try {
                ZstdWrapper.decompress(null);
                Assert.fail("expecting a IllegalArgumentException");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof IllegalArgumentException);
            }
        }

        { // decompress invalid data
            try {
                ZstdWrapper.decompress("abc123".getBytes());
                Assert.fail("expecting a PException");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof PException);
            }
        }
    }
}
