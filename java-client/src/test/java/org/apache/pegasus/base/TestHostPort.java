package org.apache.pegasus.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class TestHostPort {

  @Test
  public void testHostPort() throws Exception {
    host_port hostPort = new host_port();

    assertNotNull(hostPort);
    assertNull(hostPort.getHost());
    assertEquals(hostPort.getPort(), 0);
    assertEquals(hostPort.getHostPortType(), 0);
  }
}
