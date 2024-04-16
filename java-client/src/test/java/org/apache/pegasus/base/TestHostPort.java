package org.apache.pegasus.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.jupiter.api.Test;

public class TestHostPort {

  @Test
  public void testHostPortReadAndWrite() throws Exception {
    host_port hostPort = new host_port();

    assertNotNull(hostPort);
    assertNull(hostPort.getHost());
    assertEquals(hostPort.getPort(), 0);
    assertEquals(hostPort.getHostPortType(), 0);

    hostPort.setHost("www.baidu.com");
    hostPort.setPort(65535);
    hostPort.setHostPortType((byte) 1);

    TMemoryBuffer buffer = new TMemoryBuffer(500);
    TBinaryProtocol protocol = new TBinaryProtocol(buffer);
    hostPort.write(protocol);

    host_port decodeHostPort = new host_port();
    decodeHostPort.read(protocol);

    assertEquals(hostPort.getHost(), decodeHostPort.getHost());
    System.out.println(hostPort.getPort());
    System.out.println(decodeHostPort.getPort());
    assertEquals(hostPort.getPort(), decodeHostPort.getPort());
    assertEquals(hostPort.getHostPortType(), decodeHostPort.getHostPortType());
  }
}
