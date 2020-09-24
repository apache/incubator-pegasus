package com.xiaomi.infra.pegasus.rpc.async;

import static com.xiaomi.infra.pegasus.apps.negotiation_status.SASL_LIST_MECHANISMS;
import static org.mockito.ArgumentMatchers.any;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class NegotiationTest {
  @Test
  public void testStart() {
    Negotiation negotiation = new Negotiation(null, null, "", "");
    Negotiation mockNegotiation = Mockito.spy(negotiation);

    Mockito.doNothing().when(mockNegotiation).send(any(), any());
    mockNegotiation.start();
    Assert.assertEquals(mockNegotiation.get_status(), SASL_LIST_MECHANISMS);
  }
}
