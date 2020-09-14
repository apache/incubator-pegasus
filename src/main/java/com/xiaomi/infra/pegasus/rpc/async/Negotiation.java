package com.xiaomi.infra.pegasus.rpc.async;

public class Negotiation {
  public Negotiation(ReplicaSession session) {
    this.session = session;
  }

  public void start() {
    // TBD(zlw)
  }

  private ReplicaSession session;
}
