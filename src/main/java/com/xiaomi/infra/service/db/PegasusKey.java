package com.xiaomi.infra.service.db;

public class PegasusKey {

  private static final String STRING_NULL = "";

  public byte[] hashKey;
  public byte[] sortKey;

  public PegasusKey() {
    hashKey = STRING_NULL.getBytes();
    sortKey = STRING_NULL.getBytes();
  }

  public PegasusKey(byte[] hashKey, byte[] sortKey) {
    this.hashKey = hashKey;
    this.sortKey = sortKey;
  }
}
