package com.xiaomi.infra.pegasus.rpc;

import com.xiaomi.infra.pegasus.client.TableOptions;

public class InternalTableOptions {
  private final KeyHasher keyHasher;
  private final TableOptions tableOptions;

  public InternalTableOptions(KeyHasher keyHasher, TableOptions tableOptions) {
    this.keyHasher = keyHasher;
    this.tableOptions = tableOptions;
  }

  public KeyHasher keyHasher() {
    return keyHasher;
  }

  public TableOptions tableOptions() {
    return tableOptions;
  }

  public static InternalTableOptions forTest() {
    return new InternalTableOptions(KeyHasher.DEFAULT, new TableOptions());
  }
}
