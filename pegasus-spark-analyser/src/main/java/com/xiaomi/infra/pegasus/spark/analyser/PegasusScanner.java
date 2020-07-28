package com.xiaomi.infra.pegasus.spark.analyser;

public interface PegasusScanner {

  boolean isValid();

  void seekToFirst();

  void next();

  void close();

  PegasusRecord restore();
}
