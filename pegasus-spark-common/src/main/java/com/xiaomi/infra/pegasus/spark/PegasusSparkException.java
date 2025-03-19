package com.xiaomi.infra.pegasus.spark;

public class PegasusSparkException extends Exception {
  private static final String VERSION_PREFIX = loadVersion() + ": ";

  public PegasusSparkException(String message, Throwable cause) {
    super(VERSION_PREFIX + message, cause);
  }

  public PegasusSparkException(String message) {
    super(VERSION_PREFIX + message);
  }

  public PegasusSparkException(Throwable cause) {
    super(VERSION_PREFIX + cause.toString(), cause);
  }

  private static String loadVersion() {
    String ver = PegasusSparkException.class.getPackage().getImplementationVersion();
    if (ver == null) {
      return "{version}";
    }
    return ver;
  }
}
