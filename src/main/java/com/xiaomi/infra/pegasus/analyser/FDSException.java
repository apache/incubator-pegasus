package com.xiaomi.infra.pegasus.analyser;

public class FDSException extends Exception {
  private static final String VERSION_PREFIX = loadVersion() + ": ";

  public FDSException(String message, Throwable cause) {
    super(VERSION_PREFIX + message, cause);
  }

  public FDSException(String message) {
    super(VERSION_PREFIX + message);
  }

  public FDSException(Throwable cause) {
    super(VERSION_PREFIX + cause.toString(), cause);
  }

  private static String loadVersion() {
    String ver = FDSException.class.getPackage().getImplementationVersion();
    if (ver == null) {
      return "{version}";
    }
    return ver;
  }
}
