package com.xiaomi.infra.pegasus.analyser;

import java.io.Serializable;
import java.util.Objects;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class Config implements Serializable {

  public String DATA_URL;
  public String DATA_PORT;
  public String COLDBK_POLICY;
  public Long READ_AHEAD_SIZE;
  public int MAX_FILE_OPEN_COUNTER;

  public Config() throws ConfigurationException {
    this.loadDefaultConfig();
  }

  public Config(String configPath) throws ConfigurationException {
    this.loadConfig(configPath);
  }

  private void loadDefaultConfig() throws ConfigurationException {
    String fileName =
        Objects.requireNonNull(Config.class.getClassLoader().getResource("core-site.xml"))
            .getPath();
    load(fileName);
  }

  private void loadConfig(String configPath) throws ConfigurationException {
    load(configPath);
  }

  private void load(String configPath) throws ConfigurationException {
    XMLConfiguration conf = new XMLConfiguration(configPath);
    DATA_URL = conf.getString("pegasus.url");
    DATA_PORT = conf.getString("pegasus.port", "80");
    READ_AHEAD_SIZE = (int) conf.getDouble("pegasus.readAheadSize", 1) * 1024 * 1024L;
    COLDBK_POLICY = conf.getString("pegasus.coldBackup", "every_day");
    MAX_FILE_OPEN_COUNTER = conf.getInt("pegasus.maxFileCount", 50);
  }

  @Override
  public String toString() {
    String str =
        "  DATA_URL:"
            + DATA_URL.split("@")[1]
            + ":"
            + DATA_PORT
            + "\n"
            + "  COLDBK_POLICY:"
            + COLDBK_POLICY
            + "\n"
            + "  READ_AHEAD_SIZE:"
            + READ_AHEAD_SIZE
            + "MB";

    return str;
  }
}
