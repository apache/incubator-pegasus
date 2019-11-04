package com.xiaomi.infra.pegasus.analyser.sample.local;

import com.xiaomi.infra.pegasus.analyser.PegasusClient;
import com.xiaomi.infra.pegasus.analyser.Config;
import com.xiaomi.infra.pegasus.analyser.FdsService;
import com.xiaomi.infra.pegasus.analyser.PegasusKey;
import com.xiaomi.infra.pegasus.analyser.PegasusOptions;
import com.xiaomi.infra.pegasus.analyser.PegasusScanner;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class ScanTool {

  static {
    RocksDB.loadLibrary();
  }

  private static final Log LOG = LogFactory.getLog(ScanTool.class);
  private static volatile Boolean isComplete = false;

  private static void count(FdsService fdsService, Config config) {
    LOG.info("Workers will count the total data.");
    AtomicInteger totalDataCount = new AtomicInteger();
    AtomicInteger taskCounter = new AtomicInteger();
    PegasusOptions pegasusOptions = new PegasusOptions(config);
    PegasusClient pegasusClient = new PegasusClient(pegasusOptions, fdsService);
    int partitionCount = pegasusClient.getPartitionCount();
    ExecutorService counterTaskExecutor = Executors.newFixedThreadPool(partitionCount);
    int pid = partitionCount - 1;
    while (pid >= 0) {
      int finalPid = pid;
      counterTaskExecutor.submit(
          () -> {
            try {
              int counter = pegasusClient.getDataCount(finalPid);
              totalDataCount.addAndGet(counter);
              LOG.info(
                  "Worker ["
                      + Thread.currentThread().getName()
                      + "] update the total count is:"
                      + totalDataCount.get());
            } catch (RocksDBException e) {
              e.printStackTrace();
            }
            isComplete = taskCounter.incrementAndGet() == partitionCount;
          });
      pid--;
    }

    while (!isComplete) {}
    pegasusOptions.close();
  }

  private static void scan(FdsService fdsService, Config config) {
    AtomicInteger taskCounter = new AtomicInteger();
    PegasusOptions pegasusOptions = new PegasusOptions(config);
    PegasusClient pegasusClient = new PegasusClient(pegasusOptions, fdsService);
    int partitionCount = pegasusClient.getPartitionCount();
    ExecutorService counterTaskExecutor = Executors.newFixedThreadPool(partitionCount);
    int pid = partitionCount - 1;
    while (pid >= 0) {
      int finalPid = pid;
      counterTaskExecutor.submit(
          () -> {
            try {
              PegasusScanner pegasusScanner = pegasusClient.getScanner(finalPid);
              for (pegasusScanner.seekToFirst(); pegasusScanner.isValid(); pegasusScanner.next()) {
                PegasusKey pegasusKey = pegasusScanner.key();
                String hashKey = new String(pegasusKey.hashKey);
                String sortKey = new String(pegasusKey.sortKey);
                String value = new String(pegasusScanner.value());
                System.out.println("[" + finalPid + "]" + hashKey + ":" + sortKey + "=>" + value);
              }
              pegasusScanner.close();
            } catch (RocksDBException e) {
              e.printStackTrace();
            }
            isComplete = taskCounter.incrementAndGet() == partitionCount;
          });
      pid--;
    }

    while (!isComplete) {}
    pegasusClient.close();
  }

  public static void main(String[] arg)
      throws ParseException, IOException, java.text.ParseException, ConfigurationException {

    String args[] = {"-s", "-c", "c4tst-perfomance", "-t", "coldbackuptest"};
    Config serviceConfig;
    FdsService fdsService;

    String cluster = "";
    String table = "";
    String config = "";
    String date = "";

    Options options = new Options();

    options.addOption("h", "help", false, "help info");
    options.addOption("a", "aggregation", false, "count the total data(aggregation)");
    options.addOption("s", "scan", false, "count the total data(aggregation)");
    options.addOption("o", "out", true, "scan data output path");
    options.addOption("c", "cluster", true, "cluster name");
    options.addOption("t", "table", true, "table name");
    options.addOption("d", "date", true, "data time");
    options.addOption("p", "path", true, "config path");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("h")) {
      System.out.println(
          "  -c --cluster cluster name\n"
              + "  -t --table table name\n"
              + "  -p --path config path");
    } else {
      if (cmd.hasOption("p")) {
        config = cmd.getOptionValue("p");
        serviceConfig = new Config(config);
        LOG.info("Config:\n" + serviceConfig.toString());
      } else {
        serviceConfig = new Config();
        LOG.warn("WARNING:You are using the default config!!!!!!!!!!\n" + serviceConfig.toString());
      }

      if (cmd.hasOption("c") && cmd.hasOption("t")) {
        cluster = cmd.getOptionValue("c");
        table = cmd.getOptionValue("t");
        if (cmd.hasOption("d")) {
          date = cmd.getOptionValue("d");
          fdsService = new FdsService(serviceConfig, cluster, table, date);
        } else {
          fdsService = new FdsService(serviceConfig, cluster, table);
        }
        if (cmd.hasOption("a")) {
          count(fdsService, serviceConfig);
        } else if (cmd.hasOption("s")) {
          scan(fdsService, serviceConfig);
        } else {
          throw new MissingArgumentException("Missing required operation(-a or -s)");
        }
      } else {
        throw new MissingArgumentException("Missing required options: -c or -t");
      }
    }

    System.exit(0);
  }
}
