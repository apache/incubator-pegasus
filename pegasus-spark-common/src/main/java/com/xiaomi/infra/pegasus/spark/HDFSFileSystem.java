package com.xiaomi.infra.pegasus.spark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSFileSystem implements RemoteFileSystem {

  HDFSFileSystem() {}

  private static final Log LOG = LogFactory.getLog(RemoteFileSystem.class);

  public BufferedReader getReader(String filePath) throws PegasusSparkException {
    try {
      InputStream inputStream =
          org.apache.hadoop.fs.FileSystem.get(new URI(filePath), new Configuration())
              .open(new Path(filePath));
      return new BufferedReader(new InputStreamReader(inputStream));
    } catch (Exception e) {
      throw new PegasusSparkException("get filePath reader failed, [url: " + filePath + "]", e);
    }
  }

  public BufferedWriter getWriter(String filePath) throws PegasusSparkException {
    try {
      OutputStreamWriter outputStreamWriter =
          new OutputStreamWriter(
              org.apache.hadoop.fs.FileSystem.get(new URI(filePath), new Configuration())
                  .create(new Path(filePath)));
      return new BufferedWriter(outputStreamWriter);
    } catch (Exception e) {
      throw new PegasusSparkException("get filePath writer failed, [url: " + filePath + "]", e);
    }
  }

  public FileStatus[] getFileStatus(String path) throws PegasusSparkException {
    try {
      FileSystem fs = FileSystem.get(URI.create(path), new Configuration());
      Path Path = new Path(path);
      return fs.listStatus(Path);
    } catch (IOException e) {
      throw new PegasusSparkException("get file status failed:", e);
    }
  }

  @Override
  public String getFileMD5(String filePath) throws PegasusSparkException {
    try {
      return DigestUtils.md5Hex(
          FileSystem.get(new URI(filePath), new Configuration()).open(new Path(filePath)));
    } catch (IOException | URISyntaxException e) {
      throw new PegasusSparkException("get md5 from hdfs failed:", e);
    }
  }
}
