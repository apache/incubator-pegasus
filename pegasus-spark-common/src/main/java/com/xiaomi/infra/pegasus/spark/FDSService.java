package com.xiaomi.infra.pegasus.spark;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FDSService implements Serializable {

  private static final Log LOG = LogFactory.getLog(FDSService.class);

  public BufferedReader getReader(String filePath) throws FDSException {
    try {
      InputStream inputStream =
          FileSystem.get(new URI(filePath), new Configuration()).open(new Path(filePath));
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
      return bufferedReader;
    } catch (Exception e) {
      LOG.error("get filePath reader failed from " + filePath, e);
      throw new FDSException("get filePath reader failed, [url: " + filePath + "]" + e);
    }
  }

  public BufferedWriter getWriter(String filePath) throws URISyntaxException, FDSException {
    try {
      OutputStreamWriter outputStreamWriter =
          new OutputStreamWriter(
              FileSystem.get(new URI(filePath), new Configuration()).create(new Path(filePath)));
      BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);
      return bufferedWriter;
    } catch (Exception e) {
      LOG.error("get filePath writer failed from " + filePath, e);
      throw new FDSException("get filePath writer failed, [url: " + filePath + "]" + e);
    }
  }

  public FileStatus[] getFileStatus(String path) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(path), new Configuration());
    Path Path = new Path(path);
    return fs.listStatus(Path);
  }

  public String getMD5(String filePath) throws IOException, URISyntaxException {
    return DigestUtils.md5Hex(
        FileSystem.get(new URI(filePath), new Configuration()).open(new Path(filePath)));
  }
}
