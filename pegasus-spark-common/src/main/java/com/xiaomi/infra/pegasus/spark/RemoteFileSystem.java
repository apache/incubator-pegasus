package com.xiaomi.infra.pegasus.spark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Serializable;
import org.apache.hadoop.fs.FileStatus;

public interface RemoteFileSystem extends Serializable {

  BufferedReader getReader(String filePath) throws PegasusSparkException;

  BufferedWriter getWriter(String filePath) throws PegasusSparkException;

  FileStatus[] getFileStatus(String path) throws PegasusSparkException;

  boolean exist(String path) throws PegasusSparkException;

  boolean delete(String path, boolean recursive) throws PegasusSparkException;

  String getFileMD5(String filePath) throws PegasusSparkException;
}
