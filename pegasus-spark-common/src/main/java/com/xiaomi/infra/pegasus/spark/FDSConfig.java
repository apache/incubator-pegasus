package com.xiaomi.infra.pegasus.spark;

public class FDSConfig extends HDFSConfig {

  String remoteFsBucketName;
  String remoteFsEndPoint;
  String remoteFsAccessKey;
  String remoteFsAccessSecret;

  public FDSConfig(
      String accessKey, String accessSecret, String bucketName, String endPoint, String port) {
    super("fds://" + accessKey + ":" + accessSecret + "@" + bucketName + "." + endPoint, port);
    this.remoteFsAccessKey = accessKey;
    this.remoteFsAccessSecret = accessSecret;
    this.remoteFsBucketName = bucketName;
    this.remoteFsEndPoint = endPoint;
  }

  public void setRemoteFsBucketName(String remoteFsBucketName) {
    this.remoteFsBucketName = remoteFsBucketName;
  }

  public void setRemoteFsEndPoint(String remoteFsEndPoint) {
    this.remoteFsEndPoint = remoteFsEndPoint;
  }

  public void setRemoteFsAccessKey(String remoteFsAccessKey) {
    this.remoteFsAccessKey = remoteFsAccessKey;
  }

  public void setRemoteFsAccessSecret(String remoteFsAccessSecret) {
    this.remoteFsAccessSecret = remoteFsAccessSecret;
  }

  public String getRemoteFsBucketName() {
    return remoteFsBucketName;
  }

  public String getRemoteFsEndPoint() {
    return remoteFsEndPoint;
  }

  public String getRemoteFsAccessKey() {
    return remoteFsAccessKey;
  }

  public String getRemoteFsAccessSecret() {
    return remoteFsAccessSecret;
  }
}
