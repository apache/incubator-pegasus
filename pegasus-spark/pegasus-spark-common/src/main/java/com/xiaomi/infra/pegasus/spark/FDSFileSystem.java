package com.xiaomi.infra.pegasus.spark;

import com.xiaomi.infra.galaxy.fds.client.FDSClientConfiguration;
import com.xiaomi.infra.galaxy.fds.client.GalaxyFDS;
import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.galaxy.fds.client.credential.BasicFDSCredential;
import com.xiaomi.infra.galaxy.fds.client.exception.GalaxyFDSClientException;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObject;
import com.xiaomi.infra.galaxy.fds.model.FDSObjectMetadata;

public class FDSFileSystem extends HDFSFileSystem {

  private FDSConfig fdsConfig;

  FDSFileSystem(FDSConfig fdsConfig) {
    this.fdsConfig = fdsConfig;
  }

  @Override
  public String getFileMD5(String filePath) throws PegasusSparkException {
    FDSClientConfiguration fdsClientConfiguration = new FDSClientConfiguration(fdsConfig.endPoint);
    fdsClientConfiguration.enableCdnForDownload(false);
    fdsClientConfiguration.enableCdnForUpload(false);

    GalaxyFDS fdsClient =
        new GalaxyFDSClient(
            new BasicFDSCredential(fdsConfig.accessKey, fdsConfig.accessSecret),
            fdsClientConfiguration);

    try {
      FDSObject fdsObject =
          fdsClient.getObject(fdsConfig.bucketName, filePath.split(fdsConfig.endPoint + "/")[1]);
      FDSObjectMetadata metaData = fdsObject.getObjectMetadata();
      return metaData.getContentMD5();
    } catch (GalaxyFDSClientException e) {
      throw new PegasusSparkException("get md5 from fds failed:", e);
    }
  }
}
