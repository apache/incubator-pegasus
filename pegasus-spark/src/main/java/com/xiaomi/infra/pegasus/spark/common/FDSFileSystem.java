/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.xiaomi.infra.pegasus.spark.common;

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
