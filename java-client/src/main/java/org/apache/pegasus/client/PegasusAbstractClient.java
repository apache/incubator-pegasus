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

package org.apache.pegasus.client;

import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.pegasus.rpc.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PegasusAbstractClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(PegasusAbstractClient.class);
  protected final ClientOptions clientOptions;
  protected Cluster cluster;

  public PegasusAbstractClient(Properties properties) throws PException {
    this(ClientOptions.create(properties));
  }

  public PegasusAbstractClient(String configPath) throws PException {
    this(ClientOptions.create(configPath));
  }

  protected PegasusAbstractClient(ClientOptions options) throws PException {
    this.clientOptions = options;
    this.cluster = Cluster.createCluster(clientOptions);
    LOGGER.info(
        "Create Pegasus{}Client Instance By ClientOptions : {}",
        this.clientType(),
        this.clientOptions.toString());
  }

  protected String clientType() {
    return "";
  }

  @Override
  protected void finalize() {
    close();
  }

  public void close() {
    synchronized (this) {
      if (null != this.cluster) {
        String metaList = StringUtils.join(cluster.getMetaList(), ",");
        LOGGER.info("start to close pegasus {} client for [{}]", clientType(), metaList);
        cluster.close();
        cluster = null;
        LOGGER.info("finish to close pegasus {} client for [{}]", clientType(), metaList);
      }
    }
  }
}
