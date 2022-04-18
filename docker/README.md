<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Pegasus Docker

This project maintains the stuff you can use from building Pegasus docker images,
to deploying a standalone cluster of Pegasus containers on your local machine.

## Workflows

![Build and publish multi pegasus-build-env docker images](https://github.com/apache/incubator-pegasus/workflows/BuildCompilationEnvDocker-build%20and%20publish%20multi%20compilation%20os%20env/badge.svg?branch=master)

![Build and publish multi os env thirdparty docker images every week](https://github.com/apache/incubator-pegasus/workflows/BuildThirdpartyDockerRegularly-build%20and%20publish%20thirdparty%20every%20week/badge.svg?branch=master)

![Build pegasus/rdsn regularly based env and thirdparty docker everyday](https://github.com/apache/incubator-pegasus/workflows/BuildPegasusRegularly-build%20pegasus%20and%20rdsn%20on%20different%20env%20every%20day/badge.svg?branch=master)

## pegasus-build-env

Building environment for Pegasus compilation.

Github Actions automatically rebuilds and publishes build-env for every commit.

- `apache/pegasus/build-env:centos7`
- `apache/pegasus/build-env:ubuntu1604`
- `apache/pegasus/build-env:ubuntu1804`
- `apache/pegasus/build-env:ubuntu2004`

DockerHub: https://hub.docker.com/r/apache/pegasus/build-env

The How-to-use Manual is at: http://pegasus.apache.org/docs/build/compile-by-docker/

## thirdparties-src

This image is to eliminate extra downloading of third-party sources of Pegasus.
It packages the downloaded sources into a zip in the container, so that
other repos can easily extract third-parties from the container (via `docker cp`),
without downloading from the cloud object storage.

- `apache/pegasus/thirdparties-src`

## thirdparties-bin

This is a Docker image for Pegasus unit-testing. It prebuilts the thirdparty libraries,
so jobs based on this image can skip building third-parties.

- `apache/pegasus/thirdparties-bin:centos7`
- `apache/pegasus/thirdparties-bin:ubuntu1604`
- `apache/pegasus/thirdparties-bin:ubuntu1804`
- `apache/pegasus/thirdparties-bin:ubuntu2004`
