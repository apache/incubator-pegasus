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

[![BuildCompilationEnvDocker - build and publish multi compilation OS env](https://github.com/apache/incubator-pegasus/actions/workflows/build-push-env-docker.yml/badge.svg)](https://github.com/apache/incubator-pegasus/actions/workflows/build-push-env-docker.yml)

[![BuildThirdpartyDockerRegularly - build and publish thirdparty every week](https://github.com/apache/incubator-pegasus/actions/workflows/thirdparty-regular-push.yml/badge.svg)](https://github.com/apache/incubator-pegasus/actions/workflows/thirdparty-regular-push.yml)

[![Lint and build regularly](https://github.com/apache/incubator-pegasus/actions/workflows/regular-build.yml/badge.svg)](https://github.com/apache/incubator-pegasus/actions/workflows/regular-build.yml)

## pegasus-build-env

Building environment for Pegasus compilation.

Github Actions automatically rebuilds and publishes build-env for every commit.

- `apache/pegasus:build-env-rockylinux9-<branch>`
- `apache/pegasus:build-env-ubuntu1804-<branch>`
- `apache/pegasus:build-env-ubuntu2004-<branch>`
- `apache/pegasus:build-env-ubuntu2204-<branch>`

DockerHub: https://hub.docker.com/r/apache/pegasus

The How-to-use Manual is at: http://pegasus.apache.org/docs/build/compile-by-docker/

## thirdparties-src

This image is to eliminate extra downloading of third-party sources of Pegasus.
It packages the downloaded sources into a zip in the container, so that
other repos can easily extract third-parties from the container (via `docker cp`),
without downloading from the cloud object storage.

- `apache/pegasus:thirdparties-src-rockylinux9-<branch>`
- `apache/pegasus:thirdparties-src-ubuntu1804-<branch>`
- `apache/pegasus:thirdparties-src-ubuntu2004-<branch>`
- `apache/pegasus:thirdparties-src-ubuntu2204-<branch>`

## thirdparties-bin

This is a Docker image for Pegasus unit-testing. It prebuilts the thirdparty libraries,
so jobs based on this image can skip building third-parties.

- `apache/pegasus:thirdparties-bin-rockylinux9-<branch>`
- `apache/pegasus:thirdparties-bin-ubuntu1804-<branch>`
- `apache/pegasus:thirdparties-bin-ubuntu2004-<branch>`
- `apache/pegasus:thirdparties-bin-ubuntu2204-<branch>`
