# Pegasus Docker

This project maintains the stuff you can use from building Pegasus docker images,
to deploying a standalone cluster of Pegasus containers on your local machine.

## Workflows

![Build and publish multi pegasus-build-env docker images](https://github.com/pegasus-kv/pegasus-docker/workflows/BuildCompilationEnvDocker-build%20and%20publish%20multi%20compilation%20os%20env/badge.svg?branch=master)

![Build and publish multi os env thirdparty docker images every week](https://github.com/pegasus-kv/pegasus-docker/workflows/BuildThirdpartyDockerRegularly-build%20and%20publish%20thirdparty%20every%20week/badge.svg?branch=master)

![Build pegasus/rdsn regularly based env and thirdparty docker  everyday](https://github.com/pegasus-kv/pegasus-docker/workflows/BuildPegasusRegularly-build%20pegasus%20and%20rdsn%20on%20different%20env%20every%20day/badge.svg?branch=master)

## pegasus-build-env

Building environment for Pegasus compilation.

Github Actions automatically rebuilds and publishes build-env for every commit.

- `apachepegasus/build-env:centos7`
- `apachepegasus/build-env:ubuntu1604`
- `apachepegasus/build-env:ubuntu1804`
- `apachepegasus/build-env:ubuntu2004`

DockerHub: https://hub.docker.com/r/apachepegasus/build-env

The How-to-use Manual is at: http://pegasus.apache.org/docs/build/compile-by-docker/

## thirdparties-src

This image is to eliminate extra downloading of third-party sources of Pegasus.
It packages the downloaded sources into a zip in the container, so that
other repos can easily extract third-parties from the container (via `docker cp`),
without downloading from the cloud object storage.

- `apachepegasus/thirdparties-src`

## thirdparties-bin

This is a Docker image for Pegasus unit-testing. It prebuilts the thirdparty libraries,
so jobs based on this image can skip building third-parties.

- `apachepegasus/thirdparties-bin:centos7`
- `apachepegasus/thirdparties-bin:ubuntu1604`
- `apachepegasus/thirdparties-bin:ubuntu1804`
- `apachepegasus/thirdparties-bin:ubuntu2004`
