# Pegasus Docker

This project maintains the stuff you can use from building Pegasus docker images,
to deploying a standalone cluster of Pegasus containers on your local machine.

![Build and publish pegasus-build-env Docker images](https://github.com/pegasus-kv/pegasus-docker/workflows/Build%20and%20publish%20pegasus-build-env%20Docker%20images/badge.svg?branch=master)

![Build and upload third-party source package to GHCR](https://github.com/pegasus-kv/pegasus-docker/workflows/Build%20and%20upload%20third-party%20source%20package%20to%20GHCR/badge.svg)

![Build and upload third-party binary package to GHCR](https://github.com/pegasus-kv/pegasus-docker/workflows/Build%20and%20upload%20third-party%20binary%20package%20to%20GHCR/badge.svg?branch=master)

![Build rdsn regularly](https://github.com/pegasus-kv/pegasus-docker/workflows/Build%20rdsn%20regularly/badge.svg?branch=master)

## pegasus-build-env

Building environment for Pegasus compilation.

Github Actions automatically rebuilds and publishes build-env for every commit.

- `apachepegasus/build-env:centos7`
- `apachepegasus/build-env:ubuntu1604`
- `apachepegasus/build-env:ubuntu1804`
- `apachepegasus/build-env:ubuntu2004`
- `ghcr.io/pegasus-kv/build-env:centos7`
- `ghcr.io/pegasus-kv/build-env:ubuntu1604`
- `ghcr.io/pegasus-kv/build-env:ubuntu1804`
- `ghcr.io/pegasus-kv/build-env:ubuntu2004`

DockerHub: https://hub.docker.com/r/apachepegasus/build-env

The How-to-use Manual is at: http://pegasus.apache.org/docs/build/compile-by-docker/

## thirdparties-src

This image is to eliminate extra downloading of third-party sources of Pegasus.
It packages the downloaded sources into a zip in the container, so that
other repos can easily extract third-parties from the container (via `docker cp`),
without downloading from the cloud object storage.

Since it mostly benefits the building in Github Actions, we uploaded it automatically
to the Github Container Registry:

- `ghcr.io/pegasus-kv/thirdparties-src`

## thirdparties-bin

This is a Docker image for Pegasus unit-testing. It prebuilts the thirdparty libraries,
so jobs based on this image can skip building third-parties.

- `ghcr.io/pegasus-kv/thirdparties-bin:centos7`
- `ghcr.io/pegasus-kv/thirdparties-bin:ubuntu1604`
- `ghcr.io/pegasus-kv/thirdparties-bin:ubuntu1804`
- `ghcr.io/pegasus-kv/thirdparties-bin:ubuntu2004`
