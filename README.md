# Pegasus Docker

This project maintains the stuff you can use from building Pegasus docker images,
to deploying a standalone cluster of Pegasus containers on your local machine.

- ![Build and publish pegasus-build-env Docker images](https://github.com/pegasus-kv/pegasus-docker/workflows/Build%20and%20publish%20pegasus-build-env%20Docker%20images/badge.svg?branch=master)

- ![Build and upload third-party source package to GHCR](https://github.com/pegasus-kv/pegasus-docker/workflows/Build%20and%20upload%20third-party%20source%20package%20to%20GHCR/badge.svg)

- ![Build and upload third-party binary package to GHCR](https://github.com/pegasus-kv/pegasus-docker/workflows/Build%20and%20upload%20third-party%20binary%20package%20to%20GHCR/badge.svg?branch=master)

## pegasus-build-env

Building environment for Pegasus compilation.

It contains the Dockerfiles of the building environment for Pegasus compilation.

### Building the images

DockerHub automatically watches the changes of theses files and rebuilds them to images for every commit.

- `apachepegasus/build-env:centos7`
- `apachepegasus/build-env:ubuntu1604`
- `apachepegasus/build-env:ubuntu1804`
- `apachepegasus/build-env:ubuntu2004`

The DockerHub Homepage: https://hub.docker.com/r/apachepegasus/build-env

You can build on your local machine via the command:

```sh
cd pegasus-build-env/centos7
docker build -t apachepegasus/build-env:centos7 .
```

The How-to Manual is at: http://pegasus.apache.org/docs/build/compile-by-docker/

## thirdparties-src

This image is to eliminate extra downloading of third-party sources of Pegasus.
It packages the downloaded sources into a zip in the container, so that
other repos can easily extract third-parties from the container (via `docker cp`),
without downloading from the cloud object storage.

Since it mostly benefits the building in Github Actions, we uploaded it automatically
to the Github Container Registry: `ghcr.io/pegasus-kv/thirdparties-src`,
which is now only accessible from Github Actions runners, not public to everyone.
See also: https://docs.github.com/en/free-pro-team@latest/packages/guides/configuring-docker-for-use-with-github-packages

## thirdparties-bin

This is a Docker image for Pegasus unit-testing. It prebuilts the thirdparty libraries,
so jobs based on this image can skip building third-parties.

- `ghcr.io/pegasus-kv/thirdparties-bin:centos7`
- `ghcr.io/pegasus-kv/thirdparties-bin:ubuntu1604`
- `ghcr.io/pegasus-kv/thirdparties-bin:ubuntu1804`
- `ghcr.io/pegasus-kv/thirdparties-bin:ubuntu2004`

Only available by Github Actions runners.
