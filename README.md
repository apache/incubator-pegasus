# Pegasus Docker

This project maintains the stuff you can use from building Pegasus docker images,
to deploying a standalone cluster of Pegasus containers on your local machine.

## Guide

It consists of the following components:

- pegasus-docker-compose

- pegasus-build-env

- ci-env

## pegasus-build-env

Building environment for Pegasus compilation.

It contains the Dockerfiles of the building environment for Pegasus compilation.

### Building the images

DockerHub automatically watches the changes of theses files and rebuilds them to images for every commit.

- `apachepegasus/build-env:centos6`
- `apachepegasus/build-env:centos7`
- `apachepegasus/build-env:ubuntu1604`
- `apachepegasus/build-env:ubuntu1804`
- `apachepegasus/build-env:ubuntu2004`
- `apachepegasus/build-env:ubuntu2004clang11`

The DockerHub Homepage: https://hub.docker.com/r/apachepegasus/build-env

You can build on your local machine via the command:

```sh
cd pegasus-build-env/centos6
docker build -t apachepegasus/build-env:centos6 .
```

The How-to Manual is at: http://pegasus.apache.org/docs/build/compile-by-docker/

## ci-env

This is a Docker image for running Pegasus testing.
