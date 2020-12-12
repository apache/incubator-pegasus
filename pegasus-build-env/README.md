# Building environment for Pegasus compilation

This directory contains the Dockerfiles of the building environment for Pegasus compilation.

## Building the images

Dockerhub automatically watches the changes of theses files and rebuild to images for every commit.

- apachepegasus/build-env:centos6
- apachepegasus/build-env:centos7
- apachepegasus/build-env:ubuntu1604

The DockerHub Homepage: https://hub.docker.com/r/apachepegasus/build-env

You can build on your local machine via the command:

```sh
docker build -t apachepegasus/build-env:centos6 .
```

The How-to Manual is at: http://pegasus.apache.org/docs/build/compile-by-docker/
