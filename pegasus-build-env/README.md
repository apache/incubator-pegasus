# Building environment for Pegasus compilation

This directory contains the Dockerfiles of the building environment for Pegasus compilation.

After the container bootstrapped, it will have all the necessary dependencies installed. We can compile Pegasus by running the container that is mounted upon the local source code directory.

When the compilation completed, the binaries will be directly generated under the source directory. It won't get disappeared as the container exits.

## Download docker image

```sh
docker pull apachepegasus/build-env
```

https://hub.docker.com/r/apachepegasus/build-env

## Building Pegasus based on build-env

```sh
docker run -w /root/pegasus \
           -v /your/local/apache-pegasus-source:/root/pegasus \
           apachepegasus/build-env:ubuntu16.04 \
           /bin/bash -c "./run.sh build"
```

## Packaging Pegasus Server binaries

```sh
docker run -w /root/pegasus \
           -v /your/local/apache-pegasus-source:/root/pegasus \
           apachepegasus/build-env \
           /bin/bash -c "./scripts/pack_server.sh"
```
