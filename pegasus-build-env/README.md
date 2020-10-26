# Building environment for Pegasus compilation

This directory contains the Dockerfiles of the building environment for Pegasus compilation.

## Download docker image

```sh
docker pull apachepegasus/build-env:centos6
```

https://hub.docker.com/r/apachepegasus/build-env

## Compiling Pegasus based on build-env

After the container bootstrapped, it will have all the necessary dependencies installed. We can compile Pegasus by running the container that is mounted upon the local source code directory.

```sh
docker run -v /your/local/apache-pegasus-source:/root/pegasus \
           apachepegasus/build-env:centos6 \
           /bin/bash -c "./run.sh build"
```

When the compilation is completed, the binaries will be directly generated under the source directory. They won't get disappeared as the container exits.

## Packaging Pegasus Server binaries (Optional)

```sh
docker run -v /your/local/apache-pegasus-source:/root/pegasus \
           apachepegasus/build-env:centos6 \
           /bin/bash -c "./scripts/pack_server.sh"
```

## Packaging Pegasus toolset (Optional)

```sh
docker run -v /your/local/apache-pegasus-source:/root/pegasus \
           apachepegasus/build-env:centos6 \
           /bin/bash -c "./scripts/pack_tools.sh"
```
