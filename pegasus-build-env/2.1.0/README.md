# Building environment for Pegasus compilation

This directory contains the Dockerfiles of the building environment for Pegasus compilation.

After the container bootstrapped, it will have all the necessary dependencies installed. We can compile Pegasus by running the container that is mounted upon the local source code directory.

When the compilation completed, the binaries will be directly generated under the source directory. It won't get disappeared as the container exits.

## Building Pegasus based on build-env

```sh
docker run -w /root/pegasus \
           -v /your/local/incubator-pegasus-2.1.0:/root/pegasus \
           apachepegasus/build-env-2.1.0:ubuntu16.04 \
           /bin/bash -c "./run.sh build"
```

