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

## Package Pegasus Server binaries

```sh
docker run -w /root/pegasus \
           -v /your/local/incubator-pegasus-2.1.0:/root/pegasus \
           apachepegasus/build-env-2.1.0:ubuntu16.04 \
           /bin/bash -c "./pack_server -g"
```

Please remember that glibc is not forward compatible (programs built against newer GLIBC might be unable to work on older GLIBC). If you're building on a host where GLIBC is newer than the deployment environment GLIBC, it might happen that the compatibility issues cause the server failed to bootstrap.

Check your glibc version:

```
> ldd --version
ldd (Ubuntu GLIBC 2.31-0ubuntu9) 2.31
Copyright (C) 2020 Free Software Foundation, Inc.
This is free software; see the source for copying conditions.  There is NO
warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
Written by Roland McGrath and Ulrich Drepper.
```

So it's recommended to build pegasus against a proper GLIBC when you want to deploy the binary somewhere else.
