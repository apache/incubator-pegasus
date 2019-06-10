FROM ubuntu:18.04

# This is a docker image for any usage once you have a pre-built
# binary of Pegasus.
#
# Usage:
#
# ./run.sh build -c
# ./run.sh pack_server
# mv pegasus-server-{YOUR_VERSION}.tar.gz docker/dev/linux/
# cd docker/dev/linux/
# docker build --build-arg SERVER_PKG_NAME=pegasus-server-{YOUR_VERSION} -t pegasus:latest .
#
# Or simply run docker/build_docker.sh to build image named pegasus:latest.
#

ARG SERVER_PKG_NAME

COPY ./$SERVER_PKG_NAME.tar.gz /
RUN tar xvf /$SERVER_PKG_NAME.tar.gz; \
    mv $SERVER_PKG_NAME pegasus; \
    rm $SERVER_PKG_NAME.tar.gz

COPY ./entrypoint.sh /
RUN chmod +x /entrypoint.sh

ENV LD_LIBRARY_PATH=/pegasus/bin

ENTRYPOINT ["/entrypoint.sh"]
