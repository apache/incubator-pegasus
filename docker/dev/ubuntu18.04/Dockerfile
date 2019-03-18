# Support ssh to be testable by Jepsen.
FROM rastasheep/ubuntu-sshd:18.04

# Usage:
#
# ./run.sh build -c
# ./run.sh pack_server
# mv pegasus-server-1.12.SNAPSHOT-bcea9bd-ubuntu-release.tar.gz docker/dev/ubuntu18.04/
# cd docker/dev/ubuntu18.04/
# docker build --build-arg SERVER_PKG_NAME=pegasus-server-1.12.SNAPSHOT-bcea9bd-ubuntu-release .

ARG SERVER_PKG_NAME

COPY ./$SERVER_PKG_NAME.tar.gz /

RUN sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/' /etc/apt/sources.list; \
    apt update -y; \
    apt install libicu60; \
    tar xvf /$SERVER_PKG_NAME.tar.gz; \
    mv $SERVER_PKG_NAME pegasus; \
    rm $SERVER_PKG_NAME.tar.gz; \
    rm -rf /var/lib/apt/lists/*

ENV LD_LIBRARY_PATH=/pegasus/bin

ENTRYPOINT ["/pegasus/bin/pegasus_server", "/pegasus/bin/config.ini", "-app_list"]
