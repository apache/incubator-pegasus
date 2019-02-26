FROM ubuntu:18.04

COPY @SERVER_PKG_NAME@.tar.gz /

RUN sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/' /etc/apt/sources.list; \
    apt update -y; \
    apt install libicu60; \
    tar xvf /@SERVER_PKG_NAME@.tar.gz; \
    mv @SERVER_PKG_NAME@ pegasus; \
    rm @SERVER_PKG_NAME@.tar.gz; \
    rm -rf /var/lib/apt/lists/*

ENV LD_LIBRARY_PATH=/pegasus/bin

ENTRYPOINT ["/pegasus/bin/pegasus_server", "/pegasus/bin/config.ini", "-app_list"]
