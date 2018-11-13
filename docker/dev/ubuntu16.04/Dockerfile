FROM ubuntu:16.04

MAINTAINER WU TAO <wutao1@xiaomi.com>

RUN sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/' /etc/apt/sources.list; \
    apt-get update -y; \
    apt-get -y install build-essential cmake libboost-system-dev libboost-filesystem-dev libboost-regex-dev \
                       libaio-dev libsnappy-dev libbz2-dev libtool \
                       zlib1g zlib1g.dev patch git curl zip automake libssl-dev;

RUN git clone --recursive https://github.com/XiaoMi/pegasus.git /pegasus; \
    cd /pegasus; \
    ./run.sh build; \
    ./run.sh pack_tools; \
    cp -r pegasus-tools-* /; cd /; rm -rf /pegasus \
    rm pegasus-tools-*.tar.gz;

RUN apt-get -y install default-jre nmap netcat
