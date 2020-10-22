FROM ubuntu:16.04

MAINTAINER WU TAO <wutao1@xiaomi.com>

RUN sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/' /etc/apt/sources.list; \
    apt-get update -y; \
    apt-get -y install build-essential \
                       python3-pip \
                       libaio-dev \
                       libsnappy-dev \
                       libbz2-dev \
                       libzstd-dev \
                       liblz4-dev \
                       zlib1g \
                       zlib1g.dev \
                       patch \
                       git \
                       curl \
                       zip \
                       automake \
                       libtool \
                       libssl-dev \
                       bison \
                       flex; \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir cmake

WORKDIR /root/pegasus
