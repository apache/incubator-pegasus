FROM ubuntu:14.04

RUN apt-get update && apt-get install -y --no-install-recommends \
		build-essential \
		cmake \
		git \
		php5-cli \
		libaio-dev \
		libboost-all-dev \
		ca-certificates \
        grep \
        python2.7 \
        python-pip \
        gdb        \
        p7zip-full \
	&& rm -rf /var/lib/apt/lists/*
RUN useradd -d /home/rdsn -s /bin/bash rdsn \
    && mkdir /home/rdsn 
COPY script/bash_profile /home/rdsn/.bash_profile
COPY script/bashrc /home/rdsn/.bashrc
RUN chown -R rdsn:rdsn /home/rdsn

ADD rdsn-release.tar.gz /home/rdsn/
ADD MonitorPack.tar.gz /home/rdsn/
WORKDIR /home/rdsn
RUN python setup.py install \
    && pip install -r apps/rDSN.monitor/requirement.txt

ENV HOME /home/rdsn
