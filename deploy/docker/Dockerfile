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
	&& rm -rf /var/lib/apt/lists/*

RUN useradd -d /home/rdsn -s /bin/bash rdsn
RUN mkdir /home/rdsn 
COPY bash_profile /home/rdsn/.bash_profile
COPY bashrc /home/rdsn/.bashrc
RUN chown -R rdsn:rdsn /home/rdsn
COPY rdsn_build.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/rdsn_build.sh

USER rdsn
WORKDIR /home/rdsn
ENV HOME /home/rdsn
RUN rdsn_build.sh

