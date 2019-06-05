FROM jacobmbr/ubuntu-jepsen:v0.1.0

# Usage:
#
# ./run.sh build -c
# ./run.sh pack_server
# mv pegasus-server-{YOUR_VERSION}.tar.gz docker/dev/jepsen/
# cd docker/dev/jepsen/
# docker build --build-arg SERVER_PKG_NAME=pegasus-server-{YOUR_VERSION} -t pegasus:latest .

# Install Jepsen dependencies
RUN sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/' /etc/apt/sources.list; \
    rm /etc/apt/apt.conf.d/docker-clean && apt-get update -y
RUN apt-get install -y openssh-server \
    curl faketime iproute2 iptables iputils-ping libzip4 \
    logrotate man man-db net-tools ntpdate psmisc python rsyslog \
    sudo unzip vim wget apt-transport-https \
    && apt-get remove -y --purge --auto-remove systemd

ARG SERVER_PKG_NAME

COPY ./$SERVER_PKG_NAME.tar.gz /
RUN tar xvf /$SERVER_PKG_NAME.tar.gz; \
    mv $SERVER_PKG_NAME pegasus; \
    rm $SERVER_PKG_NAME.tar.gz

COPY ./entrypoint.sh /
RUN chmod +x /entrypoint.sh

ENV LD_LIBRARY_PATH=/pegasus/bin

ENTRYPOINT ["/entrypoint.sh"]
