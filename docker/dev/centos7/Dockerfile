# This is a docker with all stuff built after compilation. It is a
# large image that's only used for testing.
#
# ```sh
#     docker build -t pegasus_onebox .
#     docker run -it pegasus_onebox bash
# ```


FROM centos:7

MAINTAINER Wu Tao <wutao1@xiaomi.com>

RUN yum -y install gcc gcc-c++ automake autoconf libtool make cmake git file wget unzip python-devel which && \
    yum -y install openssl-devel boost-devel libaio-devel snappy-devel bzip2-devel readline-devel zlib zlib-devel patch

# Gflags
RUN cd /opt && \
  wget https://github.com/gflags/gflags/archive/v2.2.1.zip && \
  unzip v2.2.1.zip && \
  cd /opt/gflags-2.2.1 && \
  mkdir build && \
  cd /opt/gflags-2.2.1/build && \
  export CXXFLAGS="-fPIC" && \
  cmake .. && \
  make VERBOSE=1 && \
  make -j3 && \
  make install

RUN git clone --recursive https://github.com/XiaoMi/pegasus.git /pegasus

RUN cd /pegasus && \
    ./run.sh build -c

RUN yum -y install jre nmap-ncat.x86_64

CMD cd /pegasus

