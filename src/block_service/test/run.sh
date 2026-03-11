#!/bin/sh

##############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
##############################################################################

if [ -z "${REPORT_DIR}" ]; then
    REPORT_DIR="."
fi

# By default, unit tests use local_service.
# To connect to HDFS/JuiceFS or other storage systems in unit tests, configure the corresponding paths.
package_dir="example: pegasus-server-2.4.8-without-slog-cb99c3e-glibc2.17-release absolute path"
echo $package_dir

# Set the ld library path
ld_library_path=$package_dir/DSN_ROOT/lib:$package_dir/bin:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$ld_library_path

export CLASSPATH=$package_dir/hadoop/
for f in $package_dir/hadoop/*.jar; do
export CLASSPATH=$CLASSPATH:$f
done
JAVA_JVM_LIBRARY_DIR=$(dirname $(find "${JAVA_HOME}/" -name libjvm.so  | head -1))
export LD_LIBRARY_PATH=${JAVA_JVM_LIBRARY_DIR}:$LD_LIBRARY_PATH

echo CLASSPATH=$CLASSPATH
echo LD_LIBRARY_PATH=$LD_LIBRARY_PATH
echo JAVA_JVM_LIBRARY_DIR=$JAVA_JVM_LIBRARY_DIR

./clear.sh
output_xml="${REPORT_DIR}/dsn_block_service_test.xml"
GTEST_OUTPUT="xml:${output_xml}" ./dsn_block_service_test
