#!/bin/bash
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

echo "=============================================================================="
echo "Freeing up disk space on Github workflows"
echo "=============================================================================="

echo "Before freeing, the space of each disk:"
df -h

echo "Listing 100 largest packages ..."
dpkg-query -Wf '${Installed-Size}\t${Package}\n' | sort -n | tail -n 100

echo "Removing large packages ..."
apt-get -s autoremove
apt-get remove -y openjdk-11-jre-headless

echo "After removing large packages, the space of each disk:"
df -h

echo "Listing directories ..."
mount
ls -lrt /
du -csh /*
du -csh /__e/*/*
du -csh /__t/*/*
du -csh /__w/*/*
ls -lrt /github
du -csh /github/*
du -csh /opt/*
du -csh /usr/local/*
du -csh /usr/local/lib/*
du -csh /usr/local/share/*
du -csh /usr/share/*
echo "AGENT_TOOLSDIRECTORY is $AGENT_TOOLSDIRECTORY"

echo "Removing large directories ..."
rm -rf /__t/CodeQL
rm -rf /__t/PyPy
rm -rf /__t/Python
rm -rf /__t/Ruby
rm -rf /__t/go
rm -rf /__t/node
rm -rf /opt/ghc
rm -rf /usr/local/.ghcup
rm -rf /usr/local/graalvm
rm -rf /usr/local/lib/android
rm -rf /usr/local/lib/node_modules
rm -rf /usr/local/share/boost
rm -rf /usr/local/share/chromium
rm -rf /usr/local/share/powershell
rm -rf /usr/share/dotnet

echo "After freeing, the space of each disk:"
df -h
