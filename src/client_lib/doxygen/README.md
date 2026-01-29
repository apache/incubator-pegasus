<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Pegasus C++ Client Doxygen Guide

This document explains how to install Doxygen and generate the Pegasus C++ client API
documentation on **macOS** and **Linux**.

## Directory Layout

- `docs/doxygen/Doxyfile`: Preconfigured Doxygen configuration
- `docs/doxygen/output/html`: Generated HTML output

> If you update the C++ client headers or comments, re-run the generation command to
> refresh the documentation.

## 1. Install Doxygen

### macOS (Homebrew)

```bash
brew install doxygen
```

### Linux (Debian / Ubuntu)

```bash
sudo apt-get update
sudo apt-get install -y doxygen
```

Verify the installation:

```bash
doxygen -v
```

## 2. Generate Documentation

Run this from the repository root:

```bash
doxygen docs/doxygen/Doxyfile
```

The generated HTML entry point is:

```
docs/doxygen/output/html/index.html
```