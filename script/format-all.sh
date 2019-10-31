#!/usr/bin/env bash

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_DIR=$(dirname "${SCRIPT_DIR}")
cd "${PROJECT_DIR}" || exit 1

SRC_FILES=(src/main/java/com/xiaomi/infra/common/*.java
           src/main/java/com/xiaomi/infra/config/*.java
           src/main/java/com/xiaomi/infra/sample/local/*.java
           src/main/java/com/xiaomi/infra/sample/spark/*.java
           src/main/java/com/xiaomi/infra/service/db/*.java
           src/main/java/com/xiaomi/infra/service/*.java
           src/main/java/com/xiaomi/infra/*.java

           src/test/java/com/xiaomi/infra/pegasus/client/*.java
           src/test/java/com/xiaomi/infra/pegasus/metrics/*.java
           src/test/java/com/xiaomi/infra/pegasus/rpc/async/*.java
           src/test/java/com/xiaomi/infra/pegasus/tools/*.java
           src/test/java/com/xiaomi/infra/pegasus/base/*.java
           )

if [ ! -f "${PROJECT_DIR}"/google-java-format-1.7-all-deps.jar ]; then
    wget https://github.com/google/google-java-format/releases/download/google-java-format-1.7/google-java-format-1.7-all-deps.jar
fi
java -jar "${PROJECT_DIR}"/google-java-format-1.7-all-deps.jar --replace "${SRC_FILES[@]}"
