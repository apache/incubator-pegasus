#!/usr/bin/env bash

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_DIR=$(dirname "${SCRIPT_DIR}")
cd "${PROJECT_DIR}" || exit 1

SRC_FILES=(src/main/java/com/xiaomi/infra/pegasus/spark/common/*.java
           src/main/java/com/xiaomi/infra/pegasus/spark/common/utils/*.java
           src/main/java/com/xiaomi/infra/pegasus/spark/common/utils/gateway/*.java
           src/main/java/com/xiaomi/infra/pegasus/spark/common/utils/metaproxy/*.java
           src/main/java/com/xiaomi/infra/pegasus/spark/analyser/*.java
           src/main/java/com/xiaomi/infra/pegasus/spark/bulkloader/*.java
           )

if [[ ! -f "${PROJECT_DIR}"/google-java-format-1.7-all-deps.jar ]]; then
    wget https://github.com/google/google-java-format/releases/download/google-java-format-1.7/google-java-format-1.7-all-deps.jar
fi
if ! java -jar "${PROJECT_DIR}"/google-java-format-1.7-all-deps.jar --replace "${SRC_FILES[@]}"; then
    echo "ERROR: failed to format java codes"
    exit 1
fi

if [[ ! -f "${PROJECT_DIR}"/scalafmt ]]; then
    if ! wget https://github.com/scalameta/scalafmt/releases/download/v2.6.1/scalafmt-linux.zip; then
        echo "ERROR: failed to download scalafmt"
        exit 1
    fi
    unzip scalafmt-linux.zip
    rm -rf scalafmt-linux.zip
    chmod +x scalafmt
fi

if ! "${PROJECT_DIR}"/scalafmt; then
    echo "ERROR: failed to format scala codes"
    exit 1
fi
