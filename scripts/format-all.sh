#!/usr/bin/env bash

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_DIR=$(dirname "${SCRIPT_DIR}")
cd "${PROJECT_DIR}" || exit 1

SRC_FILES=(pegasus-spark-common/src/main/java/com/xiaomi/infra/pegasus/spark/*.java
           pegasus-spark-analyser/src/main/java/com/xiaomi/infra/pegasus/spark/analyser/*.java
           )

if [[ ! -f "${PROJECT_DIR}"/google-java-format-1.7-all-deps.jar ]]; then
    wget https://github.com/google/google-java-format/releases/download/google-java-format-1.7/google-java-format-1.7-all-deps.jar
fi
if ! java -jar "${PROJECT_DIR}"/google-java-format-1.7-all-deps.jar --replace "${SRC_FILES[@]}"; then
    echo "ERROR: failed to format java codes"
    exit 1
fi

if [[ ! -f "${PROJECT_DIR}"/scalafmt ]]; then
    if ! wget https://github.com/scalameta/scalafmt/releases/download/v2.3.2/scalafmt-linux.zip; then
        echo "ERROR: failed to download scalafmt"
        exit 1
    fi
    unzip scalafmt-linux.zip
    mv scalafmt-linux/scalafmt .
    rm -rf scalafmt-linux.zip scalafmt-linux
    chmod +x scalafmt
fi

if ! "${PROJECT_DIR}"/scalafmt; then
    echo "ERROR: failed to format scala codes"
    exit 1
fi
