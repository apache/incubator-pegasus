#!/usr/bin/env bash

PROJECT_DIR=$(dirname "${SCRIPT_DIR}")
cd "${PROJECT_DIR}" || exit 1

if [ ! -f "${PROJECT_DIR}"/golangci-lint-1.29.0-linux-amd64/golangci-lint ]; then
    wget https://github.com/golangci/golangci-lint/releases/download/v1.29.0/golangci-lint-1.29.0-linux-amd64.tar.gz
    tar -xzvf golangci-lint-1.29.0-linux-amd64.tar.gz
fi

gofmt -l -w .
golangci-lint-1.29.0-linux-amd64/golangci-lint run
