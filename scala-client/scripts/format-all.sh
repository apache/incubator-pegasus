#!/usr/bin/env bash

# scala format tool,see https://github.com/scalameta/scalafmt
sbt scalafmtSbt scalafmt test:scalafmt
