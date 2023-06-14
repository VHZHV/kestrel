#!/usr/bin/env bash

set -Euo pipefail

mvn versions:set
mvn clean deploy
mvn scm:tag
mvn versions:set
