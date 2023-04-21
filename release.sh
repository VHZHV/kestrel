#!/usr/bin/env bash

set -euo pipefail

mvn versions:set
mvn clean deploy
mvn scm:tag
mvn versions:set
