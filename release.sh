#!/usr/bin/env bash

set -Euo pipefail

./mvnw versions:set
./mvnw clean deploy
./mvnw scm:tag
./mvnw versions:set
