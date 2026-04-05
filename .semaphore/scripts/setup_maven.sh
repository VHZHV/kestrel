#!/usr/bin/env bash

curl -s "https://get.sdkman.io" | bash &
cache restore sdkman-java &
cache restore "maven-${BRANCH_NAME},maven-${MAIN_BRANCH}" &

wait

source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk env install
