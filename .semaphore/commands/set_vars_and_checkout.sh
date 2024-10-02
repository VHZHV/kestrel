#!/usr/bin/env bash

export REPO_NAME='kestrel'
export MAIN_BRANCH='0.3.9_hozah'

export BRANCH_NAME="${SEMAPHORE_GIT_WORKING_BRANCH:-"${MAIN_BRANCH}"}"

sem-version java 17

checkout
#(while ! checkout; do :; done) &
