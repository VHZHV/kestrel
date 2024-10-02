#!/usr/bin/env bash

export REPO_NAME='enforcement'
export MAIN_BRANCH='master'

export BRANCH_NAME="${SEMAPHORE_GIT_WORKING_BRANCH:-"${MAIN_BRANCH}"}"

sem-version java 17

(while ! checkout; do :; done) &
