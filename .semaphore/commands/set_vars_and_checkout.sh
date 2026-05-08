#!/usr/bin/env bash

export REPO_NAME='kestrel'
export MAIN_BRANCH='0.3.9_hozah'

export BRANCH_NAME="${SEMAPHORE_GIT_WORKING_BRANCH:-"${MAIN_BRANCH}"}"

(while ! checkout; do :; done)
cd ${REPO_NAME} || exit

source "scripts/wait_for.sh"
