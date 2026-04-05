#!/usr/bin/env bash

set -euo pipefail

service_account="$1"

oidc_token_file='/tmp/oidc_token'
creds_file='/home/semaphore/creds.json'

pool_name_prefix="$(echo "${service_account}" | cut --delimiter='-' -f2 | cut --delimiter='.' -f1)"

echo "$SEMAPHORE_OIDC_TOKEN" > "${oidc_token_file}"
gcloud iam workload-identity-pools create-cred-config \
  "projects/1099124060225/locations/global/workloadIdentityPools/semaphoreci-com-identity-pool/providers/${pool_name_prefix}-semaphoreci-com" \
  --service-account="${service_account}" \
  --service-account-token-lifetime-seconds=600 \
  --output-file="${creds_file}" \
  --credential-source-file="${oidc_token_file}"
export GOOGLE_APPLICATION_CREDENTIALS="${creds_file}"
gcloud auth login --cred-file="${creds_file}"
