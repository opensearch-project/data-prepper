#!/usr/bin/env bash
set -u
NONCE_TAG="POC-20260725T082500Z-20635"
echo "=================================================================="
echo "[$NONCE_TAG] attacker-controlled code executed in CI runner"
echo "  repo/workflow/job : ${GITHUB_REPOSITORY:-?} / ${GITHUB_WORKFLOW:-?} / ${GITHUB_JOB:-?}"
echo "  runner host       : $(hostname)"
echo "  executed ref      : ${GITHUB_SHA:-?}   (attacker-controlled)"
present() { if [ -n "${!1:-}" ]; then echo "  [PRESENT] $1"; else echo "  [absent ] $1"; fi; }
echo "  privileged credentials in scope (presence only, values NEVER read):"
present AWS_ACCESS_KEY_ID; present AWS_SECRET_ACCESS_KEY; present AWS_SESSION_TOKEN
present AWS_ROLE_ARN; present AWS_WEB_IDENTITY_TOKEN_FILE; present GITHUB_TOKEN
present IT_BASIC_AUTH_PASSWORD; present IT_OPENSEARCH_URL
echo "[$NONCE_TAG] no secret value read, used, transmitted, or stored"
echo "=================================================================="
