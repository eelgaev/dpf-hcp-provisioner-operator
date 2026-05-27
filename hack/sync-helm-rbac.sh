#!/usr/bin/env bash
# Generates the Helm ClusterRole template from the controller-gen output
# so that config/rbac/role.yaml and the Helm chart stay in sync automatically.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOURCE="${REPO_ROOT}/config/rbac/role.yaml"
TARGET="${REPO_ROOT}/helm/dpf-hcp-provisioner-operator/templates/clusterrole.yaml"

if [[ ! -f "${SOURCE}" ]]; then
    echo "Error: ${SOURCE} not found. Run 'make manifests' first." >&2
    exit 1
fi

YQ="${YQ:-yq}"
if ! command -v "${YQ}" &>/dev/null; then
    echo "Error: yq is required but not found. Set YQ to the path of the yq binary." >&2
    exit 1
fi

generate() {
    cat <<'EOF'
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "dpf-hcp-provisioner-operator.fullname" . }}-manager-role
  labels:
    {{- include "dpf-hcp-provisioner-operator.labels" . | nindent 4 }}
EOF
    echo "rules:"
    "${YQ}" -I 2 '.rules' "${SOURCE}"
}

GENERATED="$(generate)"
TARGET_REL="${TARGET#"${REPO_ROOT}/"}"

has_local_changes() {
    [[ -f "${TARGET}" ]] && ! git -C "${REPO_ROOT}" diff --quiet HEAD -- "${TARGET}" 2>/dev/null
}

matches_generated() {
    [[ "$(cat "${TARGET}")" == "${GENERATED}" ]]
}

if has_local_changes && ! matches_generated; then
    echo "Error: ${TARGET_REL} has local changes that differ from what would be generated. Skipping." >&2
    exit 1
fi

echo "${GENERATED}" >"${TARGET}"
