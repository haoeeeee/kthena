#!/usr/bin/env bash

# Namespace and ModelServing can be overridden through environment variables.
TEST_NS="${TEST_NS:-coordinated-role-rollout-test}"
TEST_MS="${TEST_MS:-proportional-role-rollout}"

# pod_rows prints the observed Pod state as tab-separated columns:
# POD, ROLE, VERSION, READY. Other query and control functions use this output.
pod_rows() {
  kubectl get pods -n "${TEST_NS}" \
    -l "modelserving.volcano.sh/name=${TEST_MS}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.modelserving\.volcano\.sh/role}{"\t"}{.spec.containers[0].env[?(@.name=="VERSION")].value}{"\t"}{.status.containerStatuses[0].ready}{"\n"}{end}'
}

# count_pods prints the number of Pods for one Role and version, regardless of
# whether those Pods are Ready. Usage: count_pods ROLE VERSION.
count_pods() {
  pod_rows | awk -v role="$1" -v version="$2" \
    '$2 == role && $3 == version {n++} END {print n + 0}'
}

# count_ready prints the number of Ready Pods for one Role and version.
# Usage: count_ready ROLE VERSION.
count_ready() {
  pod_rows | awk -v role="$1" -v version="$2" \
    '$2 == role && $3 == version && $4 == "true" {n++} END {print n + 0}'
}

# show_state prints every test Pod followed by an A/B/C version and Ready
# summary. Run it after each meaningful rollout action to inspect the result.
show_state() {
  printf '%-52s %-6s %-8s %-6s\n' POD ROLE VERSION READY
  pod_rows | sort

  echo
  for role in a b c; do
    printf '%s: v1=%s, v2=%s, v2Ready=%s\n' \
      "${role}" \
      "$(count_pods "${role}" v1)" \
      "$(count_pods "${role}" v2)" \
      "$(count_ready "${role}" v2)"
  done
}

# wait_count polls once per second until one Role/version reaches the expected
# Pod count. It prints the current state and fails when the timeout expires.
# Usage: wait_count ROLE VERSION EXPECTED [TIMEOUT_SECONDS].
wait_count() {
  local role="$1"
  local version="$2"
  local expected="$3"
  local timeout="${4:-300}"

  for _ in $(seq 1 "${timeout}"); do
    if [ "$(count_pods "${role}" "${version}")" -eq "${expected}" ]; then
      return 0
    fi
    sleep 1
  done

  echo "Timeout waiting for ${role}/${version}=${expected}"
  show_state
  return 1
}

# wait_ready polls once per second until one Role/version reaches the expected
# Ready count. Unlike wait_count, a created but NotReady Pod does not count.
# Usage: wait_ready ROLE VERSION EXPECTED [TIMEOUT_SECONDS].
wait_ready() {
  local role="$1"
  local version="$2"
  local expected="$3"
  local timeout="${4:-300}"

  for _ in $(seq 1 "${timeout}"); do
    if [ "$(count_ready "${role}" "${version}")" -eq "${expected}" ]; then
      return 0
    fi
    sleep 1
  done

  echo "Timeout waiting for Ready ${role}/${version}=${expected}"
  show_state
  return 1
}

# touch_role makes every currently existing NotReady v2 Pod of one Role Ready
# by creating /tmp/ready. It does not create Pods or affect v1 Pods. Newly
# created v2 Pods require another call. Usage: touch_role ROLE.
touch_role() {
  local role="$1"

  pod_rows |
    awk -v role="${role}" \
      '$2 == role && $3 == "v2" && $4 != "true" {print $1}' |
    while read -r pod; do
      [ -n "${pod}" ] || continue
      kubectl exec -n "${TEST_NS}" "${pod}" -- touch /tmp/ready || true
    done
}

# touch_all_v2 calls touch_role for A, B, and C. Use it only after the manual
# dependency and maxSkew checkpoints, when driving the remaining rollout to
# completion.
touch_all_v2() {
  touch_role a
  touch_role b
  touch_role c
}

# usage documents the executable subcommands exposed by main.
usage() {
  cat <<'EOF'
Usage:
  validate-rollout.sh pod-rows
  validate-rollout.sh count-pods ROLE VERSION
  validate-rollout.sh count-ready ROLE VERSION
  validate-rollout.sh show-state
  validate-rollout.sh wait-count ROLE VERSION EXPECTED [TIMEOUT_SECONDS]
  validate-rollout.sh wait-ready ROLE VERSION EXPECTED [TIMEOUT_SECONDS]
  validate-rollout.sh touch-role ROLE
  validate-rollout.sh touch-all-v2

Environment variables:
  TEST_NS  Test namespace (default: coordinated-role-rollout-test)
  TEST_MS  ModelServing name (default: proportional-role-rollout)
EOF
}

# main maps user-facing subcommands to the functions above. It is called only
# when this file is executed, not when it is sourced into the current shell.
main() {
  case "${1:-}" in
    pod-rows)
      pod_rows
      ;;
    count-pods)
      count_pods "${2:?Role is required}" "${3:?Version is required}"
      ;;
    count-ready)
      count_ready "${2:?Role is required}" "${3:?Version is required}"
      ;;
    show-state)
      show_state
      ;;
    wait-count)
      wait_count \
        "${2:?Role is required}" \
        "${3:?Version is required}" \
        "${4:?Expected count is required}" \
        "${5:-300}"
      ;;
    wait-ready)
      wait_ready \
        "${2:?Role is required}" \
        "${3:?Version is required}" \
        "${4:?Expected count is required}" \
        "${5:-300}"
      ;;
    touch-role)
      touch_role "${2:?Role is required}"
      ;;
    touch-all-v2)
      touch_all_v2
      ;;
    help|-h|--help|"")
      usage
      ;;
    *)
      echo "Unknown command: $1" >&2
      usage >&2
      return 1
      ;;
  esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -euo pipefail
  main "$@"
fi
