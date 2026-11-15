# Failed Pod recovery on production/release-1.0

## Base and scope

- Base repository: `VanderChen/kthena`.
- Base branch: `production/release-1.0`.
- Base commit: `cf918f17f6b44d5dd1c08e68b35ad1c1ccb0ccda`.
- Validation date: 2026-11-15.

Under `recoveryPolicy: None`, allow Failed Pods to follow the existing restart
grace, deletion, and single-member replacement paths. Preserve `-1`, restarted
non-failed Pods, dynamic policy/grace changes, and the existing UID/resource
version, ownership, rollout-intent, and deletion-plan checks. `go.mod` and
`go.sum` are unchanged from the release base.

This release has recovery permission checks at scheduling, timer execution, and
live API deletion. All three now take Pod phase into account. The timer checks
the current cached Pod; deletion checks the live Pod. No in-place update feature
or other main-branch implementation was imported.

## Automated validation

The new Failed Pod regression cases failed against the original release
controller and passed after the fix. Tests cover unset/zero/positive/-1 grace,
duplicate events preserving the deadline, deletion without another event,
entry/worker single-member replacement, UID/resource-version preconditions,
current phase checks, and recovery during controller cold start.

Existing recovery matrix tests were updated for None/Failed behavior. Cold-start
rollout and policy-transition fixtures use positive grace to hold a Failed Pod
while testing those operations; a separate zero-grace cold-start regression
asserts its immediate recovery.

- `make generate`: passed; generated CRD descriptions and API reference updated.
- `make test`: passed, including the final test fixtures.
- Focused recovery and cold-start `go test -mod=readonly -race`: passed.
- `bin/golangci-lint run --new-from-rev=cf918f17f6b44d5dd1c08e68b35ad1c1ccb0ccda`:
  passed, with the new regression file included in the staged diff.
- `git diff --check`: passed.
- Final full ModelServing/controller-manager `go test -mod=readonly -race`: passed.

The full race command was:

```sh
go test -mod=readonly -race ./pkg/model-serving-controller/... ./cmd/kthena-controller-manager/... -count=1
```

## Real kubelet eviction and regression scenarios

A dedicated single-node Kind cluster ran Kubernetes v1.34.0 on Linux/arm64. Both
baseline and fixed controller-manager binaries were built with Go 1.26.0 and
installed with this release's Helm chart, including its real admission webhooks.
The baseline binary used the release controller file through a Go build overlay.

Each workload initially had three BusyBox Pods: a prefill entry and worker, and
a decode entry. Writing 32 MiB into an 8 MiB emptyDir triggered actual kubelet
eviction. Pod API watches confirmed Failed/Evicted, deletion, and replacement
UIDs. Assertions checked the exact replacement scope, Ready state, and
ModelServing available replicas. Positive timing values are observations from
this run, not latency guarantees.

All 14 scenarios passed:

| # | Scenario | Observed result |
| --- | --- | --- |
| 1 | `baseline-eviction` | unmodified release baseline reproduces: Failed/Evicted remains beyond grace=5s; AvailableReplicas=0 |
| 2 | `fixed-recovers-baseline` | fixed controller recovers baseline Failed Pod after restart; healthy sibling UIDs unchanged |
| 3 | `none-zero-eviction` | Failed/Evicted -> deleted in 0.04s (grace=0); changed=['none-zero-0-prefill-0-1']; AvailableReplicas=1 |
| 4 | `none-grace-entry-eviction` | Failed/Evicted -> deleted in 5.27s (grace=5); changed=['none-grace-0-prefill-0-0']; AvailableReplicas=1 |
| 5 | `none-minus-one-retains` | None, grace=-1: Failed/Evicted retained; siblings unchanged; AvailableReplicas=0 |
| 6 | `none-container-restart` | None, grace=2: restartCount=2, same Pod UIDs, recovery Ready |
| 7 | `none-manual-delete` | manual deletion replaces only the missing worker |
| 8 | `role-recreate-eviction` | Failed/Evicted -> deleted in 3.03s (grace=3); changed=['role-recreate-0-prefill-0-0', 'role-recreate-0-prefill-0-1']; AvailableReplicas=1 |
| 9 | `group-recreate-eviction` | Failed/Evicted -> deleted in 3.05s (grace=3); changed=['group-recreate-0-decode-0-0', 'group-recreate-0-prefill-0-0', 'group-recreate-0-prefill-0-1']; AvailableReplicas=1 |
| 10 | `role-minus-one-retains` | RoleRecreate, grace=-1: Failed/Evicted retained; siblings unchanged; AvailableReplicas=0 |
| 11 | `group-minus-one-retains` | ServingGroupRecreate, grace=-1: Failed/Evicted retained; siblings unchanged; AvailableReplicas=0 |
| 12 | `role-transient-restart` | RoleRecreate, grace=10: restartCount=1, same Pod UIDs, recovery Ready |
| 13 | `scaling-and-delete` | ServingGroup 1->2->1 and prefill Role 1->2->1, Ready counts correct; CR deletion cleans Pods |
| 14 | `role-recreate-rollout` | Prefill members replaced with the new template; decode UID unchanged; Ready restored |

## Run notes and limits

- Full `make lint` reports ten existing formatting, import, whitespace, and
  deprecated-API findings in unchanged files. The diff-scoped lint check passed.
- Initial full race runs hit asynchronous assertions in
  `TestModelServingControllerModelServingLifecycle/ModelServingGangPolicyPodGroups`
  and `TestCreatePodAlreadyExistsRequeues`. Each passed five focused runs with
  the original release controller overlay. Their source and assertions were
  unchanged. The modified recovery and cold-start tests passed focused race
  checks; the final full-suite result is recorded above.
- The tests used default Kubernetes scheduling and required no GPU. This is
  single-node local coverage; it does not establish production-load coverage.
- The validation cluster was cleaned up after collecting logs and snapshots;
  existing local clusters were left untouched.
