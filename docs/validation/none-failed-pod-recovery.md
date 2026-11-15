# None recovery policy: Failed Pod validation

## Scope

Allow a `Failed` Pod to follow the existing deletion and replacement path when
`recoveryPolicy` is `None`. Preserve the configured restart grace period,
`restartGracePeriodSeconds: -1`, and the handling of restarted non-failed Pods.
`Succeeded` Pods and in-place timeout policy are outside this change.

## Environment and method

- Date: 2026-11-15.
- Base: `dbef2b4ada405bc75525bd59a9f7dc5d5c661b2f` from upstream `main`.
- Dedicated single-node Kind cluster, Kubernetes v1.34.0, Linux/arm64.
- Controller binaries built with Go 1.26.0 and installed using this repository's
  Helm chart. The API server, admission webhooks, and kubelet were real.
- Baseline binary built with the base commit's controller file using a Go
  overlay; fixed binary built with the changed controller file. Other source
  files were identical between these two builds.
- Each initial workload had one ServingGroup: a prefill Role with one entry and
  one worker, and a decode Role with one entry. Containers used BusyBox and the
  default Kubernetes scheduler; no GPU was required.
- Pod API watches recorded `Failed/Evicted`, deletion, and replacement UIDs.
  Assertions checked the exact set of changed UIDs, Pod readiness, and
  `status.availableReplicas`.

Eviction was triggered by writing 32 MiB into an `emptyDir` with an 8 MiB size
limit, rather than patching Pod status. The test Pod mounted the volume at
`/scratch`; its container ephemeral-storage limit was 128 MiB, above the write
size. With a test namespace and Pod selected, the trigger was:

```sh
kubectl exec -n "$TEST_NAMESPACE" "$TEST_POD" -- sh -c \
  'dd if=/dev/zero of=/scratch/evict.bin bs=1048576 count=32 >/dev/null 2>&1; sync'
```

The watch confirmed `status.phase: Failed` and `status.reason: Evicted`. Kubelet
events reported that usage of the `scratch` volume exceeded its 8 MiB limit.
Timings below measure the interval between the observed Failed and deleted
events; they are observations from this run, not latency guarantees.

## Cluster results

All 18 scenarios passed. Unless retention was expected, each scenario finished
with all desired Pods Ready and `status.availableReplicas` restored.

| # | Scenario | Observed result |
| --- | --- | --- |
| 1 | Baseline, `None`, grace 5, worker eviction | Failed worker retained beyond the grace period; available replicas dropped to 0. Reproduced the bug. |
| 2 | Switch baseline workload to fixed controller | Existing Failed worker replaced; healthy sibling UIDs unchanged. |
| 3 | `None`, grace 0, worker eviction | Deleted after approximately 0.12 seconds; only worker UID changed. |
| 4 | `None`, grace 5, entry eviction | Deleted after approximately 5.05 seconds; only failed entry UID changed. |
| 5 | `None`, grace -1, worker eviction | Failed Pod retained during the 12-second observation; all UIDs unchanged; available replicas 0. |
| 6 | `None`, grace 2, unexpected container restart | Original Pod retained beyond grace; clearing the induced liveness failure restored Ready. |
| 7 | `None`, manual worker deletion | Only the missing worker was recreated. |
| 8 | `RoleRecreate`, grace 3, worker eviction | Failed Pod deleted after approximately 3.03 seconds; both prefill members replaced; decode UID unchanged. |
| 9 | `ServingGroupRecreate`, grace 3, worker eviction | Failed Pod deleted after approximately 3.04 seconds; all three members replaced. |
| 10 | `RoleRecreate`, grace -1, worker eviction | Failed Pod retained during the 12-second observation; all UIDs unchanged; available replicas 0. |
| 11 | `ServingGroupRecreate`, grace -1, worker eviction | Failed Pod retained during the 12-second observation; all UIDs unchanged; available replicas 0. |
| 12 | `RoleRecreate`, grace 10, transient container restart | Container recovered within grace; no Pod replacement after the deadline. |
| 13 | `None`, scaling and CR deletion | ServingGroup and prefill Role replica counts each scaled 1 to 2 to 1; Ready counts matched; deleting the CR cleaned up its Pods. |
| 14 | `None`, ordinary Role rolling update | Prefill members replaced with the new template; decode UID unchanged. |
| 15 | `None`, successful in-place image update | Target image running, update marked done, all Pod UIDs unchanged. |
| 16 | `None`, in-place deadline exceeded | After 60 seconds, Running Pods retained with unchanged UIDs; releasing the readiness failure allowed completion. |
| 17 | `RoleRecreate`, in-place deadline exceeded | After the 60-second deadline and restart grace, prefill members replaced; decode UID unchanged. |
| 18 | `None`, eviction during an unfinished in-place update | Failed worker replaced on its own; sibling UIDs unchanged; target images and availability restored. |

Container restart tests used a file-controlled liveness failure. In-place
timeout tests used a separate file-controlled readiness failure without causing
a liveness restart. The second image contained an additional file, giving it a
different image ID rather than merely a second tag for the same image.

## Automated checks

The following completed successfully before preparing the commits:

```sh
make generate
go test ./pkg/model-serving-controller/... -count=1
go test -race ./pkg/model-serving-controller/... ./cmd/kthena-controller-manager/... -count=1
git diff --check
```

The race run passed for controller, datastore, in-place update, plugins,
PodGroup manager, utilities, webhook, and the controller-manager entry point.
The new regression tests additionally cover unset/zero/positive/-1 grace,
duplicate events not resetting grace, expiry without another event, UID delete
preconditions, single-member replacement, and a Failed Pod carrying an active
in-place annotation.

### Repository-wide checks before commit

- `make test` reran generation successfully. Its ModelServing packages passed.
  The remaining failures were
  `TestKVEventSubscriber_AttributesEventsToDialedPod` and
  `TestKVEventSubscriber_ReopensStreamOfReplacedPod` in the router scheduler
  plugins package, each reporting that no KV events stream opened.
- These two tests also failed with the baseline controller overlay. Their test
  server expects connections through both `127.0.0.1` and `127.0.0.2`. A separate
  local socket check connected through `127.0.0.1` but timed out through
  `127.0.0.2` on this macOS host. No host network configuration was changed.
- `make lint` reported nine existing `SA5011` findings in unchanged autoscaler,
  router connector/datastore, and controller E2E test files.
- The scoped lint command below passed with no findings:

  ```sh
  bin/golangci-lint run ./pkg/model-serving-controller/... ./pkg/apis/workload/...
  ```

Consequently, the 18 cluster scenarios and scoped checks passed; the full
repository `make test` and `make lint` targets did not pass on this host.

### Recheck after excluding module replacements

With `go.mod` restored to the upstream base, the full scoped race command was
rerun with `-mod=readonly` and passed. The scoped lint command also passed.
An initial race recheck hit a status synchronization failure in
`TestModelServingControllerModelServingLifecycle/ModelServingUpdateScaleDown`.
That subtest then passed ten focused runs with each of the baseline and fixed
controllers, and the subsequent full scoped race run passed. No test assertions
were changed.

## Run notes and limits

- The initial validation used local Kubernetes staging module replacements for
  IDE module graph resolution. Those replacements are excluded from this
  branch; its `go.mod` matches the upstream base.
- The first in-place fixtures used an invalid update-method enum and a deadline
  below the CRD minimum. Admission rejected them. They were corrected to
  `InPlace` and 60 seconds, and all three scenarios were rerun successfully.
  Product validation and assertions were not weakened.
- During the baseline stage, concurrent Go race compilation caused some
  one-second exec probes to time out on the local machine. Healthy siblings
  restarted containers but retained their Pod UIDs. All identity, recovery-scope,
  and final-readiness assertions remained enabled.
- This validates the listed scenarios in a local single-node cluster. It does
  not claim multi-node, GPU, or production load coverage.
- The dedicated test cluster was deleted after collecting results. Existing
  local clusters were left untouched.
