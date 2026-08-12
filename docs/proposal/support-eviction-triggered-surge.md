---
title: Support proactive surge for protected evictions
authors:
- "@haoeeeee"
reviewers:
- TBD
approvers:
- TBD

creation-date: 2026-08-12

---

## Support proactive surge for protected evictions

### Summary

This proposal enables a voluntary Pod eviction to make progress when a ModelServing is fully protected and its ready replica count equals the configured `minAvailable`.

When the eviction webhook observes a real `CREATE pods/eviction` request that has no disruption headroom, it durably records an eviction surge request and denies the current admission attempt with status code 429. The ModelServing controller creates one temporary ServingGroup or Role instance for that request without changing the user's desired replica count. After the complete temporary unit becomes ready, a matching eviction retry is admitted. The temporary unit remains available while the original stable logical unit is recreated and is removed only after the stable unit has recovered and availability remains safe without the temporary unit.

Temporary capacity is identified by the durable eviction surge request and child-resource metadata. It is not represented as a rolling update and does not modify `spec.replicas`, workload revisions, or Role template hashes.

### Motivation

The eviction webhook prevents voluntary disruption from reducing ModelServing availability below `minAvailable`. When `readyReplicas == minAvailable`, denying the eviction is safe, but retrying alone cannot create additional disruption headroom. A node drain can therefore remain blocked even though the workload is healthy and the cluster has enough capacity to create a replacement first.

A ModelServing directly manages multi-Pod ServingGroups and Role instances. A replacement is usable only after the complete logical unit, including its entry and worker Pods, is present and ready. The controller therefore needs an asynchronous create-before-evict workflow that coordinates admission, resource creation, stable recovery, and cleanup.

#### Goals

- Allow protected voluntary evictions to progress when ready replicas equal `minAvailable`.
- Support both ServingGroup-level and Role-level protection.
- Create temporary capacity only in response to a real Eviction API request.
- Keep admission requests short and use client retry while capacity is prepared.
- Keep `spec.replicas` and Role desired replicas unchanged.
- Bound temporary capacity by an explicit eviction surge budget.
- Prevent ordinary replica reconciliation from adopting or deleting active temporary units.
- Reserve prepared capacity for the logical unit that requested it.
- Restore the original stable ServingGroup name or Role ID after eviction.
- Persist operation state across webhook replicas, controller restart, and leader failover.
- Release temporary resources only after recovery and a final budget check.
- Preserve existing behavior when eviction surge is omitted or resolves to zero.

#### Non-Goals

- Changing native Kubernetes PodDisruptionBudget behavior.
- Handling forced deletion or `kubectl drain --disable-eviction`, which bypasses the Eviction API.
- Changing workload traffic routing or connection-draining behavior.
- Guaranteeing scheduling progress when quota, accelerators, topology, storage, or gang scheduling cannot satisfy the temporary unit.
- Writing temporary desired capacity into the ModelServing spec.
- Treating a temporary unit as a permanent replacement for the original stable identity.
- Sharing temporary capacity between eviction and rolling update operations in the initial implementation.
- Supporting eviction surge with `recoveryPolicy: None`.

### Proposal

Extend `EvictionStrategySpec` with ServingGroup and per-Role surge ceilings. A blocked eviction creates an idempotent request in the per-ModelServing eviction tracker ConfigMap. A ConfigMap informer enqueues the ModelServing controller, which allocates and reconciles a temporary logical unit. A retry for the matching protected unit may consume that capacity only after it is complete and ready.

Each distinct blocked logical unit owns at most one temporary unit. `maxSurge` is a concurrency and capacity ceiling, not an eagerly created batch size. For example, `maxSurge: 3` permits three concurrent ServingGroup requests, but one blocked target creates only one temporary ServingGroup.

A temporary unit has an explicitly persisted purpose:

```text
steady unit       = normal capacity reconciled to the user spec
temporary unit    = capacity mapped by an active eviction surge request
orphan temporary  = marked temporary capacity whose tracker state is unavailable
```

Ordinals provide deterministic, collision-free names. They do not by themselves determine whether a unit is steady or temporary.

#### User Stories

##### Story 1: Fully protected ServingGroup drain

A ModelServing has three ready ServingGroups, `minAvailable: 3`, and `maxSurge: 1`. A drain requests eviction of a Pod in `ms-1`. The webhook records a request and denies that attempt. The controller creates a complete temporary ServingGroup, waits for it to become ready, and marks the request ready for admission. A retry is admitted, `ms-1` is recreated according to the recovery policy, and the temporary ServingGroup is removed after `ms-1` is complete and ready.

##### Story 2: Fully protected Role drain

A ServingGroup has three ready `decode` Role instances, `roleMinAvailable.decode: 3`, and `roleMaxSurge.decode: 1`. A drain requests eviction of a worker in `decode-1`. The controller creates a temporary `decode` Role instance in the same ServingGroup. After its entry and all workers are ready, a retry is admitted. The original `decode-1` is recreated with the same Role ID, after which the temporary Role is removed.

##### Story 3: Insufficient accelerator capacity

No node can schedule the temporary unit. The request remains provisioning, the webhook continues denying retries, and no stable capacity is deleted. Conditions and Events expose the scheduling stall. The drain proceeds only after capacity becomes available or the client exits according to its timeout.

#### Notes/Constraints/Caveats

- Eviction surge is opt-in and defaults to zero.
- The target node must be unschedulable before a surge request is created. A node cordon alone never creates capacity; the real Eviction request remains the trigger.
- Pending temporary capacity consumes a surge slot but does not contribute to availability.
- A prepared ready unit is reserved for its target and cannot be consumed by an unrelated eviction.
- ServingGroup surge creates a complete ServingGroup outside the target ServingGroup.
- Role surge creates a complete Role instance inside the target ServingGroup and requires `RoleRecreate`.
- Percentage budgets are resolved against steady desired replicas and never include temporary units in the denominator.
- Destructive rolling update and eviction surge operations are serialized in the initial implementation.
- Temporary resources are deleted after recovery; retaining a reusable idle pool is not part of the initial implementation.

### Design Details

#### API

Extend `EvictionStrategySpec`:

```go
type EvictionStrategySpec struct {
    // ProtectionLevel selects ServingGroup-level or Role-level protection.
    ProtectionLevel ProtectionLevelType `json:"protectionLevel"`

    // MinAvailable defines the minimum number of available steady
    // ServingGroups and is used only for ServingGroup protection.
    MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`

    // RoleMinAvailable defines the minimum number of available steady Role
    // instances for each protected Role in the target ServingGroup.
    RoleMinAvailable map[string]intstr.IntOrString `json:"roleMinAvailable,omitempty"`

    // MaxSurge limits temporary ServingGroups created for protected evictions.
    // Percentages are resolved against spec.replicas and rounded up.
    MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty"`

    // RoleMaxSurge limits temporary Role instances created for protected
    // evictions. Each value is resolved against the Role's desired replicas.
    RoleMaxSurge map[string]intstr.IntOrString `json:"roleMaxSurge,omitempty"`
}
```

ServingGroup example:

```yaml
spec:
  replicas: 3
  recoveryPolicy: ServingGroupRecreate
  rolloutStrategy:
    evictionStrategy:
      protectionLevel: ServingGroup
      minAvailable: 3
      maxSurge: 1
```

Role example:

```yaml
spec:
  recoveryPolicy: RoleRecreate
  rolloutStrategy:
    evictionStrategy:
      protectionLevel: Role
      roleMinAvailable:
        prefill: 2
        decode: 3
      roleMaxSurge:
        prefill: 1
        decode: 1
```

Validation rules are:

- Integer and percentage values must be non-negative.
- Percentage values above 100 percent are rejected.
- `maxSurge` is valid only with `protectionLevel: ServingGroup`.
- `roleMaxSurge` is valid only with `protectionLevel: Role`.
- Every `roleMaxSurge` key must name a Role in `spec.template.roles`.
- A Role configured in `roleMaxSurge` must also have a `roleMinAvailable` value.
- Role-level surge requires the effective recovery policy to be `RoleRecreate`.
- Eviction surge is rejected with `recoveryPolicy: None`.
- Omitted surge fields resolve to zero.

Eviction budget fields are operational policy and do not produce a new ControllerRevision or Role template hash.

#### Budget Resolution

For ServingGroup protection with steady desired replicas $R$:

$$
M = \operatorname{ResolveMinAvailable}(R)
$$

$$
S = \operatorname{ResolveMaxSurge}(R)
$$

For Role $r$ in target ServingGroup $g$, with steady desired replicas $R_{g,r}$:

$$
M_{g,r} = \operatorname{ResolveRoleMinAvailable}(R_{g,r})
$$

$$
S_{g,r} = \operatorname{ResolveRoleMaxSurge}(R_{g,r})
$$

Percentage values are rounded up. A surge slot is consumed by every active request, including `Pending` before resource creation, and by every marked orphan temporary unit. The slot invariants are:

$$
N_{slots} \le S
$$

or, for Role scope:

$$
N_{slots,g,r} \le S_{g,r}
$$

One blocked logical unit has disruption cost one, consumes one slot, and requests at most one temporary unit. A terminating temporary unit continues consuming its slot until every resource using its identity is absent.

#### Temporary Resource Identity

The eviction tracker is the primary identity source. Every temporary child also carries durable metadata so restart bootstrap and tracker-loss handling can classify it conservatively:

```yaml
metadata:
  labels:
    workload.serving.volcano.sh/eviction-surge: "true"
  annotations:
    workload.serving.volcano.sh/eviction-surge-request: "<request-id>"
    workload.serving.volcano.sh/eviction-surge-target: "<logical-unit-key>"
```

The metadata is applied to all Pods, Services, and PodGroups that belong to the temporary unit. Owner references continue pointing to the current ModelServing UID.

Classification uses this order:

```text
mapped by active request         -> temporary
marked temporary without request -> orphan temporary
otherwise                        -> steady
```

An orphan temporary unit is excluded from ordinary replica reconciliation and retained until the controller can prove that steady capacity is complete and safe without it.

#### Durable Tracker

The existing per-ModelServing ConfigMap remains the coordination object:

```text
kthena-eviction-tracker-<modelserving-name>
```

It stores existing disruption entries and a versioned surge request map:

```go
type TrackerState struct {
    Version       string                              `json:"version"`
    Disruptions   map[string]DisruptionEntry          `json:"disruptions,omitempty"`
    SurgeRequests map[string]EvictionSurgeRequest     `json:"surgeRequests,omitempty"`
}

type DisruptionEntry struct {
    ExpiresAt      metav1.Time `json:"expiresAt"`
    TriggerPodName string      `json:"triggerPodName,omitempty"`
    TriggerPodUID  types.UID   `json:"triggerPodUID,omitempty"`
    RequestID      string      `json:"requestID,omitempty"`
}

type EvictionSurgeRequest struct {
    ID              string              `json:"id"`
    Phase           SurgeRequestPhase   `json:"phase"`
    ProtectionLevel ProtectionLevelType `json:"protectionLevel"`

    TargetGroup  string `json:"targetGroup"`
    TargetRole   string `json:"targetRole,omitempty"`
    TargetRoleID string `json:"targetRoleID,omitempty"`
    TargetNode   string `json:"targetNode"`

    AllocatedGroup   string `json:"allocatedGroup,omitempty"`
    AllocatedOrdinal *int   `json:"allocatedOrdinal,omitempty"`
    AllocatedRoleID  string `json:"allocatedRoleID,omitempty"`

    TargetRevision  string `json:"targetRevision,omitempty"`
    RoleTemplateHash string `json:"roleTemplateHash,omitempty"`
    SpecGeneration  int64  `json:"specGeneration"`

    RequestedPods  map[types.UID]PodReference   `json:"requestedPods,omitempty"`
    AuthorizedPods map[types.UID]Authorization  `json:"authorizedPods,omitempty"`

    CreatedAt       metav1.Time  `json:"createdAt"`
    LastRequestedAt metav1.Time  `json:"lastRequestedAt"`
    AuthorizedAt    *metav1.Time `json:"authorizedAt,omitempty"`
    Message         string       `json:"message,omitempty"`
}

type PodReference struct {
    Name        string      `json:"name"`
    RequestedAt metav1.Time `json:"requestedAt"`
}

type Authorization struct {
    Name               string       `json:"name"`
    AllowedAt          metav1.Time  `json:"allowedAt"`
    DeletionObservedAt *metav1.Time `json:"deletionObservedAt,omitempty"`
}
```

ServingGroup request identity contains the ModelServing UID and target group name. Role request identity additionally contains Role name and Role ID. Requests for multiple Pods in the same logical unit share one request and one temporary unit.

When creating `Pending`, the webhook records the current ModelServing generation, target ServingGroup revision, and target Role template hash where applicable. The controller validates these captured inputs before provisioning and uses the captured template for temporary creation and stable recovery. A later incompatible template change cancels an ungranted request instead of silently changing the meaning of its reserved capacity.

The webhook and controller update the ConfigMap with `resourceVersion` compare-and-update and conflict retry. A process-local mutex may reduce local contention but is not the correctness boundary. A protected eviction is denied if its required durable update cannot be committed.

#### Request State Machine

```text
Pending
  -> Provisioning
  -> Ready
  -> Authorized
  -> Recovering
  -> Releasing
  -> Completed

Pending | Provisioning | Ready
  -> Releasing (inactive or cancelled before authorization)

Authorized
  -> Ready (all authorizations proven unused and client remains active)
  -> Releasing (all authorizations proven unused and client is inactive)
```

1. **Pending**: the webhook atomically reserved one surge slot and denied the triggering admission attempt.
2. **Provisioning**: the controller persisted a temporary identity and is creating or waiting for the complete unit.
3. **Ready**: the complete temporary unit is ready and reserved for the request target.
4. **Authorized**: the webhook admitted a matching retry and atomically recorded its disruption reservation and concrete Pod UID.
5. **Recovering**: at least one authorized Pod is deleting or absent and the stable logical unit is being restored.
6. **Releasing**: temporary resources are no longer required and their exact mapped resources are being deleted.
7. **Completed**: all temporary resources are absent and the request can be removed.

An ungranted request may transition from `Pending`, `Provisioning`, or `Ready` to `Releasing` after retry inactivity or target node uncordon. An `Authorized` request may return to `Ready` when every authorized Pod remains present, non-deleting, and ready through the authorization observation interval, proving that the admitted Eviction did not take effect.

`Stalled` is reported as a Condition reason while the request remains in its current safe phase; it is not a separate phase that permits unsafe cleanup.

#### Admission Snapshot and Adjusted Readiness

The webhook evaluates one conflict-retried transaction using:

- current-owner Pods and ModelServing;
- complete logical-unit readiness;
- active disruption reservations;
- active and terminating temporary units;
- ready temporary units reserved by other requests;
- resolved minimum-availability and surge budgets.

For target $T$:

```text
adjustedReady(T)
  = complete ready logical units
  - ready temporary units reserved by other Ready or Releasing requests
```

The original stable unit covered by a disruption reservation is already excluded from complete available units. Temporary units in `Authorized` or `Recovering` remain available because they are actively backing a disruption. A unit in `Releasing` is never exposed as generic admission headroom because it may be deleted immediately after the decision.

This calculation prevents a concurrent target from consuming capacity prepared for another request.

#### Admission Flow

The feature is evaluated only for real `CREATE pods/eviction` requests.

The webhook performs these steps:

1. Resolve the target Pod from the informer cache, with live API GET fallback.
2. Resolve the current ModelServing and validate owner UID.
3. Allow Pods that do not belong to a protected current ModelServing.
4. Resolve the protected logical unit and its complete readiness.
5. Read the tracker ConfigMap directly from the API server.
6. Decode current disruption entries and surge requests.
7. Refresh live Pods when the cached logical-unit set is incomplete.
8. Resolve `minAvailable` and the applicable surge ceiling.
9. Recompute adjusted readiness and used surge slots.
10. Apply the decision table and commit any required tracker mutation before returning the admission response.

The decision table is:

| State | Decision |
| --- | --- |
| The target Pod is already non-ready or deleting and its logical unit is not counted as ready | Admit without requesting surge because this Eviction cannot consume another ready unit. |
| The logical unit already has a valid disruption reservation | Admit same-unit continuation without consuming another disruption slot; append the actual Pod UID when the reservation is linked to a surge request. |
| `adjustedReady > minAvailable` and no surge request is needed | Atomically record a normal disruption entry and admit. |
| Matching request is `Ready`, its temporary unit is still complete and ready, and `adjustedReady > minAvailable` | Atomically transition to `Authorized`, record the actual Pod UID and linked disruption entry, then admit. |
| Matching request is `Pending` or `Provisioning` | Refresh `lastRequestedAt` with write throttling and deny with status code 429. |
| No request, `adjustedReady == minAvailable`, the target node is unschedulable, and a surge slot is free | Atomically create `Pending` with the target revision/hash snapshot, then deny with status code 429. |
| No request and all applicable surge slots are used | Deny with status code 429 and report `maxSurge exhausted`. |
| `adjustedReady < minAvailable` | Deny and wait for normal recovery; do not use surge to hide an existing deficit. |
| Recovery policy, node state, or operation coordination is incompatible | Deny without creating a request. |

The admission response includes the ModelServing, protection scope, request phase, allocated temporary identity when known, and the current blocker.

#### Admission Side Effects and Failure Policy

The webhook persists idempotent state because an Eviction request has no durable declarative object for the controller to watch after denial.

Dry-run requests never write or refresh tracker state. The webhook configuration declares:

```yaml
sideEffects: NoneOnDryRun
failurePolicy: Fail
```

Status code 429 is carried in the denied `AdmissionResponse`; the webhook HTTP transport response remains a valid AdmissionReview response.

#### Controller Trigger and Reconciliation Order

A filtered ConfigMap informer watches eviction tracker ConfigMaps. Add, update, and delete events validate the owner UID and enqueue the owning ModelServing. Active requests also schedule timed requeues for expiry, stalled provisioning, authorization observation, and release checks.

The ModelServing reconciliation order is:

```text
1. Read and normalize tracker state.
2. Classify steady, temporary, and orphan temporary resources.
3. Advance at most the safe request transitions for this snapshot.
4. Persist allocation or phase changes before creating or deleting resources.
5. Restore exact stable targets reserved by recovering requests.
6. Reconcile ordinary steady ServingGroups to spec.replicas.
7. Reconcile ordinary steady Role instances to Role replicas.
8. Reconcile temporary ServingGroup and Role resources.
9. Reconcile Services and PodGroups for both resource classes.
10. Coordinate destructive rollout work.
11. Update status, Conditions, and Events.
```

A resource-changing request transition returns and requeues before a generic scale-down path can make a contradictory decision.

#### ServingGroup Surge Reconciliation

For a ServingGroup request, the controller:

1. Validates the target ServingGroup revision captured in the request.
2. Collects occupied ordinals from live and terminating Pods, Services, PodGroups, and active requests.
3. Selects the next ordinal above the maximum occupied ordinal and persists `allocatedOrdinal` and `allocatedGroup` before creating resources.
4. Creates a complete ServingGroup using the captured target revision.
5. Creates a separate PodGroup when gang scheduling is enabled.
6. Creates all expected Role instances, entry Pods, worker Pods, and Services.
7. Repairs partial creation idempotently after restart.
8. Waits for complete readiness and transitions the request to `Ready`.

The temporary ServingGroup does not advance `CurrentRevision`, `UpdateRevision`, `CurrentReplicas`, or `UpdatedReplicas`.

ServingGroup-level protection supports both recovery scopes:

- `ServingGroupRecreate` restores the complete original target ServingGroup.
- `RoleRecreate` restores the affected Role inside the original target ServingGroup.

#### Role Surge Reconciliation

For a Role request, the controller:

1. Resolves the target `(ServingGroup, Role, RoleID)` and captures its revision and Role template hash.
2. Collects occupied Role ordinals for that Role inside the target ServingGroup.
3. Selects the next Role ordinal above the maximum occupied ordinal and persists `allocatedRoleID` before resource creation.
4. Reconciles the existing ServingGroup PodGroup to include the temporary Role task when gang scheduling is enabled.
5. Creates one entry Pod, all configured worker Pods, and the headless Service.
6. Repairs partial creation idempotently after restart.
7. Waits for the exact expected Pod set to become ready and transitions the request to `Ready`.

The temporary Role exists in the same `(ServingGroup, Role)` availability scope as its target. Role surge requires `RoleRecreate`; a whole-ServingGroup recovery would remove the temporary Role together with the target group.

#### Ordinal Allocation

An allocated identity stored in the request is the request's durable reservation. A separate public lease object is not required.

Allocation follows these rules:

1. Read all current-owner live and terminating resources, not only the in-memory datastore.
2. Include identities already persisted by every active request.
3. Allocate `maxOccupiedOrdinal + 1`; never reuse a name while any Pod, Service, or PodGroup with that identity still exists.
4. Persist allocation through a conflict-retried ConfigMap update before child creation.
5. Adopt an existing child only when owner UID and request metadata match exactly.
6. Keep the allocation occupied until all associated resources are confirmed absent.
7. Use the same occupied-ordinal snapshot for ordinary scale-up and temporary allocation.

Temporary identity remains request-driven even when `spec.replicas` changes. A normal scale-up creates another steady unit with a non-conflicting identity; it does not adopt an active temporary unit.

#### Steady and Temporary Replica Isolation

Ordinary replica reconciliation separates units before comparing counts:

```go
steadyGroups, temporaryGroups := classifyServingGroups(allGroups, tracker)

reconcileSteadyGroups(
    current = steadyGroups,
    desired = ms.Spec.Replicas,
)

reconcileTemporaryGroups(temporaryGroups, tracker)
```

The same rule applies to Role instances inside a ServingGroup.

Ordinary Role reconciliation iterates only steady ServingGroups. Temporary ServingGroups are reconciled exclusively from their request's captured template so the latest steady template cannot mutate them into rollout capacity.

Ordinary scale-down candidates exclude:

- temporary units mapped by active requests;
- temporary resources that are still terminating;
- marked orphan temporary units;
- stable target identities reserved for recovery.

When a target stable unit is absent during recovery, the controller restores that exact group name or Role ID before performing anonymous steady scale-up. While the previous resources are still terminating, the reserved target counts as pending stable intent so another steady unit is not created as a substitute.

#### Complete Readiness

Readiness is evaluated from expected resource shape, not from a non-empty observed subset.

A Role instance is complete and ready only when:

- exactly one expected entry Pod exists;
- worker count and indexes match `workerReplicas`;
- every expected Pod belongs to the current ModelServing UID;
- no expected Pod has a deletion timestamp;
- every expected Pod reports `PodReady=True`;
- the Role instance is not covered by an active disruption reservation.

A ServingGroup is complete and ready only when:

- every expected steady Role exists with its desired steady replica count;
- temporary Role instances are excluded from the steady count;
- every expected steady Role instance is complete and ready;
- the ServingGroup is not covered by an active disruption reservation.

Temporary Role readiness is evaluated independently, allowing an additional temporary Role without causing the target ServingGroup's steady readiness check to fail.

A temporary ServingGroup is evaluated against the complete Role shape stored in its captured ControllerRevision. A temporary Role is evaluated against its captured Role template and worker count. Neither uses a later workload template implicitly.

The webhook uses live API fallback when informer data is demonstrably incomplete. Unknown or inconsistent protected state fails closed.

#### Stable Target Recovery

The webhook never creates the replacement for an evicted target. Existing Pod event handling applies the configured recovery policy, and the ModelServing controller gives the request's target identity priority during reconciliation.

For `ServingGroupRecreate`:

1. Delete the remaining target ServingGroup resources.
2. Wait until the original group identity is free.
3. Recreate the original group name and ordinal from its captured revision.
4. Wait for the complete ServingGroup to become ready.

For `RoleRecreate`:

1. Delete the remaining Pods and Service of the affected stable Role instance.
2. Recreate the same Role ID from its captured revision and Role template hash.
3. Wait for the complete Role instance and containing ServingGroup to become ready.

The replacement Pods receive new UIDs and may be scheduled on different nodes, while stable Service, group, Role, and PodGroup identities are restored.

#### Authorization Observation and Rollback

An `Allowed=true` response from the Kthena webhook does not prove that the Pod was deleted. A later admission stage or native disruption policy may still reject the Eviction.

The webhook records every concretely admitted Pod UID in `authorizedPods`. The controller moves `Authorized` to `Recovering` when any authorized Pod receives a deletion timestamp or disappears.

If an authorized Pod continuously remains present, non-deleting, and ready for the authorization observation interval, that authorization is unused. When all authorizations are unused and the target remains complete and ready, the controller atomically:

- removes the linked disruption reservation;
- clears unused authorization records;
- returns the request to `Ready` if the client is still retrying; or
- transitions to `Releasing` if the request has become inactive.

Once an authorized UID has been observed deleting or absent, elapsed time can never prove recovery. The request must wait for complete stable-target recovery.

#### Disruption Reservation Lifetime

A normal disruption entry retains its existing recovery-aware TTL behavior. A disruption entry created from a surge authorization also carries the request ID.

While the linked request is `Authorized` or `Recovering`:

- the controller renews the disruption entry;
- the entry is not removed solely because its original TTL elapsed;
- concurrent eviction calculations continue treating the target logical unit as disrupted.

The linked entry is removed only when authorization is rolled back without deletion or when the stable target is complete and ready and the request advances to release.

#### Safe Release

The controller may transition to `Releasing` only when all of the following hold:

- at least one authorization either resulted in a real deletion or all authorizations were proven unused;
- every authorization observation is resolved;
- the target stable logical identity exists;
- the target stable unit is complete and ready;
- no active disruption reservation other than the linked request remains unresolved;
- excluding the temporary unit leaves ready replicas greater than or equal to the current applicable `minAvailable`;
- no other active request depends on the same temporary identity.

During `Releasing`, the controller deletes the exact `allocatedGroup` or `allocatedRoleID`. A terminating unit continues consuming a surge slot. The request is removed only after every associated Pod, Service, and PodGroup is confirmed absent.

If recovery cannot complete, the controller retains temporary capacity and reports a stalled condition rather than performing an unsafe shrink.

#### Request Expiry

Matching denied retries refresh `lastRequestedAt`, with writes throttled to avoid updating the ConfigMap on every retry.

Before authorization, inactivity in `Pending`, `Provisioning`, or `Ready` transitions the request to `Releasing`. Partial and ready temporary resources are then cleaned up. After authorization, request lifetime is determined by observed deletion and recovery, not by retry activity.

Initial controller defaults are:

```text
pre-authorization inactivity timeout: 10 minutes
authorization observation interval: 60 seconds
```

These may initially be controller configuration rather than CRD fields.

#### Node State

The real Eviction request is the operation trigger. Node state is an additional safety gate:

- a cordoned node without an Eviction request does not create temporary capacity;
- a blocked request creates surge only when the target node has `spec.unschedulable=true`;
- uncordon before authorization cancels the request and releases unused capacity;
- uncordon after authorization does not bypass observed recovery requirements.

Temporary readiness is not accepted if the complete temporary unit is placed on the target draining node.

#### Spec Changes During a Request

Before authorization, incompatible template, protection-level, Role, or recovery-policy changes cancel the waiting request and release temporary capacity. Replica and budget changes are resolved again from the latest spec.

After authorization, safety takes precedence:

- lowering `maxSurge` does not remove active temporary capacity before safe release;
- the latest `minAvailable` is used by the release predicate;
- non-destructive scale-up may continue using identities that do not conflict with temporary allocations;
- destructive steady scale-down waits until the active request completes;
- target recovery completes before later desired scale-down is applied.

#### Rolling Update Coordination

The initial implementation serializes destructive rollout and eviction operations:

- active destructive rolling update work blocks creation of a new eviction surge request;
- an active eviction surge request blocks initiation of new destructive rolling-update mutations;
- already authorized or recovering eviction requests always retain protection until safe release;
- template changes may be observed and revisions recorded while resource mutation waits;
- eviction temporary units do not promote or complete a rolling update.

#### Status, Conditions, Events, and Logs

Status uses these counting scopes:

| Field | Counting scope |
| --- | --- |
| `replicas` | All live steady and temporary ServingGroups. |
| `availableReplicas` | All complete ready steady and temporary ServingGroups. |
| `currentReplicas` | Steady ServingGroups on `CurrentRevision`; excludes temporary units. |
| `updatedReplicas` | Steady ServingGroups on `UpdateRevision`; excludes temporary units. |
| `currentRevision` | Derived from steady ServingGroups only. |
| `updateRevision` | Latest desired workload revision; unaffected by eviction temporary units. |

Role surge does not increase top-level ServingGroup replica counts.

Existing availability and progression Conditions are calculated from steady intent plus aggregate ready capacity. A provisioning temporary unit alone does not make an otherwise healthy workload unavailable, target recovery may report `Progressing`, and eviction activity never reports `UpdateInProgress`. Revision promotion and rollout completion replace physical-replica equality checks with steady-only completion checks.

Add a non-exclusive Condition:

```text
type: EvictionSurgeInProgress
reason: Requested | Provisioning | Ready | Authorized | Recovering | Releasing | Stalled
```

Suggested Events are:

- `EvictionSurgeRequested`
- `EvictionSurgeProvisioning`
- `EvictionSurgeReady`
- `EvictionSurgeAuthorized`
- `EvictionRecoveryStarted`
- `EvictionStableUnitRecovered`
- `EvictionSurgeReleasing`
- `EvictionSurgeReleased`
- `EvictionSurgeExpired`
- `EvictionSurgeStalled`

Admission and controller logs include physical and adjusted ready counts, minimum availability, resolved surge ceiling, used slots, request ID, phase, target identity, allocated identity, tracker resourceVersion, and decision reason.

#### Failure and Recovery

- **Temporary unit cannot schedule**: remain provisioning, deny retries, preserve all stable capacity, and report scheduler state.
- **Temporary Pod failure**: repair the mapped temporary unit and do not authorize until complete readiness returns.
- **Controller restart**: read tracker state, scan live resources, validate owner UID and markers, and resume the observed phase.
- **Webhook restart or replica change**: read the tracker before deciding; no admission safety depends on process memory.
- **Tracker deletion or corruption**: classify marked children as orphan temporary, fail closed for ambiguous protected evictions, restore the marked target when incomplete, and release only after steady capacity is safe without the orphan.
- **Manual temporary deletion**: recreate the same allocated identity while the active request still requires it.
- **Name collision**: adopt only exact owner/request matches; otherwise select and persist another free identity before creation.
- **ModelServing deletion**: owner references garbage-collect tracker and child resources.
- **Same-name ModelServing recreation**: owner UID validation prevents adoption of old tracker and child state.
- **Revision unavailable or corrupt**: retain stable capacity, mark the request stalled, and do not fall back to an unverified template.
- **ConfigMap conflict**: reload all admission and allocation state and recompute the decision.
- **Persistent tracker write failure**: deny protected eviction fail-closed.

#### Backward Compatibility

Both surge fields are optional and default to zero. Existing ModelServing resources retain current eviction admission behavior.

The tracker decoder accepts existing disruption-only entries and rewrites them into the versioned document on the next successful update. No existing Pod, Service, or PodGroup is classified as temporary unless it is mapped by an active request or has explicit eviction-surge metadata.

The API change requires regeneration of deepcopy code, client-go types, apply configurations, CRDs, Helm-embedded CRDs, and API reference documentation. Controller RBAC requires ConfigMap read/write/watch and Node read permissions.

### Test Plan

Unit tests cover:

- Integer and percentage surge resolution and validation.
- ServingGroup and Role recovery-policy validation.
- `ready > minAvailable`, equality, and deficit admission cases.
- Dry-run requests producing no tracker writes.
- Idempotent retry for the same Pod and logical unit.
- Multiple Pods in one logical unit consuming one request.
- Concurrent targets never exceeding the applicable surge ceiling.
- Reserved ready capacity remaining unavailable to unrelated targets.
- Atomic authorization and disruption-entry creation.
- ConfigMap conflict retry across webhook replicas.
- Ordinary ServingGroup scale-up and scale-down excluding temporary groups.
- Ordinary Role scale-up and scale-down excluding temporary Role instances.
- Exact stable group and Role identity recovery.
- Complete readiness rejecting partial entry/worker sets.
- Authorization rollback when the Pod never starts deletion.
- Linked disruption renewal while recovery exceeds the normal TTL.
- Pre-authorization expiry and target node uncordon.
- Spec and budget changes during every active phase.
- Status and revision counts excluding temporary units where required.
- Rolling-update serialization.
- Tracker loss and orphan temporary reconstruction.

Integration tests cover webhook, ConfigMap informer, workqueue, Pod events, state transitions, restart recovery, and timed requeue behavior.

End-to-end tests cover:

- ServingGroup drain at `ready == minAvailable` from request through cleanup.
- Role entry and worker drain at `ready == roleMinAvailable`.
- Concurrent protected targets bounded by `maxSurge`.
- Unschedulable temporary capacity preserving stable replicas.
- Canceled drain cleaning an ungranted temporary unit.
- Admission allowed followed by no Pod deletion and safe rollback.
- Controller and webhook restart in `Provisioning`, `Ready`, `Authorized`, and `Recovering`.
- User scale-up during an active request without adopting the temporary identity.
- Exact target identity recovery under both supported recovery policies.
- Tracker deletion retaining marked temporary capacity until safe cleanup.
- Volcano PodGroup behavior for ServingGroup and Role temporary units.
- Proof that `spec.replicas` never changes and ready capacity never drops below the configured minimum.
