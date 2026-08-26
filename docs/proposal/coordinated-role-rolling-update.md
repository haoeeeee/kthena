---
title: Coordinated Proportional Role Rolling Update
authors:
- "@haoeeeee"
reviewers:
- TBD
approvers:
- TBD

creation-date: 2026-08-17

---

## Coordinated Proportional Role Rolling Update

### Summary

Kthena supports `RoleRollingUpdate` for replacing changed Role replicas inside a ServingGroup. The existing controller calculates each Role's rolling candidates independently. For a ServingGroup whose Roles cooperate in one request path, independent replacement can create a large difference between their target-version capacities.

This proposal adds optional coordination to `RoleRollingUpdate`. Users configure:

- the Roles participating in one coordination domain;
- `maxSkew`, which limits the percentage-point difference in normalized rollout progress;
- a Role dependency DAG, which describes the version call topology.

For every reconcile, the controller builds one snapshot for all participating Roles in a ServingGroup, calculates a temporary `coordinationPartition` for each Role, and combines it with the Role's configured partition:

```text
effectivePartition = max(userPartition, coordinationPartition)
```

The existing Role rolling-update path then uses `effectivePartition` and `maxUnavailable` to perform deletion and recreation.

For a dependency chain `A -> B -> C`, A is the rollout root. B and C can prestart within the `maxSkew` limit. A starts after B and C have eligible target-version `RoleRunning` capacity. At rollout completion, an old A keeps at least one old B, and an old B keeps at least one old C.

### Motivation

Consider one ServingGroup with:

```text
A: 100 replicas
B:  50 replicas
C:  10 replicas

A depends on B
B depends on C
```

The target revision changes all three Roles. The application routes new-version requests through the new-version dependency chain.

Independent Role rolling updates can produce two problems:

1. A large Role can accumulate much more target-version Ready capacity than another Role, so the dependency chain has unbalanced processing capacity.
2. A target-version upstream Role can become reachable before its target-version dependency has Ready capacity.

The coordinated rollout maintains proportional target-version capacity and establishes the new dependency chain before exposing its root.

#### Goals

- Coordinate selected Roles independently inside each ServingGroup.
- Express rollout progress as target-version `RoleRunning` replicas divided by update-eligible replicas.
- Bound proportional progress with a percentage `maxSkew`.
- Support different replica counts and user partitions across Roles.
- Allow internal dependency Roles to prestart together.
- Start each rollout root after its complete dependency closure has target-version Ready capacity.
- Preserve the old-version dependency chain until old callers have exited.
- Reuse the existing Role partition, `maxUnavailable`, deletion, recreation, and readiness mechanisms.
- Reconstruct rollout decisions from Kubernetes resources after controller restart.
- Report the current coordination blocker through ModelServing status.

### Proposal

#### Applicability

Coordination is enabled under `RoleRollingUpdate`:

```yaml
spec:
  rolloutStrategy:
    type: RoleRollingUpdate
    roleCoordination:
      maxSkew: 10%
```

Users continue to submit Role templates through `ModelServing.spec.template.roles`. With `RoleRollingUpdate`, the controller compares Role template hashes and replaces changed Role replicas inside each existing ServingGroup. Coordination is calculated separately for every ServingGroup.

#### API Design

```go
type RolloutStrategy struct {
    Type RolloutStrategyType `json:"type"`

    RollingUpdateConfiguration *RollingUpdateConfiguration
        `json:"rollingUpdateConfiguration,omitempty"`

    RoleCoordination *RoleCoordination
        `json:"roleCoordination,omitempty"`
}

type RoleCoordination struct {
    // Empty selects every Role in spec.template.roles.
    Roles []string `json:"roles,omitempty"`

    // Maximum percentage-point difference in normalized rollout progress.
    MaxSkew *intstr.IntOrString `json:"maxSkew"`

    // Version call topology used for root startup and old-chain retention.
    Dependencies []RoleRolloutDependency `json:"dependencies,omitempty"`
}

type RoleRolloutDependency struct {
    Role      string   `json:"role"`
    DependsOn []string `json:"dependsOn"`
}
```

Example:

```yaml
apiVersion: workload.serving.volcano.sh/v1alpha1
kind: ModelServing
metadata:
  name: example
spec:
  rolloutStrategy:
    type: RoleRollingUpdate
    roleCoordination:
      roles: [a, b, c]
      maxSkew: 10%
      dependencies:
      - role: a
        dependsOn: [b]
      - role: b
        dependsOn: [c]

  template:
    roles:
    - name: a
      replicas: 100
      maxUnavailable: 1
      partition: 0
    - name: b
      replicas: 50
      maxUnavailable: 1
      partition: 0
    - name: c
      replicas: 10
      maxUnavailable: 1
      partition: 0
```

The `roles` list defines one coordination domain:

- an empty list selects all Roles in `spec.template.roles`;
- a non-empty list selects the named Roles;
- selected Roles share one `maxSkew` calculation;
- each selected Role keeps its own `partition` and `maxUnavailable`.

The webhook validates:

- `roleCoordination` is used with `RoleRollingUpdate`;
- `maxSkew` is a percentage in the range `(0%, 100%]`;
- at least two Roles participate;
- all Role names exist;
- Role names and dependency edges are unique;
- dependency endpoints belong to the selected Role set;
- the dependency graph contains no self-edge or cycle.

### Design Details

#### Overall Flow

For each non-deleting ServingGroup, one reconcile performs the following work:

1. Resolve the participating Roles and their target Role template hashes.
2. Resolve each Role's `userPartition` and rollout ordinal range.
3. Classify current Role replicas as target Ready, target in-flight, or old.
4. Calculate normalized Ready progress and the `maxSkew` allowance.
5. Apply rollout-root startup and old-version dependency constraints.
6. Calculate `coordinationPartition` and `effectivePartition` for every participating Role.
7. Pass the allowed ordinal range to the existing Role rolling-update logic.
8. Apply the existing per-Role `maxUnavailable` budget.
9. Delete admitted old Role replicas and recreate their target-version replacements through the existing paths.
10. Reconcile again when deletion, creation, or readiness changes the observed state.

Each reconcile uses the complete cross-Role snapshot before admitting new rollout work.

#### Per-Role State

For a participating Role `r`:

```text
D_r = desired Role replica count
P_r = resolved user partition
T_r = max(D_r - P_r, 0)

R_r = target-hash replicas in RoleRunning
I_r = admitted target-version work that is not RoleRunning
O_r = old-hash replicas that still exist
```

The rollout ordinal range is:

```text
[P_r, D_r)
```

Only replicas in this range contribute to `T_r`, `R_r`, `I_r`, and target-version dependency Ready capacity.

Replicas below `P_r` remain protected by the user partition. Replicas at or above `D_r` belong to ordinary scale-down. Old-hash replicas that still exist contribute to `O_r` while they can remain part of the old-version request path.

`I_r` includes:

- an old Role replica whose rollout deletion is in progress;
- a target-hash Role replica that has not reached `RoleRunning`;
- a missing replacement ordinal between deletion completion and target-version recreation.

The in-flight count reserves already admitted work across asynchronous deletion, creation, scheduling, and readiness.

#### Rollout Progress

The coordinator calculates two normalized values:

```text
readyProgress_r =
    R_r / T_r

startedProgress_r =
    min(R_r + I_r, T_r) / T_r
```

Ready progress supplies the common baseline. Started progress consumes the allowance already granted to a Role.

A selected Role participates in the active baseline while its template changed and its update-eligible rollout still has work in progress. Completed and unchanged Roles leave the active minimum. Their eligible target-version Ready replicas can still satisfy a rollout-root dependency.

#### maxSkew

`maxSkew` is expressed as percentage points of normalized progress. For each reconcile:

```text
readyBaseline =
    minimum readyProgress across active participating Roles

ratioLimit =
    min(1, readyBaseline + maxSkew)

allowedStarted_r =
    min(T_r, ceil(ratioLimit * T_r))
```

The per-Role ceiling converts the percentage allowance to a replica count. Each Role keeps its own integer step.

With A=100, B=50, C=10, and `maxSkew: 10%`:

| State | Ready progress A/B/C | Allowed started total A/B/C |
| --- | --- | --- |
| Initial | 0% / 0% / 0% | 10 / 5 / 1 |
| B=5 and C=1 Ready | 0% / 10% / 10% | 10 / 5 / 1 |
| A=10 Ready | 10% / 10% / 10% | 20 / 10 / 2 |
| A=20, B=10, C=2 Ready | 20% / 20% / 20% | 30 / 15 / 3 |

The table shows the percentage relationship across unequal replica counts. The startup dependency rule blocks A in the first row, so initial target-version work is admitted only for B and C. Each Role's §maxUnavailable§ can further reduce the number acted on in one reconcile.

The implementation compares ratios with integer cross-multiplication and converts the configured percentage to fixed-point integer units.

#### Dependency Graph

An edge:

```yaml
- role: a
  dependsOn: [b]
```

describes a version call from A to B.

The graph provides two coordination rules:

1. rollout-root startup;
2. old-version dependency retention.

##### Rollout roots

A rollout root is a selected Role whose name does not appear in another selected Role's `dependsOn` list.

For:

```text
A -> B -> C
```

A is the rollout root. Its transitive dependency closure is `{B, C}`. B and C are internal Roles.

Before the first target-version A replacement is admitted, B and C must each have at least one target-hash `RoleRunning` replica inside their own `[P, D)` range. B and C can prestart together under `maxSkew`.

For a graph containing only:

```text
B -> C
```

B is the rollout root and waits for target-version C Ready capacity.

The first-start state is reconstructed from `R_r + I_r`. A root with `R_r + I_r = 0` applies the startup check. Once target-version work for that root has started, subsequent progress is coordinated by `maxSkew`.

##### Old-version dependency retention

Every direct edge also protects the old-version request path.

For `A -> B -> C`:

- while an old A replica exists, B retains at least one old replica;
- while an old B replica exists, C retains at least one old replica.

With zero user partitions, the old-version completion order is:

```text
A completes
    -> B can remove its final old replica
        -> C can remove its final old replica
```

The retention rule uses observed old replicas. A terminating old replica contributes to `O_r` until its Pods have disappeared.

If a Role's `userPartition` already preserves one or more old replicas, that boundary satisfies the retention requirement. A partition-protected old caller also keeps its direct old dependency for the same lifetime.

#### Effective Partition

The coordinator maintains three partition values:

- `userPartition`: the Role partition supplied by the user;
- `coordinationPartition`: the temporary boundary calculated for the current snapshot;
- `effectivePartition`: the boundary passed to the Role rolling-update executor.

The initial coordination boundary comes from `maxSkew`:

```text
coordinationPartition_r =
    D_r - allowedStarted_r
```

The dependency rules then adjust the same boundary:

```text
if r is a rollout root,
   R_r + I_r = 0,
   and a Role in r's dependency closure has R = 0:
    coordinationPartition_r = D_r

if D_r > 0 and a direct dependent d has O_d > 0:
    coordinationPartition_r =
        max(coordinationPartition_r, 1)
```

The final boundary is:

```text
effectivePartition_r =
    max(userPartition_r, coordinationPartition_r)
```

The existing rolling-update path selects outdated Role replicas in:

```text
[effectivePartition_r, D_r)
```

and applies the Role's `maxUnavailable` budget to that range. Existing descending ordinal ordering remains in use.

The rollout denominator continues to use:

```text
T_r = D_r - userPartition_r
```

This keeps normalized progress stable while `coordinationPartition` changes between reconciles.

#### Example: A, B, and C

Assume each Role has ten replicas, `maxUnavailable: 1`, `partition: 0`, and `maxSkew: 10%`.

Startup:

| Role | maxSkew boundary | Dependency adjustment | effectivePartition |
| --- | ---: | ---: | ---: |
| A | 9 | A is a blocked root: 10 | 10 |
| B | 9 | old A retention: at least 1 | 9 |
| C | 9 | old B retention: at least 1 | 9 |

B and C each replace one replica. When both replacements become `RoleRunning`, A's startup adjustment is released and A can replace one replica.

Middle:

- the Ready baseline advances as target replicas become `RoleRunning`;
- `allowedStarted` grows independently for A, B, and C from the same percentage limit;
- dependency edges impose no continuous A/B/C progress ordering.

Completion:

```text
effective partitions near the tail

A = 0
B = 1
C = 1

old A disappears  -> B may use 0
old B disappears  -> C may use 0
```

#### Interaction with Existing Role Rolling Update

The current `rolesToDeleteForRoleRollingUpdate` path calculates outdated replicas and each Role's local `maxUnavailable` allowance. Coordination adds one cross-Role calculation before the final delete list is returned:

```text
current Role objects
    -> build all participating Role states
    -> calculate coordinationPartition
    -> calculate effectivePartition
    -> filter outdated ordinals to [effectivePartition, D)
    -> apply existing maxUnavailable
    -> DeleteRole
    -> existing scaleUpRoles recreates the target replica
```

Replica-only scaling continues through the existing scaling logic.

When a replica increase and template change are submitted together, creation of additional target-version ordinals is admitted through the same `effectivePartition`. A rollout-root scale-up therefore waits for its dependency closure, and every participating Role consumes its `maxSkew` allowance.

Scale-down continues through the existing scaling logic. The new desired count immediately defines `D_r`, so scale-down excess at or above `D_r` leaves progress and dependency Ready calculations.

`maxUnavailable` remains a per-Role availability budget. The coordinator determines the rollout range, and `maxUnavailable` determines how many replicas inside that range can be deleted.

#### Reconcile Continuation

The controller completes one bounded amount of work and returns from reconcile. Later resource state changes enqueue the ModelServing again.

A participating Role replica becomes `RoleRunning` after its entry Pod and all worker Pods are Running and Ready. That transition enqueues the ModelServing so the next reconcile can:

- unlock a rollout root;
- advance the Ready baseline;
- admit the next proportional work;
- release an old-version retention boundary.

#### Controller Restart

The controller rebuilds coordination state from:

- the current ModelServing spec;
- current and target ControllerRevisions;
- Role and Pod revision/hash labels;
- Pod Ready conditions;
- Pod deletion timestamps;
- current and missing Role ordinals.

The startup path lists existing Pods, rebuilds the datastore, and enqueues existing ModelServings. The first reconcile recalculates `coordinationPartition` and `effectivePartition`.

Restart-state classification:

| Observed state | Reconstructed state |
| --- | --- |
| target-hash Role is `RoleRunning` | Ready progress |
| target-hash Role exists and is not `RoleRunning` | in-flight work |
| old-hash Pods are terminating | in-flight replacement and old-version presence |
| an ordinal in current `[P, D)` is missing after delete-first admission | in-flight replacement awaiting recreation |
| a missing ordinal belongs to a requested scale-up | scale-up work awaiting admission |
| an ordinal is at or above current `D` | scale-down excess |

Terminating old Pods are read from the Pod informer cache so old-version dependency retention survives datastore reconstruction.

### Status and Observability

The proposal adds a ModelServing condition through the existing `status.conditions` field:

```go
const ModelServingCoordinatedRoleRolloutBlocked ModelServingConditionType =
    "CoordinatedRoleRolloutBlocked"
```

The condition is `True` when a participating Role still has rollout work and is waiting on a coordination constraint or admitted target-version readiness.

Supported reasons:

- `DependencyNotReady`;
- `MaxSkewLimitReached`;
- `OldVersionDependencyPresent`;
- `TargetReplicaNotReady`.

Example:

```yaml
status:
  conditions:
  - type: CoordinatedRoleRolloutBlocked
    status: "True"
    reason: DependencyNotReady
    message: "ServingGroup example-0: root role a is waiting for target-version RoleRunning capacity from roles b and c"
    observedGeneration: 12
```

Current blockers are ordered by ServingGroup ordinal, Role declaration order, and fixed reason priority. The first blocker supplies the condition reason, and the message summarizes the affected Roles.

The controller sets the condition to `False` with `ProgressAvailable` or `RolloutComplete` when the corresponding state is observed. Kubernetes Events record blocker changes and rollout resumption.

### Failure and Blocking Behavior

#### Slow or failed target replica

A target replica in Creating, pending scheduling, or waiting for Pod Ready remains in `I_r`. Its reserved allowance stays occupied. The blocker condition reports `TargetReplicaNotReady`.

#### Dependency readiness

A rollout root remains at `coordinationPartition = D` while a Role in its dependency closure has no eligible target-version `RoleRunning` replica. The blocker condition reports `DependencyNotReady` and identifies the dependency Roles.

#### maxSkew limit

A Role whose `R_r + I_r` reaches `allowedStarted_r` waits for the shared Ready baseline to advance. The blocker condition reports `MaxSkewLimitReached`.

#### Old-version retention

A dependency at its final old replica keeps an effective partition of at least one while an old direct caller exists. The blocker condition reports `OldVersionDependencyPresent`.

#### Partition-limited dependency

A dependency with no eligible target-version ordinal remains unable to satisfy a rollout-root Ready requirement. Its user partition remains authoritative and the root remains blocked.

#### Single-replica dependency

With delete-first replacement, a one-replica dependency can require the same replica for the old chain and for creation of new-version capacity. The rollout remains blocked at that boundary and reports the current dependency reason.

### Implementation

The implementation adds a per-ServingGroup coordinator with a pure calculation interface:

```go
type coordinatedRoleState struct {
    roleName string

    desired       int
    userPartition int
    target        int
    ready         int
    inFlight      int
    oldReplicas   int
    active        bool
}

type coordinatedRoleDecision struct {
    effectivePartitions map[string]int
    blockers             []coordinatedRoleBlocker
}

func coordinatedRolePartitions(
    states []coordinatedRoleState,
    coordination *RoleCoordination,
) (coordinatedRoleDecision, error)
```

Main code changes:

- API types, generated clients, deepcopy code, and CRDs for `roleCoordination`;
- webhook validation for Role selection, `maxSkew`, and dependency DAGs;
- Role-state construction from datastore Roles, Pods, hashes, readiness, and ordinals;
- `rolesToDeleteForRoleRollingUpdate` integration for `effectivePartition`;
- `scaleUpRoles` integration for concurrent template update and scale-up;
- immediate ModelServing enqueue on participating Role `RoleRunning` transitions;
- ModelServing condition and Event updates for coordination blockers.

Existing `DeleteRole`, `CreatePodsByRole`, ControllerRevision, Role template hash, and per-Role `maxUnavailable` execution remain the rollout mechanisms.

### Risks and Mitigations

#### Integer progress granularity

Small Roles have coarse progress steps. Per-Role ceiling conversion permits one local step while retaining the configured percentage baseline for every other Role.

#### Repeated reconciliation

In-flight reservations include deleting, missing, Creating, and waiting-for-Ready work. Repeated reconciles calculate the same or a stricter admission boundary from the observed snapshot.

#### Informer convergence

Terminating Pods and missing ordinals are classified conservatively. Reconciliation resumes as informer state converges.

#### Long-running blockers

The ModelServing condition records the blocking ServingGroup, Role, reason, and relevant progress data. Events record transitions between blocker states.

### Test Plan

#### Unit tests

- Role selection and `maxSkew` validation.
- Dependency existence, duplicate edge, self-edge, and cycle validation.
- Rollout-root inference for chains, branches, multiple roots, and isolated Roles.
- Exact ordinal range `[P, D)`.
- Scale-down excess excluded from progress and dependency Ready capacity.
- Ready and in-flight progress from Running, Creating, Deleting, terminating, and missing states.
- Equal and unequal Role replica counts.
- Per-Role ceiling conversion and small-Role quantization.
- `A -> B -> C` internal prestart and root startup behavior.
- `B -> C` root startup behavior.
- Direct-edge old-version retention.
- User partition combined with coordination partition.
- Descending ordinal selection and `maxUnavailable`.
- Deterministic blocker aggregation and condition transitions.
- Restart reconstruction during deletion, missing replacement, creation, and readiness.

#### Property and fuzz tests

For generated replica counts, partitions, Role states, and acyclic graphs:

- `effectivePartition >= userPartition`;
- projected started work stays within each Role's quantized allowance;
- a rollout root starts after every Role in its closure has eligible Ready capacity;
- target Ready replicas outside `[P, D)` never unlock a root;
- an observed old caller retains its direct old dependency;
- identical snapshots produce identical decisions;
- generated invalid graphs fail validation.

#### Integration tests

- A ModelServing spec update enters coordinated Role rolling update.
- Deletion and recreation gaps preserve in-flight reservation.
- Participating Role `RoleRunning` transitions trigger the next decision.
- Controller restart reconstructs deleting and Creating work.
- Terminating old Pods retain the old-version dependency boundary.
- Status updates preserve existing conditions and update `CoordinatedRoleRolloutBlocked`.
- Concurrent template update and scale-up uses the same effective partition.

#### End-to-end tests

- Equal-replica A/B/C rollout with delayed C readiness.
- A/B/C rollout completion in old-version order A, B, C.
- Unequal A/B/C replica counts with `maxSkew: 10%`.
- Different user partitions across Roles.
- Unschedulable dependency with a visible blocker condition.
- Explicit Role subset coordination.
- Controller restart during delete, create, and readiness stages.

### Rollout Plan

The feature is enabled by configuring `roleCoordination` under `RoleRollingUpdate`. ModelServings without `roleCoordination` continue through the existing independent Role rolling-update path.

Implementation proceeds in four stages:

1. add the API, generated artifacts, and webhook validation;
2. add per-ServingGroup state construction and the pure coordination calculation;
3. integrate effective partitions, Ready-triggered continuation, and status reporting;
4. add unit, integration, and end-to-end coverage.

### References

- [Kthena Role rolling update proposal](https://github.com/volcano-sh/kthena/blob/main/docs/proposal/role-rollingupdate.md)
