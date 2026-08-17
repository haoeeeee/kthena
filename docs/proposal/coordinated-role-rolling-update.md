---
title: Coordinated Proportional Role Rolling Update
authors:
- TBD
reviewers:
- TBD
approvers:
- TBD

creation-date: 2026-08-17

---

## Coordinated Proportional Role Rolling Update

### Summary

Kthena supports `RoleRollingUpdate`, but the current controller calculates `partition` and `maxUnavailable` independently for every Role in every ServingGroup. If several cooperating Roles change in one ModelServing revision, a fast Role can advance much further than a slow Role. The controller also has no Role dependency graph, so an upstream Role may be replaced and become ready before the new revision of its downstream dependency is ready.

This proposal adds optional, per-ServingGroup coordination to `RoleRollingUpdate`. Users select the participating Roles, define a maximum percentage-point skew between their normalized ready progress, and optionally define a dependency DAG. The controller continues to use each Role's existing `partition` and `maxUnavailable` as local safety limits, then applies a second, cross-Role filter before deleting or creating a rollout candidate.

When dependencies are configured, a Role may not start its next update increment until its dependencies have reached the corresponding new-revision Ready progress. For `A -> B -> C`, the controller prepares C first, then B, then A. `maxSkew` prevents C from running arbitrarily far ahead while it has no new-version traffic. The feature remains event-driven and restart-safe: no durable wave number is required; every reconcile derives progress and in-flight reservations from Role state, Pod labels, and readiness.

### Motivation

Consider one ServingGroup containing three Roles:

```text
A depends on B
B depends on C
```

The application guarantees version-aware routing: new A calls new B, and new B calls new C. That guarantee alone does not make an uncoordinated rollout safe. In the first reconcile, the current Role rolling update can independently select one replica from A, B, and C. If new A and B become Ready before new C, traffic can reach new A and then fail because the required new C endpoint is not Ready.

Updating only C until completion is also undesirable. New C receives no new-version traffic before B advances, and a large excess of new C replicas may be idle. The required behavior is:

1. keep normalized new-version Ready progress approximately proportional across selected Roles;
2. honor dependency order when admitting new rollout work;
3. retain existing per-Role availability and partition semantics;
4. allow slow or failed dependencies to stop upstream progress safely.

The current implementation cannot provide these properties because `rolesToDeleteForRoleRollingUpdate` loops over Role specs and calculates each Role's `maxScaleDown` independently. `RoleRunning` is already a useful readiness signal: a Role replica becomes Running only after its entry Pod and all worker Pods are Running and Ready. This proposal builds coordination on that existing signal instead of introducing a separate readiness definition.

#### Goals

- Coordinate Role rolling updates independently inside each ServingGroup.
- Define progress as the percentage of update-eligible Role replicas that are both on the target Role template hash and `RoleRunning`.
- Limit the progress difference among selected active Roles with a user-supplied percentage such as `10%`.
- Allow users to define an acyclic Role dependency graph for rollout ordering.
- Require an upstream Role to wait until its dependencies reach the required new-version Ready progress.
- Preserve each Role's current `maxUnavailable` and `partition` behavior.
- Reserve in-flight work so asynchronous deletion, creation, scheduling, and readiness cannot oversubscribe the skew budget.
- Remain compatible with the current informer, workqueue, and repeated-reconcile architecture.
- Remain correct across controller restarts without persisting a wave counter.
- Define validation, status, events, metrics, failure behavior, and a complete test plan.

#### Non-Goals

- Implement business request routing or verify that new A actually calls only new B.
- Change initial ModelServing creation ordering. Dependencies in this proposal apply only to coordinated Role rolling updates.
- Coordinate rollout progress across different ServingGroups. Each ServingGroup has an independent coordinator.
- Replace the existing Role `maxUnavailable` or `partition` fields.
- Implement `maxSurge`. A future Role `maxSurge` implementation must feed its in-flight replicas into the same coordinator.
- Coordinate ordinary scaling, failure recovery, or Role addition/removal as proportional rolling-update work.
- Automatically roll back a revision when rollout progress stalls.
- Turn Pod readiness on or off, mutate readiness gates, or change Service endpoint selection.
- Guarantee runtime dependency health after an upstream replica has already completed rollout. Ordinary recovery policy handles later failures.

### Proposal

Coordination is opt-in and only valid with `rolloutStrategy.type: RoleRollingUpdate`.

```yaml
apiVersion: workload.serving.volcano.sh/v1alpha1
kind: ModelServing
metadata:
  name: example
spec:
  rolloutStrategy:
    type: RoleRollingUpdate
    roleRollingUpdateConfiguration:
      coordination:
        # Optional. Omission selects all Roles in spec.template.roles.
        roles: [a, b, c]

        # Percentage points of normalized rollout progress. Percent only.
        maxSkew: 10%

        # Rollout-only dependency graph. "a dependsOn b" means b must
        # reach the corresponding target progress before a can advance.
        dependencies:
        - role: a
          dependsOn: [b]
        - role: b
          dependsOn: [c]

  template:
    roles:
    - name: a
      replicas: 4
      maxUnavailable: 1
      partition: 0
      # templates omitted
    - name: b
      replicas: 4
      maxUnavailable: 1
      partition: 0
    - name: c
      replicas: 4
      maxUnavailable: 1
      partition: 0
```

The `roles` list scopes one coordination domain:

- omitted or empty: all Roles in `spec.template.roles`;
- specified: exactly those Roles participate in progress skew and dependency ordering;
- an unchanged selected Role is treated as complete for the current revision and does not block changed Roles;
- a non-selected Role retains the existing independent Role rollout behavior;
- a dependency endpoint must be in the selected set.

#### User Stories

##### Proportional PD rollout

A ServingGroup has 10 prefill and 20 decode Role replicas. Both templates change. With `maxSkew: 10%`, the controller prevents one Role from accumulating substantially more target-hash Ready capacity than the other while both still have rollout work.

##### Dependency-safe three-stage rollout

A, B, and C each have four replicas, `maxUnavailable: 1`, `maxSkew: 10%`, and dependencies A -> B -> C. Replica granularity makes the effective minimum step 25%. The first logical progression is:

```text
C-3 delete/recreate -> C-3 target hash and RoleRunning
B-3 delete/recreate -> B-3 target hash and RoleRunning
A-3 delete/recreate -> A-3 target hash and RoleRunning
```

Only then does the next C/B/A progression begin. New A cannot become available before the required progress of new B and C exists.

##### Different Role replica counts

A has 2 replicas, B has 4, and C has 10. One A replica represents 50% progress, so a strict 10-percentage-point bound is mathematically impossible once A advances. The controller reports an effective skew of 50% for this ServingGroup and prepares enough dependency progress before admitting A's first candidate. This is an explicit replica-granularity relaxation, not a raw-count interpretation of `maxSkew`.

##### Different partitions

A has four replicas with `partition: 2`; B and C have four replicas with no partition. A's rollout target contains only A-2 and A-3. Its denominator is therefore two, not four. Once those two replicas are updated and Ready, A's coordinated progress is 100%, while A-0 and A-1 intentionally remain old. B and C may still reach 100% of their own four-replica targets; A's partition does not implicitly partition B or C.

### API Design

```go
type RolloutStrategy struct {
    Type RolloutStrategyType `json:"type"`

    // Used only by ServingGroupRollingUpdate.
    RollingUpdateConfiguration *RollingUpdateConfiguration `json:"rollingUpdateConfiguration,omitempty"`

    // Used only by RoleRollingUpdate.
    RoleRollingUpdateConfiguration *RoleRollingUpdateConfiguration `json:"roleRollingUpdateConfiguration,omitempty"`
}

type RoleRollingUpdateConfiguration struct {
    Coordination *RoleRollingUpdateCoordination `json:"coordination,omitempty"`
}

type RoleRollingUpdateCoordination struct {
    // Empty means all Roles.
    Roles []string `json:"roles,omitempty"`

    // Required. Percentage strings only, for example "10%".
    MaxSkew *intstr.IntOrString `json:"maxSkew"`

    Dependencies []RoleRolloutDependency `json:"dependencies,omitempty"`
}

type RoleRolloutDependency struct {
    Role      string   `json:"role"`
    DependsOn []string `json:"dependsOn"`
}

```

Dependency ordering has one fixed meaning: a dependency's target-revision replica must be `RoleRunning` before its Ready progress can unlock a dependent. The API intentionally does not expose a weaker scheduled-only mode because it would allow new A or B to become Ready before new C and would not satisfy the safety requirement motivating this proposal.

#### Validation and defaulting

The webhook must enforce:

- `roleRollingUpdateConfiguration` is valid only for `RoleRollingUpdate`.
- `coordination.maxSkew` is required and must be a percentage string in `(0%, 100%]`; integer values are rejected.
- Role names in `roles`, `role`, and `dependsOn` exist in `spec.template.roles`.
- Names are unique; self-dependencies and duplicate dependency edges are rejected.
- The dependency graph is acyclic.
- Every dependency endpoint belongs to the effective selected Role set.
- At least two Roles are selected; a single Role provides no coordination benefit.
- Role-level `partition` remains less than or equal to Role replicas after percentage resolution.

Enabling coordination does not alter the historical default of an omitted Role `maxUnavailable`. Operators should configure it for every participating Role. The webhook should emit a warning when a selected Role omits it, because `maxSkew` coordinates relative progress but is not a substitute for a local availability budget.

### Progress Model

All calculations are performed independently for each ServingGroup and target ModelServing revision.

For participating Role `r`:

```text
D_r = desired Role replica count
P_r = resolved partition count
T_r = max(D_r - P_r, 0)              update-eligible target replicas
R_r = eligible replicas with target RoleTemplateHash and RoleRunning
I_r = eligible rollout work already reserved but not Ready

readyProgress_r    = R_r / T_r
reservedProgress_r = min(R_r + I_r, T_r) / T_r
```

`I_r` includes:

- Role replicas in `RoleDeleting` because of this rollout;
- target-hash Role replicas that are not `RoleRunning`;
- stable replacement slots missing between deletion completion and successful recreation;
- future target-revision surge replicas that have been admitted but are not Ready.

Counting in-flight work is essential. If the controller selected A, B, and C using only Ready progress, A and B could become Ready faster than C and exceed a decision made when all progress values were zero. Reservation prevents repeated reconciles from admitting more work than the coordination budget already committed.

Only ordinals greater than or equal to the resolved Role partition contribute to `T_r`, `R_r`, or `I_r`. Protected replicas remain available but are outside the rollout target.

If a selected Role's template did not change, it is complete for this rollout and is excluded from the active skew minimum. If `T_r` is zero for a changed dependency Role, the rollout is blocked with reason `DependencyHasNoEligibleReplica`; treating that Role as complete could violate new-to-new compatibility.

#### Percentage semantics

`maxSkew: 10%` means ten percentage points:

```text
abs(progress_a - progress_b) <= 0.10
```

It does not mean:

- ten replica instances;
- ten percent of the larger Role's raw replica count;
- a relative error such as `progress_a / progress_b`.

The implementation should use integer cross-multiplication or fixed-point basis points rather than floating-point comparison.

#### Replica granularity

A Role with `T_r` eligible replicas advances in increments of `1/T_r`. Exact percentage skew below the largest Role step cannot always be enforced.

Define:

```text
configuredSkew = parsed maxSkew
quantum        = max(1 / T_r) for active participating Roles
effectiveSkew  = max(configuredSkew, quantum)
```

For A/B/C with four eligible replicas each, `quantum = 25%`. A configured `10%` is therefore enforced as an effective 25% bound. For Roles with at least ten eligible replicas, a configured 10% bound is directly representable.

The controller must expose the relaxation through a condition/event and metric. It must not silently describe observed 25% skew as satisfying an exact 10% physical bound. The relaxation is at most the progress contribution of one replica of the coarsest Role.

### Dependency Semantics

An edge:

```yaml
- role: a
  dependsOn: [b]
```

means that B is a rollout prerequisite of A.

For a candidate that would advance A's reserved progress to `q`, every dependency B must have `readyProgress_B >= q`.

Comparison uses the normalized percentages, not matching ordinal numbers. A-3 does not have to pair with B-3. This is required because Roles may have different replica counts and partitions.

The dependency gate is checked when rollout work is admitted, before deleting the old upstream Role replica. It does not change Kubernetes Pod readiness. Consequently, the required downstream new-version capacity already exists before the upstream replacement can be created and become reachable.

For a DAG, candidates are tie-broken in reverse topological order: dependencies first, then dependents. Among candidates at the same topological level, the Role with the lowest reserved progress is preferred, followed by Role order in the ModelServing spec and descending Role ordinal.

### Reconcile Algorithm

The existing `syncModelServing` order remains conceptually unchanged:

```text
sync ServingGroup count
sync Role count and missing Pods
manage rolling update
sync Services
update status
```

Coordination changes only the Role rolling-update decision inside `manageRollingUpdate`.

#### Phase 1: build a snapshot

For each non-deleting ServingGroup:

1. Resolve selected Roles and validate that scaling has converged.
2. Calculate the target Role template hash for every Role.
3. Resolve per-Role partition and eligible ordinals.
4. Classify eligible replicas as old Ready, old unavailable, deleting, target Creating, or target Running.
5. Calculate `T`, `R`, `I`, Ready progress, reserved progress, configured skew, quantum, and effective skew.
6. Generate local outdated candidates using the existing `maxUnavailable` logic.

If Role replica counts are currently converging because of ordinary scaling, coordinated deletion is paused for that ServingGroup. Scaling and rolling should not compete over the same missing ordinals.

#### Phase 2: filter and select candidates

The coordinator repeatedly evaluates local candidates using a simulated reservation snapshot:

```text
selected = []

while candidates remain:
    eligible = candidates that satisfy all of:
      1. Role partition allows the ordinal
      2. Role maxUnavailable/local budget allows the candidate
      3. projected reserved progress <= min active Ready progress + effectiveSkew
      4. dependency Ready gate is satisfied

    if eligible is empty:
        break

    choose the candidate with:
      1. lowest projected/reserved Role progress
      2. dependency-first reverse topological priority
      3. Role order in spec
      4. highest replica ordinal

    append candidate to selected
    increment the simulated in-flight reservation for its Role
```

The simulated increment prevents one reconcile from selecting an unlimited number of replicas before informer events report `RoleDeleting`.

After selection, the existing `DeleteRole` path performs the actual delete. Deletion-completion events remove the logical Role from the datastore and enqueue the ModelServing. The next reconcile recreates the missing Role with the target hash. A target-hash non-Running Role consumes both the local `maxUnavailable` budget and the cross-Role reservation until it becomes `RoleRunning`.

#### Phase 3: event-driven continuation

The Pod handler already changes a Role to `RoleRunning` only after its entry Pod and all worker Pods are Running and Ready. Coordinated rollout additionally enqueues the ModelServing whenever a participating Role replica transitions to `RoleRunning`, not only when the whole ServingGroup becomes Running. This allows newly satisfied dependency and skew gates to be evaluated promptly.

The controller still does not wait inside reconcile:

```text
reconcile -> select/delete -> return
Pod/Service delete events -> enqueue
reconcile -> recreate -> return
Pod Ready events -> RoleRunning -> enqueue
reconcile -> select the next allowed work
```

There is no stored `wave=1` value. A "wave" is only an explanatory name for the set of work bounded by the current Ready frontier and reservations. After restart, Pod revision/hash labels, Role status reconstruction, desired counts, and missing slots reproduce the same constraints.

### Interaction with Existing Features

#### maxUnavailable

`maxUnavailable` remains a local availability constraint for one Role. Coordination is an additional global filter:

```text
final candidates = local maxUnavailable candidates
                 intersect partition-eligible candidates
                 intersect skew-allowed candidates
                 intersect dependency-allowed candidates
```

Coordination may reduce the number selected by the existing logic but never increases it.

#### partition

Partition is resolved separately for every Role and affects only that Role's denominator. A partition on A never stops B or C from completing their own rollout. Protected old replicas remain old after coordinated rollout completes.

#### future maxSurge

When Role `maxSurge` is implemented, a surge replica is rollout work at the moment the coordinator authorizes its creation. It must increment `I_r` immediately. The reservation consumes skew budget, but it cannot unlock a dependent until the surge Role becomes `RoleRunning`.

#### recovery policy

Intentional deletion continues to be identified by `RoleDeleting`, so it must not be treated as a failure-recovery event. A later failure after rollout admission follows the configured RecoveryPolicy. The coordinator does not retract already Ready upstream replicas.

#### new template changes during rollout

The latest ModelServing spec remains authoritative. A new target hash starts a new reconciliation target. Previously created replicas that no longer match become outdated again. Because no durable wave state exists, no wave migration is needed; status and events should report that the target revision changed.

### Failure and Blocking Behavior

#### Slow or failed dependency

If new C remains Creating:

- C consumes its local and reserved budget;
- B cannot advance until C reaches the required Ready progress;
- A cannot advance because B cannot advance;
- C cannot run farther ahead than the effective skew bound;
- old A and B replicas remain serving.

The rollout is intentionally stalled rather than exposing an incompatible upstream chain.

#### Unschedulable replicas

Unschedulable target replicas remain in-flight and do not release budget. Existing Kubernetes events explain scheduling failure; Kthena reports the rollout as waiting for Role readiness.

#### Dependency cycle or missing Role

These are admission errors and never reach the controller.

#### No eligible dependency replica

If a changed dependency is fully protected by partition, dependent rollout is blocked. The controller sets a condition and emits an event. It must not bypass the dependency silently.

#### Inconsistent or legacy hash data

The existing ControllerRevision fallback is used to infer missing Role template hashes. If a hash cannot be resolved, the coordinator conservatively treats the replica as not satisfying target Ready progress and reports the unresolved state.

### Status and Observability

The first implementation should keep the public status compact and use the existing `ModelServing.status.conditions` plus events and metrics.

Recommended condition reasons include:

- `CoordinatedRoleRolloutProgressing`
- `WaitingForDependencyReady`
- `WaitingForSkewBudget`
- `WaitingForRoleAvailability`
- `WaitingForRoleScaling`
- `DependencyHasNoEligibleReplica`
- `ReplicaGranularityRelaxedSkew`

Events should be emitted only on reason transitions or with rate limiting to avoid event storms.

Recommended metrics:

```text
kthena_modelserving_role_rollout_ready_progress_ratio
kthena_modelserving_role_rollout_reserved_progress_ratio
kthena_modelserving_role_rollout_target_replicas
kthena_modelserving_role_rollout_updated_ready_replicas
kthena_modelserving_role_rollout_inflight_replicas
kthena_modelserving_role_rollout_configured_max_skew_ratio
kthena_modelserving_role_rollout_effective_max_skew_ratio
kthena_modelserving_role_rollout_blocked
```

Labels should include namespace, ModelServing, ServingGroup, Role, and target revision where appropriate. A future status API may expose per-Role aggregate progress, but a high-cardinality ServingGroup-by-Role matrix should not be placed in the CR status without a size analysis.

The existing `status.updatedReplicas` remains ServingGroup-level and must not be documented as Role rollout progress.

### Controller Integration

The implementation should introduce a pure coordination component so algorithm tests do not require fake Kubernetes clients:

```go
type RoleRolloutSnapshot struct {
    GroupName       string
    TargetRevision  string
    Roles           map[string]RoleProgress
    Dependencies    DependencyGraph
    ConfiguredSkew  int32 // basis points
    EffectiveSkew   int32 // basis points
}

type RoleRolloutCoordinator interface {
    Select(snapshot RoleRolloutSnapshot, localCandidates []RoleCandidate) Decision
}

type Decision struct {
    Candidates []RoleCandidate
    Blocked    []BlockedReason
}
```

Expected code touch points:

- API types and generated clients/deepcopy/CRDs.
- ModelServing webhook validation/defaulting.
- `rolesToDeleteForRoleRollingUpdate`: build all Role candidates first instead of immediately concatenating independent selections.
- `outdatedRoles` and helpers: expose target Ready and in-flight classifications.
- `handleReadyPod`: enqueue on participating Role transition to Running.
- status condition, event, and metric reporting.

The existing `DeleteRole`, role recreation, ControllerRevision, Pod hash labels, and workqueue retry paths remain in use.

### Risks and Mitigations

#### Rollout throughput reduction

Ready-based dependency gating deliberately serializes dependency stages and can be slower than independent rollout. `maxUnavailable` and a representable `maxSkew` remain the throughput tuning controls. Safety is not optional when dependencies are configured.

#### Deadlock from discrete replica counts

Strict percentage inequalities can be impossible for small Roles. The effective-skew rule provides one-replica granularity and reports the relaxation. Pure coordinator property tests cover mixed replica counts.

#### Stale informer observations

Candidate admission uses conservative in-flight reservations and simulated increments. Operations are idempotent, and RoleDeleting/target-Creating/missing replacement slots remain reserved across reconciles.

#### Misconfigured dependency direction

API documentation states that `A dependsOn B` means B must advance first. Webhook DAG validation, status messages, and examples reduce ambiguity.

#### Status or metric cardinality

Detailed per-group state is kept in metrics rather than an unbounded CR status list in the initial version.

#### Feature interaction complexity

The coordinator only filters candidates that already passed existing Role update rules. It does not own deletion, creation, readiness, or recovery.

### Test Plan

#### Unit tests

- Percent parsing and rejection of absolute `maxSkew`.
- Role selection defaulting and explicit subset behavior.
- Dependency existence, duplicate, self-edge, and cycle validation.
- Progress calculation with target hash, `RoleRunning`, Creating, Deleting, and missing replacement slots.
- Per-Role partition denominator and different partitions across Roles.
- Unchanged selected Roles and fully partitioned changed dependencies.
- Effective skew for equal and unequal replica counts.
- Ready-based dependency gating at equal and unequal replica counts.
- Candidate simulation prevents over-selection in one reconcile.
- Deterministic selection order and descending ordinal behavior.
- Existing `maxUnavailable` always remains an upper bound.
- Controller restart snapshot produces the same decision.

#### Property and fuzz tests

For random DAGs, replica counts, partitions, Ready states, and local budgets:

- selected candidates never violate local availability or partition;
- projected reservations never exceed effective skew;
- dependency gating never admits a dependent beyond dependency Ready progress;
- selection is deterministic for the same snapshot;
- repeated Ready transitions eventually complete when all creations succeed;
- invalid graphs are always rejected.

#### Integration tests

- Spec update event enqueues a ModelServing and selects only dependency-allowed work.
- Delete events preserve reservation through deletion/recreation gaps.
- each participating RoleRunning transition enqueues reconciliation.
- rate-limited retries do not duplicate deletions.
- controller restart during Deleting and Creating reconstructs reservations.
- a second template update during rollout switches target hashes safely.

#### End-to-end tests

- A/B/C equal replicas, delayed C readiness: B and A do not start early.
- Different replica counts with configured skew below replica quantum.
- Mixed partitions: A partially remains old while B/C complete.
- One Role unschedulable: upstream stops and old capacity remains.
- Explicit Role subset: selected Roles coordinate; unselected Role preserves legacy behavior.
- Future `maxSurge` compatibility test when that feature lands.

### Rollout Plan

1. Add the alpha API behind a `CoordinatedRoleRollingUpdate` feature gate.
2. Add validation, pure progress calculation, and dry-run decision logging/metrics.
3. Enable candidate filtering in unit and integration environments.
4. Add E2E coverage with injected readiness delays.
5. Enable by default only after skew, restart, and dependency-stall behavior are proven.

When the feature gate is disabled or `coordination` is absent, behavior is byte-for-byte compatible at the API level and logically compatible with the existing independent Role rolling update.

### Alternatives

#### Rely only on business-layer version routing

Rejected as the controller can still make a new upstream Role reachable before any compatible downstream replica is Ready. Routing cannot select a nonexistent endpoint.

#### Use only dependency order

The invariant `progress_A <= progress_B <= progress_C` prevents upstream from leading, but by itself allows C to reach 100% while A remains at 0%. `maxSkew` is still needed to bound idle downstream progress.

#### Use only maxSkew with simultaneous candidate admission

Rejected because all Roles can be at zero when A, B, and C are admitted simultaneously. Different readiness latency can then expose A before C. The dependency Ready gate must constrain admission.

#### Define skew as a raw replica-count difference

Rejected because Roles commonly have different desired counts. A one-replica difference means 50% for a two-replica Role but 1% for a hundred-replica Role.

#### Persist explicit wave numbers

Rejected because the controller can derive the current frontier from target hash, Ready state, and in-flight work. A persisted wave adds recovery and spec-change complexity without improving correctness.

#### Mutate Pod readiness until dependencies are Ready

Rejected for the initial design. It couples rollout control to kubelet readiness and Service endpoint semantics, can hide otherwise healthy Pods, and still needs a policy for already Ready Pods when a dependency later fails. Candidate admission prevents the unsafe state earlier.

### References

- [Kthena Role rolling update proposal](https://github.com/volcano-sh/kthena/blob/main/docs/proposal/role-rollingupdate.md)
