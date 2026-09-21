/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

type templateComparison int

const (
	templateUnknown templateComparison = iota
	templateEquivalent
	templateDifferent
)

type revisionSnapshot struct {
	roles    []workloadv1alpha1.Role
	err      error
	revision *appsv1.ControllerRevision
}

// revisionHistory is a read-only interpretation of persisted identities. Its
// cache belongs to one reconcile, not the controller process: a missing history
// can be retried next time, and no adoption state is lost on leader changes.
type revisionHistory struct {
	controller  *ModelServingController
	ms          *workloadv1alpha1.ModelServing
	snapshots   map[string]revisionSnapshot
	target      string
	roleHashes  map[*workloadv1alpha1.ModelServing]map[string]string
	comparisons map[roleComparisonKey]templateComparison
}

// Include the desired object: recovery also compares against an older stable
// template using a copy with the same UID, generation and resource version.
type roleComparisonKey struct {
	desired  *workloadv1alpha1.ModelServing
	revision string
	roleName string
	target   string
}

func (h *revisionHistory) desiredRoleHash(ms *workloadv1alpha1.ModelServing, role workloadv1alpha1.Role) string {
	if h.roleHashes == nil {
		h.roleHashes = make(map[*workloadv1alpha1.ModelServing]map[string]string)
	}
	if h.roleHashes[ms] == nil {
		h.roleHashes[ms] = make(map[string]string)
	}
	if hash, ok := h.roleHashes[ms][role.Name]; ok {
		return hash
	}
	hash := utils.CalRoleTemplateHash(role)
	h.roleHashes[ms][role.Name] = hash
	return hash
}

// Matching identities remain a fast path. A different hash is never evidence
// of a different template: interpret this instance's own immutable history and
// compare the projected specs using Kubernetes semantic equality.
func (h *revisionHistory) compareObservedRole(ctx context.Context, ms *workloadv1alpha1.ModelServing, group datastore.ServingGroup, roleName string, observed datastore.Role) templateComparison {
	var desired *workloadv1alpha1.Role
	for i := range ms.Spec.Template.Roles {
		if ms.Spec.Template.Roles[i].Name == roleName {
			desired = &ms.Spec.Template.Roles[i]
			break
		}
	}
	if desired == nil {
		return templateDifferent
	}
	if observed.RoleTemplateHash == h.desiredRoleHash(ms, *desired) {
		return templateEquivalent
	}
	revision := observed.Revision
	if revision == "" {
		revision = group.Revision
	}
	key := roleComparisonKey{desired: ms, revision: revision, roleName: roleName, target: h.target}
	if result, ok := h.comparisons[key]; ok {
		return result
	}
	result := templateUnknown
	roles, err := h.roles(ctx, revision)
	if err == nil {
		for _, historical := range roles {
			if historical.Name == roleName {
				result = templateDifferent
				if utils.EqualRoleTemplatesForRevision([]workloadv1alpha1.Role{historical}, []workloadv1alpha1.Role{*desired}) {
					result = templateEquivalent
				}
				break
			}
		}
	} else if apierrors.IsNotFound(err) && h.canReplaceMissing(ctx, revision, roleName) {
		result = templateDifferent
	}
	if h.comparisons == nil {
		h.comparisons = make(map[roleComparisonKey]templateComparison)
	}
	h.comparisons[key] = result
	return result
}

// A new worker cannot promote an entire old Role to the target. Pod observations
// are checked on every call; only immutable historical comparisons are cached.
func (c *ModelServingController) compareRoleTemplate(ctx context.Context, ms *workloadv1alpha1.ModelServing, group datastore.ServingGroup, roleName string, role datastore.Role) templateComparison {
	history := c.revisionHistory(ctx, ms)
	result := history.compareObservedRole(ctx, ms, group, roleName, role)
	if result != templateEquivalent || (c.podsInformer == nil && c.observation == nil) {
		return result
	}
	pods, err := c.getPodsByIndex(RoleIDKey, fmt.Sprintf("%s/%s/%s/%s", ms.Namespace, group.Name, roleName, role.Name))
	if err != nil {
		return templateUnknown
	}
	for _, pod := range pods {
		if !utils.IsOwnedByModelServingWithUID(pod, ms.UID) {
			continue
		}
		comparison := history.compareObservedRole(ctx, ms, group, roleName, datastore.Role{Revision: utils.ObjectRevision(pod), RoleTemplateHash: utils.ObjectRoleTemplateHash(pod)})
		if comparison == templateDifferent {
			return templateDifferent
		}
		if comparison == templateUnknown {
			result = templateUnknown
		}
	}
	return result
}

type revisionHistoryKey struct{}

func (c *ModelServingController) withRevisionHistory(ctx context.Context, ms *workloadv1alpha1.ModelServing) context.Context {
	return context.WithValue(ctx, revisionHistoryKey{}, c.revisionHistory(ctx, ms))
}

func (c *ModelServingController) revisionHistory(ctx context.Context, ms *workloadv1alpha1.ModelServing) *revisionHistory {
	if history, ok := ctx.Value(revisionHistoryKey{}).(*revisionHistory); ok &&
		history.ms.UID == ms.UID && history.ms.Namespace == ms.Namespace && history.ms.Name == ms.Name &&
		history.ms.ResourceVersion == ms.ResourceVersion {
		return history
	}
	return &revisionHistory{controller: c, ms: ms, snapshots: make(map[string]revisionSnapshot)}
}

func (h *revisionHistory) decode(cr *appsv1.ControllerRevision) revisionSnapshot {
	if cr == nil {
		return revisionSnapshot{err: fmt.Errorf("ControllerRevision is missing")}
	}
	if !metav1.IsControlledBy(cr, h.ms) {
		return revisionSnapshot{err: fmt.Errorf("ControllerRevision %s is not controlled by the current ModelServing UID", cr.Name)}
	}
	roles, err := utils.GetRolesFromControllerRevision(cr)
	if err == nil && len(roles) == 0 {
		err = fmt.Errorf("ControllerRevision %s contains no Roles", cr.Name)
	}
	return revisionSnapshot{roles: roles, err: err, revision: cr}
}

func (h *revisionHistory) roles(ctx context.Context, revision string) ([]workloadv1alpha1.Role, error) {
	if snapshot, ok := h.snapshots[revision]; ok {
		return snapshot.roles, snapshot.err
	}
	snapshot := revisionSnapshot{err: fmt.Errorf("revision or Kubernetes client is missing")}
	if revision != "" && h.controller.kubeClientSet != nil {
		cr, err := utils.GetControllerRevision(ctx, h.controller.kubeClientSet, h.ms, revision)
		if err != nil {
			snapshot.err = err
		} else if cr == nil {
			snapshot.err = apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "controllerrevisions"}, utils.GenerateControllerRevisionName(h.ms.Name, revision))
		} else {
			snapshot = h.decode(cr)
		}
	}
	if snapshot.err != nil {
		snapshot.err = &revisionResolutionError{snapshot.err}
	}
	h.snapshots[revision] = snapshot
	if snapshot.err != nil {
		h.controller.enqueueModelServingAfter(h.ms, 5*time.Second)
		klog.Warningf("Cannot resolve revision %q for ModelServing %s/%s; template-driven deletion is paused for affected replicas: %v",
			revision, h.ms.Namespace, h.ms.Name, snapshot.err)
		if h.controller.recorder != nil {
			h.controller.recorder.Eventf(h.ms, corev1.EventTypeWarning, "RevisionUnresolved",
				"Cannot resolve revision %q; template-driven deletion is paused for affected replicas: %v", revision, snapshot.err)
		}
	}
	return snapshot.roles, snapshot.err
}

// role resolves a replica's recorded template. Missing history must not turn
// into permission to render the current spec under an older revision label.
func (h *revisionHistory) role(ctx context.Context, revision, name string) (workloadv1alpha1.Role, error) {
	roles, err := h.roles(ctx, revision)
	if err != nil {
		return workloadv1alpha1.Role{}, err
	}
	for _, role := range roles {
		if role.Name == name {
			return *role.DeepCopy(), nil
		}
	}
	return workloadv1alpha1.Role{}, &revisionResolutionError{fmt.Errorf("Role %s not found in ControllerRevision %s", name, revision)}
}

// desiredRevision keeps an existing identity when its immutable template is
// equivalent, even if a newer binary calculates a different hash. The current
// status wins deterministically; otherwise use the newest equivalent history.
// Legacy snapshots contain Roles only, so this does not infer historical
// scheduler/plugin configuration or change the revision-data format.
func (h *revisionHistory) desiredRevision(ctx context.Context) (string, error) {
	list, err := h.controller.kubeClientSet.AppsV1().ControllerRevisions(h.ms.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{utils.ControllerRevisionLabelKey: h.ms.Name}).String(),
	})
	if err != nil {
		return "", fmt.Errorf("list ControllerRevisions: %w", err)
	}
	priority := func(cr appsv1.ControllerRevision) int {
		revision := cr.Labels[utils.ControllerRevisionRevisionLabelKey]
		if revision == h.ms.Status.UpdateRevision {
			return 2
		}
		if revision == h.ms.Status.CurrentRevision {
			return 1
		}
		return 0
	}
	sort.Slice(list.Items, func(i, j int) bool {
		a, b := list.Items[i], list.Items[j]
		if priority(a) != priority(b) {
			return priority(a) > priority(b)
		}
		if a.Revision != b.Revision {
			return a.Revision > b.Revision
		}
		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			return b.CreationTimestamp.Before(&a.CreationTimestamp)
		}
		return a.Name < b.Name
	})
	for i := range list.Items {
		cr := &list.Items[i]
		revision := cr.Labels[utils.ControllerRevisionRevisionLabelKey]
		if revision == "" || cr.Name != utils.GenerateControllerRevisionName(h.ms.Name, revision) {
			continue
		}
		snapshot := h.decode(cr)
		if snapshot.err == nil {
			h.snapshots[revision] = snapshot
			if utils.EqualRoleTemplatesForRevision(snapshot.roles, h.ms.Spec.Template.Roles) {
				return h.selectTarget(ctx, revision)
			}
		}
	}
	revision := utils.ModelServingRevision(h.ms)
	cr, err := utils.CreateControllerRevision(ctx, h.controller.kubeClientSet, h.ms, revision, h.ms.Spec.Template.Roles)
	if err != nil {
		return "", fmt.Errorf("persist desired ControllerRevision: %w", err)
	}
	snapshot := h.decode(cr)
	if snapshot.err != nil {
		return "", fmt.Errorf("resolve persisted desired ControllerRevision: %w", snapshot.err)
	}
	h.snapshots[revision] = snapshot
	return h.selectTarget(ctx, revision)
}

func (c *ModelServingController) compareServingGroupTemplate(ctx context.Context, ms *workloadv1alpha1.ModelServing, group datastore.ServingGroup, targetRevision string) templateComparison {
	history := c.revisionHistory(ctx, ms)
	ctx = context.WithValue(ctx, revisionHistoryKey{}, history)
	rolesByName, err := c.store.GetRolesByGroup(utils.GetNamespaceName(ms), group.Name)
	if err != nil {
		return templateUnknown
	}
	roleRollingUpdate := ms.Spec.RolloutStrategy != nil && ms.Spec.RolloutStrategy.Type == workloadv1alpha1.RoleRollingUpdate
	remaining := make(map[string]workloadv1alpha1.Role, len(ms.Spec.Template.Roles))
	for _, role := range ms.Spec.Template.Roles {
		// RoleRollingUpdate compares the templates carried by live Role
		// replicas. A zero-replica Role has no target instance to replace or
		// observe, so its historical template must not keep the group outdated.
		// Any old instance that still exists is handled below as unexpected
		// live capacity and continues to block completion until it disappears.
		if roleRollingUpdate && roleReplicas(role) == 0 {
			continue
		}
		remaining[role.Name] = role
	}
	// Empty groups have no per-Role observation yet. Their recorded revision
	// still describes the intended template; readiness is evaluated separately.
	if len(rolesByName) == 0 {
		if roleRollingUpdate && len(remaining) == 0 {
			return templateEquivalent
		}
		if group.Revision != "" && (group.Revision == targetRevision || group.Revision == utils.ModelServingRevision(ms)) {
			return templateEquivalent
		}
		roles, err := history.roles(ctx, group.Revision)
		if err != nil {
			if apierrors.IsNotFound(err) && history.canReplaceMissing(ctx, group.Revision, "") {
				return templateDifferent
			}
			return templateUnknown
		}
		if utils.EqualRoleTemplatesForRevision(roles, ms.Spec.Template.Roles) {
			return templateEquivalent
		}
		return templateDifferent
	}
	result := templateEquivalent
	for roleName, roles := range rolesByName {
		// DeleteRole removes the last instance but deliberately retains the
		// per-Role map. An empty map is therefore absence, not an unexpected
		// Role template.
		if len(roles) == 0 {
			continue
		}
		_, exists := remaining[roleName]
		if !exists {
			return templateDifferent
		}
		delete(remaining, roleName)
		for _, role := range roles {
			comparison := c.compareRoleTemplate(ctx, ms, group, roleName, *role)
			if comparison == templateUnknown {
				result = templateUnknown
			} else if comparison == templateDifferent {
				return templateDifferent
			}
		}
	}
	// Missing Role instances are not themselves a template change. Resolve their
	// intended template without requiring the desired replica count or ordinals.
	if len(remaining) > 0 {
		roles, err := history.roles(ctx, group.Revision)
		if err != nil {
			return templateUnknown
		}
		for _, role := range roles {
			if expected, exists := remaining[role.Name]; exists {
				if !utils.EqualRoleTemplatesForRevision([]workloadv1alpha1.Role{role}, []workloadv1alpha1.Role{expected}) {
					return templateDifferent
				}
				delete(remaining, role.Name)
			}
		}
		if len(remaining) > 0 {
			return templateDifferent
		}
	}
	return result
}

// revisionForServingGroup resolves the recovery template without rewriting the
// group's observed identity. After a completed independent Role rollout, a live
// group's original label may predate the current stable template. Only select
// CurrentRevision when the actual Role observations prove it is equivalent.
func (c *ModelServingController) revisionForServingGroup(ctx context.Context, ms *workloadv1alpha1.ModelServing, group datastore.ServingGroup) string {
	current := ms.Status.CurrentRevision
	if current == "" || current == group.Revision {
		return group.Revision
	}
	ctx = c.withRevisionHistory(ctx, ms)
	roles, err := c.revisionHistory(ctx, ms).roles(ctx, current)
	if err != nil {
		return group.Revision
	}
	stable := ms.DeepCopy()
	stable.Spec.Template.Roles = roles
	if c.compareServingGroupTemplate(ctx, stable, group, current) == templateEquivalent {
		return current
	}
	return group.Revision
}

// Revision failures pause only operations that require the affected template.
// Plugin/API mutation failures remain hard errors in the reconciliation path.
type revisionResolutionError struct{ err error }

func (e *revisionResolutionError) Error() string { return e.err.Error() }
func (e *revisionResolutionError) Unwrap() error { return e.err }
func isRevisionResolutionError(err error) bool {
	switch e := err.(type) {
	case *revisionResolutionError:
		return true
	case interface{ Unwrap() []error }:
		nested := e.Unwrap()
		if len(nested) == 0 {
			return false
		}
		for _, cause := range nested {
			if !isRevisionResolutionError(cause) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return isRevisionResolutionError(e.Unwrap())
	default:
		return false
	}
}
