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
	"errors"
	"fmt"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const rolloutVersionAnnotation = "modelserving.volcano.sh/rollout-version"

const (
	rolloutBlockedCondition = "RolloutBlocked"
	rolloutBlockedMessage   = "Keeping the accepted workload template until rollout-version changes; replica changes continue"
)

func rolloutVersion(ms *workloadv1alpha1.ModelServing) (string, bool) {
	value, exists := ms.Annotations[rolloutVersionAnnotation]
	return value, exists
}

func rolloutVersionChanged(oldMS, newMS *workloadv1alpha1.ModelServing) bool {
	oldVersion, oldProtected := rolloutVersion(oldMS)
	newVersion, newProtected := rolloutVersion(newMS)
	return oldVersion != newVersion || oldProtected != newProtected
}

type rolloutBlockedKey struct{}

// guardModelServingRollout selects the protected configuration and carries its decision to status.
func (c *ModelServingController) guardModelServingRollout(ctx context.Context, ms *workloadv1alpha1.ModelServing) (context.Context, *workloadv1alpha1.ModelServing, string, error) {
	selected, revision, blocked, err := c.resolveModelServingRollout(ctx, ms)
	if err != nil {
		return ctx, nil, "", errors.Join(fmt.Errorf("resolve rollout protection: %w", err), c.scaleDownOnRevisionError(ctx, ms))
	}
	return context.WithValue(ctx, rolloutBlockedKey{}, blocked), selected, revision, nil
}

// resolveModelServingRollout selects this reconcile's configuration and revision.
// An empty revision lets the existing history resolver select the target.
func (c *ModelServingController) resolveModelServingRollout(ctx context.Context, ms *workloadv1alpha1.ModelServing) (*workloadv1alpha1.ModelServing, string, bool, error) {
	version, enabled := rolloutVersion(ms)
	if !enabled {
		return ms, "", false, nil
	}
	revision := ms.Status.UpdateRevision
	if revision == "" {
		if ms.Status.CurrentRevision != "" || ms.Status.ObservedGeneration != 0 {
			return nil, "", false, fmt.Errorf("accepted updateRevision is missing")
		}
		return ms, "", false, nil // Initial creation has no template to protect yet.
	}
	cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, revision)
	if err != nil {
		return nil, "", false, err
	}
	if cr == nil {
		return nil, "", false, fmt.Errorf("accepted ControllerRevision %s is missing", revision)
	}
	roles, err := utils.GetRolesFromControllerRevision(cr)
	if err != nil {
		return nil, "", false, err
	}
	if len(roles) == 0 {
		return nil, "", false, fmt.Errorf("accepted ControllerRevision %s contains no Roles", revision)
	}
	if utils.EqualRoleTemplatesForRevision(roles, ms.Spec.Template.Roles) {
		return ms, revision, false, nil
	}
	recorded, hasRecord := cr.Annotations[rolloutVersionAnnotation]
	if !hasRecord || version != recorded {
		return ms, "", false, nil
	}

	// Restore only revisioned Role fields. Counts and rollout policy stay current;
	// immutable scheduler/plugin inputs remain sourced from the owning MS.
	templates := make(map[string]workloadv1alpha1.Role, len(roles))
	for _, role := range roles {
		templates[role.Name] = role
	}
	guarded := ms.DeepCopy()
	for i := range guarded.Spec.Template.Roles {
		role := &guarded.Spec.Template.Roles[i]
		historical, exists := templates[role.Name]
		if !exists {
			return nil, "", false, fmt.Errorf("Role %s is missing from accepted ControllerRevision %s", role.Name, revision)
		}
		role.EntryTemplate = *historical.EntryTemplate.DeepCopy()
		role.WorkerReplicas = historical.WorkerReplicas
		role.WorkerTemplate = historical.WorkerTemplate.DeepCopy()
	}
	c.emitRoleStatusEvent(ms, corev1.EventTypeWarning, rolloutBlockedCondition, rolloutBlockedMessage)
	return guarded, revision, true, nil
}

func updateRolloutBlockedCondition(ctx context.Context, ms *workloadv1alpha1.ModelServing) bool {
	blocked, decided := ctx.Value(rolloutBlockedKey{}).(bool)
	if !decided {
		return false
	}
	if !blocked && apiMeta.FindStatusCondition(ms.Status.Conditions, rolloutBlockedCondition) == nil {
		return false
	}
	condition := metav1.Condition{
		Type:               rolloutBlockedCondition,
		Status:             metav1.ConditionFalse,
		Reason:             "RolloutAllowed",
		Message:            "No template change is blocked by rollout protection",
		ObservedGeneration: ms.Generation,
	}
	if blocked {
		condition.Status = metav1.ConditionTrue
		condition.Reason = "RolloutVersionUnchanged"
		condition.Message = rolloutBlockedMessage
	}
	return apiMeta.SetStatusCondition(&ms.Status.Conditions, condition)
}

// saveRolloutProtectionState saves the selected target before normal expansion and
// rollout. Only revision metadata and MS status are written, never the MS spec.
func (c *ModelServingController) saveRolloutProtectionState(ctx context.Context, ms *workloadv1alpha1.ModelServing, revision string) error {
	version, enabled := rolloutVersion(ms)
	// Unprotected reconciliation already resolved this snapshot. Clearing its
	// marker makes a later enablement start without a stale protection record.
	cr := c.revisionHistory(ctx, ms).snapshots[revision].revision
	if enabled || cr == nil {
		var err error
		cr, err = utils.GetControllerRevision(ctx, c.kubeClientSet, ms, revision)
		if err != nil {
			return err
		}
	}
	if cr == nil {
		if !enabled {
			return nil
		}
		return fmt.Errorf("target ControllerRevision %s is missing", revision)
	}
	recorded, exists := cr.Annotations[rolloutVersionAnnotation]
	if (enabled && (!exists || recorded != version)) || (!enabled && exists) {
		updated := cr.DeepCopy()
		if enabled {
			if updated.Annotations == nil {
				updated.Annotations = make(map[string]string)
			}
			updated.Annotations[rolloutVersionAnnotation] = version
		} else {
			delete(updated.Annotations, rolloutVersionAnnotation)
		}
		if _, err := c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	if !enabled || ms.Status.UpdateRevision == revision {
		return nil
	}
	updated := ms.DeepCopy()
	updated.Status.UpdateRevision = revision
	stored, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	ms.Status = stored.Status
	ms.ResourceVersion = stored.ResourceVersion
	return nil
}
