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

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const rolloutVersionAnnotation = "modelserving.volcano.sh/rollout-version"

func rolloutVersion(ms *workloadv1alpha1.ModelServing) (string, bool) {
	value, exists := ms.Annotations[rolloutVersionAnnotation]
	return value, exists
}

// resolveModelServingRollout selects this reconcile's configuration and revision.
// An empty revision means the existing history code should select the target.
func (c *ModelServingController) resolveModelServingRollout(ctx context.Context, ms *workloadv1alpha1.ModelServing) (*workloadv1alpha1.ModelServing, string, error) {
	version, enabled := rolloutVersion(ms)
	if !enabled {
		return ms, "", nil
	}
	// Keep the execution object independent of the informer cache.
	ms = ms.DeepCopy()
	revision := ms.Status.UpdateRevision
	if revision == "" {
		if ms.Status.CurrentRevision != "" || ms.Status.ObservedGeneration != 0 {
			return nil, "", fmt.Errorf("accepted updateRevision is missing")
		}
		return ms, "", nil // Initial creation has no template to protect yet.
	}
	cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, revision)
	if err != nil {
		return nil, "", err
	}
	if cr == nil {
		return nil, "", fmt.Errorf("accepted ControllerRevision %s is missing", revision)
	}
	if err := validateControllerRevisionOwner(ms, cr); err != nil {
		return nil, "", err
	}
	previous, err := utils.ModelServingForControllerRevision(ms, cr)
	if err != nil {
		return nil, "", err
	}
	// Use main's existing normalized template comparison, which excludes replica counts.
	equal, err := utils.EqualModelServingRevisions("", "", previous, ms)
	if err != nil {
		return nil, "", err
	}
	if equal {
		return ms, revision, nil
	}
	recorded, hasRecord := cr.Annotations[rolloutVersionAnnotation]
	if hasRecord && version != recorded {
		return ms, "", nil // A changed version allows the requested template.
	}
	// Role names are immutable; restoration already retains each current Role replica count.
	c.emitRoleStatusEvent(ms, corev1.EventTypeWarning, "RolloutBlocked",
		"Keeping the accepted workload template until rollout-version changes; replica changes continue")
	return previous, revision, nil
}

// persistRolloutVersion records the selected target before starting new workload changes.
// Only annotations and status are written; the user's spec is never replaced.
func (c *ModelServingController) persistRolloutVersion(ctx context.Context, ms *workloadv1alpha1.ModelServing, revision string) error {
	version, enabled := rolloutVersion(ms)
	if !enabled {
		return nil
	}
	cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, revision)
	if err != nil {
		return err
	}
	if cr == nil {
		return fmt.Errorf("target ControllerRevision %s is missing", revision)
	}
	if err := validateControllerRevisionOwner(ms, cr); err != nil {
		return err
	}
	recorded, exists := cr.Annotations[rolloutVersionAnnotation]
	if !exists || recorded != version {
		updated := cr.DeepCopy()
		if updated.Annotations == nil {
			updated.Annotations = make(map[string]string)
		}
		updated.Annotations[rolloutVersionAnnotation] = version
		updated, err = c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		if round := coordinatedRoundOf(ctx, ms); round != nil {
			round.snapshots[revision] = controllerRevisionObservation{revision: updated}
		}
	}
	if ms.Status.UpdateRevision == revision {
		return nil
	}
	updated := ms.DeepCopy()
	updated.Status.UpdateRevision = revision
	c.revisionHistory(ctx, ms).copyCollisionCount(updated)
	stored, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	ms.Status = stored.Status
	ms.ResourceVersion = stored.ResourceVersion
	return nil
}
