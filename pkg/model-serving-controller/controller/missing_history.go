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
	"slices"
	"strings"

	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const rolloutFromRevisionsAnnotation = "modelserving.volcano.sh/rollout-from-revisions"
const rolloutFromRolesAnnotation = "modelserving.volcano.sh/rollout-from-roles"

func (h *revisionHistory) selectTarget(ctx context.Context, target string) (string, error) {
	h.target = target
	if err := h.recordMissingRevisionRollout(ctx, target); err != nil {
		return "", err
	}
	return target, nil
}

// Record only missing revisions observed when a persisted previous target proves
// a real template change. Save before advancing status so retries and restarts
// retain that decision. Never fabricate the missing revision's template.
func (h *revisionHistory) recordMissingRevisionRollout(ctx context.Context, target string) error {
	previous := h.ms.Status.UpdateRevision
	if previous == "" || previous == target {
		return nil
	}
	oldRoles, err := h.roles(ctx, previous)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if utils.EqualRoleTemplatesForRevision(oldRoles, h.ms.Spec.Template.Roles) {
		return nil
	}
	groups, err := h.controller.store.GetServingGroupByModelServing(utils.GetNamespaceName(h.ms))
	if errors.Is(err, datastore.ErrServingGroupNotFound) {
		groups = nil
	} else if err != nil {
		return err
	}

	observedRevisions := map[string]bool{}
	for _, group := range groups {
		observedRevisions[group.Revision] = true
		roles, err := h.controller.store.GetRolesByGroup(utils.GetNamespaceName(h.ms), group.Name)
		if err != nil {
			return err
		}
		for _, instances := range roles {
			for _, role := range instances {
				observedRevisions[role.Revision] = true
			}
		}
	}
	missing := map[string]bool{}
	for revision := range observedRevisions {
		if revision == "" {
			continue
		}
		_, err := h.controller.kubeClientSet.AppsV1().ControllerRevisions(h.ms.Namespace).Get(ctx, utils.GenerateControllerRevisionName(h.ms.Name, revision), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			missing[revision] = true
		} else if err != nil {
			return err
		}
	}
	if len(missing) == 0 {
		return nil
	}
	cr, err := utils.CreateControllerRevision(ctx, h.controller.kubeClientSet, h.controller.controllerRevisionLister, h.ms, target, h.ms.Spec.Template.Roles)
	if err != nil {
		return err
	}
	// A revision-level decision is sufficient for ServingGroup rollout, but
	// Role rollout may only replace roles whose templates actually changed.
	roleDecisions := map[string]bool{}
	for _, entry := range strings.Split(cr.Annotations[rolloutFromRolesAnnotation], ",") {
		if entry != "" {
			roleDecisions[entry] = true
		}
	}
	for _, desired := range h.ms.Spec.Template.Roles {
		unchanged := false
		for _, old := range oldRoles {
			if old.Name == desired.Name && utils.EqualRoleTemplateForRevision(old, desired) {
				unchanged = true
				break
			}
		}
		if !unchanged {
			for revision := range missing {
				roleDecisions[revision+"/"+desired.Name] = true
			}
		}
	}
	roleEntries := make([]string, 0, len(roleDecisions))
	for entry := range roleDecisions {
		roleEntries = append(roleEntries, entry)
	}
	slices.Sort(roleEntries)
	roleValue := strings.Join(roleEntries, ",")
	for _, revision := range strings.Split(cr.Annotations[rolloutFromRevisionsAnnotation], ",") {
		if revision != "" {
			missing[revision] = true
		}
	}
	revisions := make([]string, 0, len(missing))
	for revision := range missing {
		revisions = append(revisions, revision)
	}
	slices.Sort(revisions)
	value := strings.Join(revisions, ",")
	if cr.Annotations[rolloutFromRevisionsAnnotation] == value && cr.Annotations[rolloutFromRolesAnnotation] == roleValue {
		return nil
	}
	if cr.Annotations == nil {
		cr.Annotations = map[string]string{}
	}
	cr.Annotations[rolloutFromRevisionsAnnotation] = value
	cr.Annotations[rolloutFromRolesAnnotation] = roleValue
	updated, err := h.controller.kubeClientSet.AppsV1().ControllerRevisions(h.ms.Namespace).Update(ctx, cr, metav1.UpdateOptions{})
	if err == nil {
		h.snapshots[target] = h.decode(updated)
	}
	return err
}

// Reading a recovery decision must not select or persist a target as a side
// effect of rollout/status comparison.
func (h *revisionHistory) canReplaceMissing(ctx context.Context, revision, role string) bool {
	for _, target := range []string{h.target, h.ms.Status.UpdateRevision, utils.ModelServingRevision(h.ms)} {
		if target == "" {
			continue
		}
		snapshot, exists := h.snapshots[target]
		if !exists {
			cr, err := utils.GetControllerRevision(ctx, h.controller.kubeClientSet, h.ms, target)
			if err != nil || cr == nil {
				continue
			}
			snapshot = h.decode(cr)
			h.snapshots[target] = snapshot
		}
		cr := snapshot.revision
		if snapshot.err != nil || cr == nil || !utils.EqualRoleTemplatesForRevision(snapshot.roles, h.ms.Spec.Template.Roles) {
			continue
		}
		if !slices.Contains(strings.Split(cr.Annotations[rolloutFromRevisionsAnnotation], ","), revision) {
			continue
		}
		return role == "" || slices.Contains(strings.Split(cr.Annotations[rolloutFromRolesAnnotation], ","), revision+"/"+role)
	}
	return false
}
