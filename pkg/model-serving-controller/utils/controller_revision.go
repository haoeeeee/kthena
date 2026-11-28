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

package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/klog/v2"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
)

const (
	// ControllerRevisionLabelKey is the label key for ModelServing name
	ControllerRevisionLabelKey = workloadv1alpha1.ModelServingNameLabelKey
	// ControllerRevisionRevisionLabelKey is the label key for revision
	ControllerRevisionRevisionLabelKey = workloadv1alpha1.RevisionLabelKey
	// ControllerRevisionDataVersionAnnotation identifies the canonical revision
	// data format introduced for stable revision history and rollback.
	ControllerRevisionDataVersionAnnotation = "modelserving.volcano.sh/revision-data-version"
	// ControllerRevisionDataVersionV1 is the current revision data format.
	ControllerRevisionDataVersionV1 = "v1"
)

// CreateControllerRevision maintains the legacy wrapped Role revision format
// used by the current controller integration. New v1 revision paths must use
// BuildRevisionData and RecordModelServingRevision so revision data remains
// immutable. New snapshots use the highest owned history number plus one;
// reusing an existing snapshot preserves its number and data.
func CreateControllerRevision(ctx context.Context, client kubernetes.Interface, revisions appslisters.ControllerRevisionLister, ms *workloadv1alpha1.ModelServing, revision string, templateData interface{}) (*appsv1.ControllerRevision, error) {
	// Serialize template data
	// Wrap data in a map to ensure it's a valid JSON object (Kubernetes requirement for RawExtension)
	wrappedData := map[string]interface{}{
		"data": templateData,
	}
	data, err := json.Marshal(wrappedData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal template data: %v", err)
	}

	// Check if ControllerRevision already exists
	controllerRevisionName := GenerateControllerRevisionName(ms.Name, revision)
	validateExisting := func(existing *appsv1.ControllerRevision) (*appsv1.ControllerRevision, error) {
		if !metav1.IsControlledBy(existing, ms) {
			return nil, fmt.Errorf("ControllerRevision %s/%s is not controlled by the current ModelServing UID", ms.Namespace, controllerRevisionName)
		}
		// A revision name identifies immutable historical template data. Never
		// overwrite it: doing so would make stable ordinal recovery use a
		// template different from the one referenced by live resources.
		sameTemplate := string(existing.Data.Raw) == string(data)
		if roles, ok := templateData.([]workloadv1alpha1.Role); ok {
			historical, err := GetRolesFromControllerRevision(existing)
			sameTemplate = err == nil && EqualRoleTemplatesForRevision(historical, roles)
		}
		if !sameTemplate {
			return nil, fmt.Errorf("ControllerRevision %s/%s already exists with different template data", ms.Namespace, controllerRevisionName)
		}
		return existing, nil
	}
	existing, err := client.AppsV1().ControllerRevisions(ms.Namespace).Get(ctx, controllerRevisionName, metav1.GetOptions{})
	if err == nil {
		return validateExisting(existing)
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get ControllerRevision: %w", err)
	}

	// Read informer history so numbering survives restarts and history cleanup.
	// Labels narrow the list; ownership excludes history from an older ModelServing
	// with the same name and from unrelated objects carrying the same label.
	history, err := revisions.ControllerRevisions(ms.Namespace).List(labels.SelectorFromSet(map[string]string{ControllerRevisionLabelKey: ms.Name}))
	if err != nil {
		return nil, fmt.Errorf("failed to list ControllerRevisions: %w", err)
	}
	var maxRevision int64
	for _, previous := range history {
		if metav1.IsControlledBy(previous, ms) && previous.Revision > maxRevision {
			maxRevision = previous.Revision
		}
	}

	// Create ControllerRevision
	cr := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      controllerRevisionName,
			Namespace: ms.Namespace,
			Labels: map[string]string{
				ControllerRevisionLabelKey:         ms.Name,
				ControllerRevisionRevisionLabelKey: revision,
			},
			OwnerReferences: []metav1.OwnerReference{
				newModelServingOwnerRef(ms),
			},
		},
		Revision: maxRevision + 1,
		Data: runtime.RawExtension{
			Raw: data,
		},
	}

	// Create ControllerRevision
	created, err := client.AppsV1().ControllerRevisions(ms.Namespace).Create(ctx, cr, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		// A previous request may have succeeded before its response was lost, or
		// another reconcile may have created it. Apply the same immutable checks.
		existing, getErr := client.AppsV1().ControllerRevisions(ms.Namespace).Get(ctx, controllerRevisionName, metav1.GetOptions{})
		if getErr != nil {
			return nil, fmt.Errorf("get concurrently created ControllerRevision: %w", getErr)
		}
		return validateExisting(existing)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create ControllerRevision: %w", err)
	}

	klog.V(4).Infof("Created ControllerRevision %s/%s with revision %s", ms.Namespace, controllerRevisionName, revision)
	return created, nil
}

// GetControllerRevision retrieves a ControllerRevision by its revision string
func GetControllerRevision(
	ctx context.Context,
	client kubernetes.Interface,
	ms *workloadv1alpha1.ModelServing,
	revision string,
) (*appsv1.ControllerRevision, error) {
	//TODO: get it from a informer's store
	controllerRevisionName := GenerateControllerRevisionName(ms.Name, revision)
	cr, err := client.AppsV1().ControllerRevisions(ms.Namespace).Get(ctx, controllerRevisionName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if !metav1.IsControlledBy(cr, ms) {
		return nil, fmt.Errorf("ControllerRevision %s/%s is not controlled by the current ModelServing UID", ms.Namespace, cr.Name)
	}
	return cr, nil
}

// GetRolesFromControllerRevision extracts roles template data from a ControllerRevision
func GetRolesFromControllerRevision(cr *appsv1.ControllerRevision) ([]workloadv1alpha1.Role, error) {
	if cr == nil || cr.Data.Raw == nil {
		return nil, fmt.Errorf("ControllerRevision or its data is nil")
	}

	version := cr.Annotations[ControllerRevisionDataVersionAnnotation]
	var patch *modelServingRevisionPatch
	if version == ControllerRevisionDataVersionV1 {
		var err error
		patch, err = decodeRevisionPatch(cr.Data.Raw)
		if err != nil {
			return nil, err
		}
	} else if version != "" {
		return nil, fmt.Errorf("unsupported revision data version %q", version)
	}

	// Try to unmarshal as wrapped data first.
	var wrapper map[string]json.RawMessage
	if err := json.Unmarshal(cr.Data.Raw, &wrapper); err == nil {
		if rawData, ok := wrapper["data"]; ok {
			var roles []workloadv1alpha1.Role
			if err := json.Unmarshal(rawData, &roles); err != nil {
				return nil, fmt.Errorf("failed to unmarshal roles from wrapped data: %v", err)
			}
			return roles, nil
		}
	}

	if patch != nil {
		roles := make([]workloadv1alpha1.Role, 0, len(patch.Spec.Template.Roles))
		for _, role := range patch.Spec.Template.Roles {
			roles = append(roles, revisionRole(role))
		}
		return roles, nil
	}
	// Fallback: try to unmarshal directly (for backward compatibility or if not wrapped)
	var roles []workloadv1alpha1.Role
	if err := json.Unmarshal(cr.Data.Raw, &roles); err != nil {
		return nil, fmt.Errorf("failed to unmarshal roles from ControllerRevision: %v", err)
	}

	return roles, nil
}

// CleanupOldControllerRevisions deletes old ControllerRevisions that are no longer in use.
// Preserve status, datastore and live Pod references, including mixed Role revisions
// and terminating Pods. Only history controlled by this ModelServing can be deleted.
func CleanupOldControllerRevisions(
	ctx context.Context,
	client kubernetes.Interface,
	revisions appslisters.ControllerRevisionLister,
	ms *workloadv1alpha1.ModelServing,
	referencedRevisions ...string,
) error {
	// Get all ControllerRevisions for this ModelServing
	selector := labels.SelectorFromSet(map[string]string{
		ControllerRevisionLabelKey: ms.Name,
	})

	history, err := revisions.ControllerRevisions(ms.Namespace).List(selector)
	if err != nil {
		return fmt.Errorf("failed to list ControllerRevisions: %v", err)
	}

	// Get the revision names that must be preserved (CurrentRevision and UpdateRevision)
	var currentRevisionName, updateRevisionName string
	if ms.Status.CurrentRevision != "" {
		currentRevisionName = GenerateControllerRevisionName(ms.Name, ms.Status.CurrentRevision)
	}
	if ms.Status.UpdateRevision != "" {
		updateRevisionName = GenerateControllerRevisionName(ms.Name, ms.Status.UpdateRevision)
	}
	preservedRevisionNames := make(map[string]struct{}, len(referencedRevisions)+2)
	if currentRevisionName != "" {
		preservedRevisionNames[currentRevisionName] = struct{}{}
	}
	if updateRevisionName != "" {
		preservedRevisionNames[updateRevisionName] = struct{}{}
	}
	for _, referencedRevision := range referencedRevisions {
		if referencedRevision != "" {
			preservedRevisionNames[GenerateControllerRevisionName(ms.Name, referencedRevision)] = struct{}{}
		}
	}
	// List history before Pods: a snapshot created after this history list is
	// not a deletion candidate. Live reads also cover Pods not yet in the store.
	pods, err := client.CoreV1().Pods(ms.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return fmt.Errorf("list Pods before cleaning ControllerRevisions: %w", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if revision := ObjectRevision(pod); metav1.IsControlledBy(pod, ms) && revision != "" {
			preservedRevisionNames[GenerateControllerRevisionName(ms.Name, revision)] = struct{}{}
		}
	}

	// Live references do not count toward the API's non-live history limit.
	historyLimit := 10
	if ms.Spec.RevisionHistoryLimit != nil {
		historyLimit = max(0, int(*ms.Spec.RevisionHistoryLimit))
	}
	var unused []*appsv1.ControllerRevision
	for _, revision := range history {
		if !metav1.IsControlledBy(revision, ms) {
			continue
		}
		// Skip if this revision must be preserved
		if _, preserved := preservedRevisionNames[GenerateControllerRevisionName(ms.Name, revision.Labels[ControllerRevisionRevisionLabelKey])]; preserved {
			continue
		}
		unused = append(unused, revision)
	}
	sort.Slice(unused, func(i, j int) bool { return ControllerRevisionLess(unused[i], unused[j]) })

	deletedCount := 0
	var deletionErrors []error
	for _, revision := range unused[:max(0, len(unused)-historyLimit)] {
		uid := revision.UID
		resourceVersion := revision.ResourceVersion
		err := client.AppsV1().ControllerRevisions(ms.Namespace).Delete(ctx, revision.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion},
		})
		if err != nil && !apierrors.IsNotFound(err) {
			deletionErrors = append(deletionErrors, fmt.Errorf("delete ControllerRevision %s/%s: %w", ms.Namespace, revision.Name, err))
		} else {
			deletedCount++
			klog.V(4).Infof("Deleted old ControllerRevision %s/%s", ms.Namespace, revision.Name)
		}
	}

	if deletedCount > 0 {
		klog.V(4).Infof("Cleaned up %d old ControllerRevisions for ModelServing %s/%s (preserved CurrentRevision=%s, UpdateRevision=%s)",
			deletedCount, ms.Namespace, ms.Name, ms.Status.CurrentRevision, ms.Status.UpdateRevision)
	}

	return errors.Join(deletionErrors...)
}
