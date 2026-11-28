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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func TestRevisionErrorStillScalesDown(t *testing.T) {
	for _, scenario := range []string{"groups", "roles"} {
		t.Run(scenario, func(t *testing.T) {
			ms := createStandardModelServing("shrink", 1, 1)
			ms.UID = "shrink-uid"
			c := newRevisionTestController(t, ms)
			t.Cleanup(c.workqueue.ShutDown)
			ctx := context.Background()
			revision := utils.ModelServingRevision(ms)
			cr, err := utils.CreateControllerRevision(ctx, c.kubeClientSet, revisiontest.ControllerRevisions(c.kubeClientSet), ms, revision, ms.Spec.Template.Roles)
			require.NoError(t, err)
			cr.Data = runtime.RawExtension{Raw: []byte(`{"data":"invalid"}`)}
			_, err = c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, cr, metav1.UpdateOptions{})
			require.NoError(t, err)
			count := 2
			if scenario == "roles" {
				count = 1
				ms.Spec.Template.Roles[0].Replicas = ptr.To[int32](0)
			}
			for i := 0; i < count; i++ {
				addReadyLegacyGroupToController(t, c, ms, ms.Spec.Template.Roles[0], i, "legacy")
			}
			client := c.kubeClientSet.(*kubefake.Clientset)
			client.ClearActions()
			require.Error(t, c.syncModelServing(ctx, namespacedKey(ms.Namespace, ms.Name)))
			key := utils.GetNamespaceName(ms)
			if scenario == "groups" {
				require.Equal(t, datastore.ServingGroupDeleting, c.store.GetServingGroupStatus(key, "shrink-1"))
			} else {
				require.Equal(t, datastore.RoleDeleting, c.store.GetRoleStatus(key, "shrink-0", "prefill", "prefill-0"))
			}
			for _, action := range client.Actions() {
				if action.GetResource().Resource == "pods" {
					require.NotEqual(t, "create", action.GetVerb())
				}
			}
		})
	}
}

func TestUnresolvedHistorySchedulesRetry(t *testing.T) {
	ms := createStandardModelServing("history-retry", 1, 1)
	ms.UID = "retry-uid"
	c := newRevisionTestController(t, ms)
	t.Cleanup(c.workqueue.ShutDown)
	_, err := c.revisionHistory(context.Background(), ms).roles(context.Background(), "missing")
	require.Error(t, err)
	require.Eventually(t, func() bool { return c.workqueue.Len() > 0 }, 7*time.Second, 20*time.Millisecond)
}

func TestRevisionErrorDoesNotHideMutationFailure(t *testing.T) {
	unresolved := &revisionResolutionError{fmt.Errorf("missing history")}
	require.True(t, isRevisionResolutionError(fmt.Errorf("Role template: %w", unresolved)))
	require.False(t, isRevisionResolutionError(errors.Join(unresolved, fmt.Errorf("delete hook failed"))))
}

func TestRevisionErrorDistinguishesTemporarySurgeFromExplicitShrink(t *testing.T) {
	for _, scope := range []string{surgeServingGroup, surgeRole} {
		for _, desired := range []int32{2, 1, 0} {
			t.Run(fmt.Sprintf("%s/desired=%d", scope, desired), func(t *testing.T) {
				ctx := context.Background()
				c, ms, _ := newSurgeCompletionController(t, scope)
				if scope == surgeServingGroup {
					ms.Spec.Replicas = ptr.To(desired)
				} else {
					ms.Spec.Template.Roles[0].Replicas = ptr.To(desired)
				}
				_, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Update(ctx, ms, metav1.UpdateOptions{})
				require.NoError(t, err)
				kube := c.kubeClientSet.(*kubefake.Clientset)
				kube.ClearActions()
				require.NoError(t, c.scaleDownOnRevisionError(ctx, ms))
				deleted := []string{}
				for _, action := range kube.Actions() {
					if action.Matches("delete", "pods") {
						deleted = append(deleted, action.(kubetesting.DeleteAction).GetName())
					}
				}
				want := 2 - int(desired)
				if desired == 0 {
					want = 3
				}
				require.Len(t, deleted, want, "only formal excess is explicit scale down, except zero removes all")
				surgeName := "completion-3-inference-0-0"
				if scope == surgeRole {
					surgeName = "completion-0-inference-3-0"
				}
				if desired > 0 {
					require.NotContains(t, deleted, surgeName)
				} else {
					require.Contains(t, deleted, surgeName)
				}
			})
		}
	}
}

func TestRevisionErrorPreservesLegalSurgeOnCorruptHistory(t *testing.T) {
	ctx := context.Background()
	old := lifecycleMS("surge-error", 1, 0, 1)
	old.ResourceVersion = "1"
	ms := old.DeepCopy()
	ms.Generation++
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "test:v2"
	c := lifecycleController(t, ms, old)
	lifecyclePod(t, c, ms, old, 0, "stable")
	surge := lifecyclePod(t, c, ms, ms, 1, "surge")
	surge.Annotations = map[string]string{surgeAnnotation: surgeServingGroup}
	surge.Status.Conditions[0].Status = corev1.ConditionFalse
	_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Update(ctx, surge, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, c.podsInformer.GetIndexer().Update(surge))
	require.NoError(t, c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), ms.Name+"-1", datastore.ServingGroupCreating))
	revision := utils.ModelServingRevision(ms)
	cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, revision)
	require.NoError(t, err)
	cr.Data.Raw = []byte(`{"data":"invalid"}`)
	_, err = c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, cr, metav1.UpdateOptions{})
	require.NoError(t, err)
	c.kubeClientSet.(*kubefake.Clientset).ClearActions()
	require.Error(t, c.syncModelServing(ctx, namespacedKey(ms.Namespace, ms.Name)))
	require.False(t, lifecycleDeletionMatches(c, surge), "unchanged replicas plus allowed target surge is not an explicit scale-down")
}
