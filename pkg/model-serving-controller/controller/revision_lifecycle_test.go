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
	"testing"

	"github.com/stretchr/testify/require"
	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func TestDesiredRevisionFailureStopsRoleRollout(t *testing.T) {
	for _, verb := range []string{"list", "create", "update"} {
		t.Run(verb, func(t *testing.T) {
			ctx := context.Background()
			old := createStandardModelServing("history-first", 1, 1)
			old.UID = "owner"
			ms := old.DeepCopy()
			ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate}
			ms.Spec.Template.Roles[0].MaxUnavailable = ptr.To(intstr.FromInt(1))
			ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "new:v2"
			ms.Status.CurrentRevision, ms.Status.UpdateRevision = "legacy", "legacy"
			c := newRevisionTestController(t, ms)
			client := c.kubeClientSet.(*kubefake.Clientset)
			if verb == "update" {
				// The desired template already exists as older history: rollback
				// must persist its promoted number before touching Pods.
				_, err := utils.CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, utils.ModelServingRevision(ms), ms.Spec.Template.Roles)
				require.NoError(t, err)
			}
			_, err := utils.CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "legacy", old.Spec.Template.Roles)
			require.NoError(t, err)
			pod := addReadyLegacyGroupToController(t, c, ms, old.Spec.Template.Roles[0], 0, "legacy")
			_, err = client.CoreV1().Pods(ms.Namespace).Create(ctx, pod.DeepCopy(), metav1.CreateOptions{})
			require.NoError(t, err)
			fail := true
			originalLister := c.controllerRevisionLister
			if verb == "list" {
				c.controllerRevisionLister = revisiontest.FailingControllerRevisions(fmt.Errorf("injected history list failure"))
			}
			client.PrependReactor(verb, "controllerrevisions", func(kubetesting.Action) (bool, runtime.Object, error) {
				if fail {
					return true, nil, fmt.Errorf("injected history %s failure", verb)
				}
				return false, nil, nil
			})
			client.ClearActions()
			require.ErrorContains(t, c.syncModelServing(ctx, ms.Namespace+"/"+ms.Name), "injected history")
			for _, action := range client.Actions() {
				if action.GetResource().Resource == "pods" {
					require.Contains(t, []string{"get", "list"}, action.GetVerb(), "no Pod mutation before persisting history")
				}
			}
			require.Equal(t, datastore.RoleRunning, c.store.GetRoleStatus(utils.GetNamespaceName(ms), ms.Name+"-0", "prefill", "prefill-0"))
			status, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(ctx, ms.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, "legacy", status.Status.UpdateRevision)
			fail = false
			c.controllerRevisionLister = originalLister
			client.ClearActions()
			require.NoError(t, c.syncModelServing(ctx, ms.Namespace+"/"+ms.Name))
			historySaved, podDeletion := -1, -1
			saveVerb := "create"
			if verb == "update" {
				saveVerb = "update"
			}
			for index, action := range client.Actions() {
				if action.Matches(saveVerb, "controllerrevisions") {
					historySaved = index
				}
				if action.Matches("delete", "pods") {
					podDeletion = index
				}
			}
			require.GreaterOrEqual(t, historySaved, 0)
			require.Greater(t, podDeletion, historySaved)
			target, err := utils.GetControllerRevision(ctx, client, ms, utils.ModelServingRevision(ms))
			require.NoError(t, err)
			require.NotNil(t, target)
			if verb == "update" {
				require.EqualValues(t, 3, target.Revision)
			}
		})
	}
}

func TestRevisionCleanupRetriesAfterStatusConverges(t *testing.T) {
	ctx := context.Background()
	ms := createStandardModelServing("cleanup-retry", 1, 1)
	ms.UID = "owner"
	ms.Spec.RevisionHistoryLimit = ptr.To[int32](0)
	ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate}
	decode := *ms.Spec.Template.Roles[0].DeepCopy()
	decode.Name = "decode"
	ms.Spec.Template.Roles = append(ms.Spec.Template.Roles, decode)
	old := ms.DeepCopy()
	ms.Spec.Template.Roles[1].EntryTemplate.Spec.Containers[0].Image = "new:v2"
	ms.Status.CurrentRevision, ms.Status.UpdateRevision = "legacy", "legacy"
	newRevision := utils.ModelServingRevision(ms)
	c := newRevisionTestController(t, ms)
	client := c.kubeClientSet.(*kubefake.Clientset)
	for _, revision := range []string{"legacy", "unused"} {
		_, err := utils.CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, revision, old.Spec.Template.Roles)
		require.NoError(t, err)
	}
	_, err := utils.CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, newRevision, ms.Spec.Template.Roles)
	require.NoError(t, err)
	for i, role := range ms.Spec.Template.Roles {
		revision := newRevision
		if i == 0 {
			revision = "legacy" // unchanged prefill must not be relabeled or replaced
		}
		pod := addReadyLegacyGroupToController(t, c, ms, role, 0, revision)
		_, err := client.CoreV1().Pods(ms.Namespace).Create(ctx, pod.DeepCopy(), metav1.CreateOptions{})
		require.NoError(t, err)
	}
	require.NoError(t, c.store.UpdateServingGroupRevision(utils.GetNamespaceName(ms), ms.Name+"-0", newRevision))
	fail := true
	client.PrependReactor("delete", "controllerrevisions", func(action kubetesting.Action) (bool, runtime.Object, error) {
		if fail && action.(kubetesting.DeleteAction).GetName() == ms.Name+"-unused" {
			return true, nil, fmt.Errorf("injected cleanup failure")
		}
		return false, nil, nil
	})
	require.ErrorContains(t, c.syncModelServing(ctx, ms.Namespace+"/"+ms.Name), "injected cleanup failure")
	updated, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(ctx, ms.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, newRevision, updated.Status.CurrentRevision)
	require.Equal(t, newRevision, updated.Status.UpdateRevision)
	require.NoError(t, c.modelServingsInformer.GetIndexer().Update(updated))
	fail = false
	require.NoError(t, c.syncModelServing(ctx, ms.Namespace+"/"+ms.Name))
	for _, revision := range []string{"legacy", newRevision} {
		cr, err := utils.GetControllerRevision(ctx, client, ms, revision)
		require.NoError(t, err)
		require.NotNil(t, cr, "preserve mixed Role/Pod history after status promotion")
	}
	cr, err := utils.GetControllerRevision(ctx, client, ms, "unused")
	require.NoError(t, err)
	require.Nil(t, cr, "cleanup retries without another revision-status change")
	roles, err := c.store.GetRoleList(utils.GetNamespaceName(ms), ms.Name+"-0", "prefill")
	require.NoError(t, err)
	require.Equal(t, "legacy", roles[0].Revision)
}

func TestProtectedRoleRecoveryRequiresHistoricalSnapshot(t *testing.T) {
	for _, state := range []string{"valid", "missing", "malformed", "foreign", "missing-role"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			ms := createStandardModelServing("protected", 1, 2)
			ms.UID = "owner"
			ms.Status.CurrentRevision = "old"
			ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate}
			ms.Spec.Template.Roles[0].Partition = ptr.To(intstr.FromInt(1))
			c := newRevisionTestController(t, ms)
			client := c.kubeClientSet.(*kubefake.Clientset)
			if state != "missing" {
				old := ms.DeepCopy()
				old.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "old:v1"
				if state == "missing-role" {
					old.Spec.Template.Roles[0].Name = "different"
				}
				cr, err := utils.CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "old", old.Spec.Template.Roles)
				require.NoError(t, err)
				if state == "malformed" {
					cr.Data.Raw = []byte(`{"data":"broken"}`)
				} else if state == "foreign" {
					cr.OwnerReferences[0].UID = "foreign-owner"
				}
				_, err = client.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, cr, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			key, groupName := utils.GetNamespaceName(ms), ms.Name+"-0"
			c.store.AddServingGroup(key, 0, "old")
			role := ms.Spec.Template.Roles[0]
			client.ClearActions()
			err := c.scaleUpRoles(ctx, ms, groupName, role, nil, 1, 0, "new", true)
			if state == "valid" {
				require.NoError(t, err)
				pods, err := client.CoreV1().Pods(ms.Namespace).List(ctx, metav1.ListOptions{})
				require.NoError(t, err)
				require.Len(t, pods.Items, 1)
				require.Equal(t, "old:v1", pods.Items[0].Spec.Containers[0].Image)
				require.Equal(t, "old", utils.ObjectRevision(&pods.Items[0]))
			} else {
				require.Error(t, err)
				for _, action := range client.Actions() {
					require.False(t, action.Matches("create", "pods"))
				}
			}
			_, revision, _, err := c.roleTemplateForReplica(ctx, ms, role, datastore.Role{Revision: "old"}, "new", true)
			if state == "valid" {
				require.NoError(t, err)
				require.Equal(t, "old", revision)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestProtectedRoleCreationUsesCurrentNotArbitraryHistory(t *testing.T) {
	ms := createStandardModelServing("initial-protected", 1, 1)
	ms.UID = "owner"
	ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate}
	ms.Spec.Template.Roles[0].Partition = ptr.To(intstr.FromInt(1))
	c := newRevisionTestController(t, ms)
	ctx := context.Background()
	old := ms.DeepCopy()
	old.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "obsolete:v0"
	_, err := utils.CreateControllerRevision(ctx, c.kubeClientSet, revisiontest.ControllerRevisions(c.kubeClientSet), ms, "arbitrary-old", old.Spec.Template.Roles)
	require.NoError(t, err)
	revision, err := c.revisionHistory(ctx, ms).desiredRevision(ctx)
	require.NoError(t, err)
	ms.Status.CurrentRevision = revision
	c.store.AddServingGroup(utils.GetNamespaceName(ms), 0, revision)
	require.NoError(t, c.scaleUpRoles(ctx, ms, ms.Name+"-0", ms.Spec.Template.Roles[0], nil, 1, 0, revision, true))
	pods, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, pods.Items, 1)
	require.Equal(t, revision, utils.ObjectRevision(&pods.Items[0]))
	require.Equal(t, ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image, pods.Items[0].Spec.Containers[0].Image)
}

func TestZeroReplicasPersistsHistoryAndConvergesStatus(t *testing.T) {
	ms := createStandardModelServing("zero-history", 0, 1)
	ms.UID = "owner"
	ms.Generation = 3
	ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate}
	ms.Status.Replicas, ms.Status.AvailableReplicas = 2, 2
	c := newRevisionTestController(t, ms)
	ctx := context.Background()
	require.NoError(t, c.syncModelServing(ctx, ms.Namespace+"/"+ms.Name))
	updated, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(ctx, ms.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, ms.Generation, updated.Status.ObservedGeneration)
	require.Zero(t, updated.Status.Replicas)
	require.Zero(t, updated.Status.AvailableReplicas)
	require.Equal(t, updated.Status.CurrentRevision, updated.Status.UpdateRevision)
	cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, updated.Status.UpdateRevision)
	require.NoError(t, err)
	require.NotNil(t, cr)
	for _, action := range c.kubeClientSet.(*kubefake.Clientset).Actions() {
		require.False(t, action.Matches("create", "pods"))
	}
}

func TestRevisionCleanupPreservesRoleHistoryWithoutLivePod(t *testing.T) {
	ctx := context.Background()
	ms := createStandardModelServing("pending-history", 1, 1)
	ms.UID = "owner"
	ms.Spec.RevisionHistoryLimit = ptr.To[int32](0)
	revision := utils.ModelServingRevision(ms)
	ms.Status.CurrentRevision, ms.Status.UpdateRevision = revision, revision
	c := newRevisionTestController(t, ms)
	for _, name := range []string{revision, "role-history", "unused"} {
		_, err := utils.CreateControllerRevision(ctx, c.kubeClientSet, revisiontest.ControllerRevisions(c.kubeClientSet), ms, name, ms.Spec.Template.Roles)
		require.NoError(t, err)
	}
	key, groupName := utils.GetNamespaceName(ms), ms.Name+"-0"
	c.store.AddServingGroup(key, 0, revision)
	c.store.AddRole(key, groupName, "prefill", "prefill-0", "role-history", utils.CalRoleTemplateHash(ms.Spec.Template.Roles[0]))
	require.NoError(t, c.UpdateModelServingStatus(ms, revision))
	pods, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, pods.Items)
	cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, "role-history")
	require.NoError(t, err)
	require.NotNil(t, cr, "a pending Role still references its recovery snapshot")
	cr, err = utils.GetControllerRevision(ctx, c.kubeClientSet, ms, "unused")
	require.NoError(t, err)
	require.Nil(t, cr)
}
