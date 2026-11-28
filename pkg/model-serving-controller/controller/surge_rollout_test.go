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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	kthenafake "github.com/volcano-sh/kthena/client-go/clientset/versioned/fake"
	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
)

func TestPlanSurgeCompletion(t *testing.T) {
	stable := func(name string) surgeReplica { return surgeReplica{name: name, ready: true} }
	temporary := func(name string) surgeReplica { return surgeReplica{name: name, ready: true, temporary: true} }
	notReady := func(r surgeReplica) surgeReplica { r.ready = false; return r }
	deleting := func(r surgeReplica) surgeReplica { r.deleting = true; return r }
	protected := func(r surgeReplica) surgeReplica { r.protected = true; return r }
	tests := []struct {
		name        string
		surge       int
		unavailable int
		instances   []surgeReplica
		wantCount   int
		wantDelete  []string
	}{
		{"restore missing zero before deleting surge", 1, 0, []surgeReplica{stable("1"), stable("2"), temporary("3")}, 4, nil},
		{"wait for restored zero to become ready", 1, 0, []surgeReplica{notReady(stable("0")), stable("1"), stable("2"), temporary("3")}, 4, nil},
		{"delete only temporary replica when zero is ready", 1, 0, []surgeReplica{stable("0"), stable("1"), stable("2"), temporary("3")}, 4, []string{"3"}},
		{"unready stable replica is not a scale-down candidate", 1, 1, []surgeReplica{notReady(stable("0")), stable("1"), stable("2"), temporary("3")}, 4, []string{"3"}},
		{"deleting replacement does not provide availability", 1, 0, []surgeReplica{deleting(stable("0")), stable("1"), stable("2"), temporary("3")}, 4, nil},
		{"terminating surge consumes capacity", 1, 0, []surgeReplica{stable("1"), stable("2"), deleting(temporary("3")), temporary("4")}, 4, nil},
		{"unready surge can release occupied capacity", 1, 0, []surgeReplica{stable("1"), stable("2"), notReady(temporary("3")), temporary("4")}, 4, []string{"3"}},
		{"reduced surge uses unavailable budget", 0, 1, []surgeReplica{stable("1"), stable("2"), temporary("3")}, 3, []string{"3"}},
		{"zero budgets hold ready surge", 0, 0, []surgeReplica{stable("1"), stable("2"), temporary("3")}, 3, nil},
		{"zero budgets hold unready surge", 0, 0, []surgeReplica{stable("1"), stable("2"), notReady(temporary("3"))}, 3, nil},
		{"unmarked sparse identities remain stable", 1, 0, []surgeReplica{stable("0"), stable("2"), stable("5")}, 3, nil},
		{"protected temporary identity is retained", 1, 0, []surgeReplica{stable("1"), stable("2"), protected(temporary("3"))}, 3, nil},
		{"multiple missing ordinals respect surge limit", 1, 0, []surgeReplica{stable("2"), temporary("3"), temporary("4")}, 4, nil},
		{"multiple temporary replicas share one availability budget", 2, 0, []surgeReplica{notReady(stable("0")), stable("1"), stable("2"), temporary("3"), temporary("4")}, 5, []string{"4"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, toDelete := planSurgeCompletion(3, tt.surge, tt.unavailable, tt.instances)
			assert.Equal(t, tt.wantCount, count)
			assert.Equal(t, tt.wantDelete, toDelete)
		})
	}
}

// Rebuild a controller from Pod objects, as happens after a process restart.
// No in-memory surge state is supplied.
func newSurgeCompletionController(t *testing.T, scope string) (*ModelServingController, *workloadv1alpha1.ModelServing, string) {
	t.Helper()
	ms := &workloadv1alpha1.ModelServing{
		ObjectMeta: metav1.ObjectMeta{Name: "completion", Namespace: "default", UID: types.UID("completion-owner")},
		Spec: workloadv1alpha1.ModelServingSpec{
			Replicas: ptr.To[int32](3),
			RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
				Type: workloadv1alpha1.ServingGroupRollingUpdate,
				RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
					MaxSurge: ptr.To(intstr.FromInt(1)), MaxUnavailable: ptr.To(intstr.FromInt(0)),
				},
			},
			Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{{
				Name: "inference", Replicas: ptr.To[int32](1),
				EntryTemplate: workloadv1alpha1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "server", Image: "new"}},
				}},
			}}},
		},
		Status: workloadv1alpha1.ModelServingStatus{CurrentRevision: "old"},
	}
	if scope == surgeRole {
		ms.Spec.Replicas = ptr.To[int32](1)
		ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate}
		ms.Spec.Template.Roles[0].Replicas = ptr.To[int32](3)
		ms.Spec.Template.Roles[0].MaxSurge = ptr.To(intstr.FromInt(1))
		ms.Spec.Template.Roles[0].MaxUnavailable = ptr.To(intstr.FromInt(0))
	}
	revision := utils.ModelServingRevision(ms)
	ms.Status.UpdateRevision = revision
	c, err := newControllerWithRevisionHistory(t, kubefake.NewSimpleClientset(), kthenafake.NewSimpleClientset(ms), nil, apiextfake.NewSimpleClientset())
	require.NoError(t, err)
	t.Cleanup(c.workqueue.ShutDown)
	require.NoError(t, c.modelServingsInformer.GetIndexer().Add(ms))
	role := ms.Spec.Template.Roles[0]
	for ordinal := 1; ordinal <= 3; ordinal++ {
		groupOrdinal, roleOrdinal := ordinal, 0
		if scope == surgeRole {
			groupOrdinal, roleOrdinal = 0, ordinal
		}
		groupName := utils.GenerateServingGroupName(ms.Name, groupOrdinal)
		roleID := utils.GenerateRoleID(role.Name, roleOrdinal)
		c.store.AddServingGroup(utils.GetNamespaceName(ms), groupOrdinal, revision)
		c.store.AddRole(utils.GetNamespaceName(ms), groupName, role.Name, roleID, revision, utils.CalRoleTemplateHash(role))
		require.NoError(t, c.store.UpdateRoleStatus(utils.GetNamespaceName(ms), groupName, role.Name, roleID, datastore.RoleRunning))
		require.NoError(t, c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), groupName, datastore.ServingGroupRunning))
		pod := utils.GenerateEntryPod(*role.DeepCopy(), ms, groupName, roleID, revision, utils.CalRoleTemplateHash(role))
		pod.UID = types.UID(pod.Name)
		if ordinal == 3 {
			setSurgeScope(pod, scope)
		}
		_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
		require.NoError(t, err)
		require.NoError(t, c.podsInformer.GetIndexer().Add(pod))
	}
	return c, ms, revision
}

func TestSurgeCompletionAfterRestart(t *testing.T) {
	for _, scope := range []string{surgeServingGroup, surgeRole} {
		t.Run(scope, func(t *testing.T) {
			c, ms, revision := newSurgeCompletionController(t, scope)
			ctx := context.Background()
			key := utils.GetNamespaceName(ms)
			groupName := utils.GenerateServingGroupName(ms.Name, 0)
			role := ms.Spec.Template.Roles[0]
			sync := func() {
				if scope == surgeServingGroup {
					require.NoError(t, c.syncServingGroupReplicas(ctx, ms, revision))
				} else {
					require.NoError(t, c.manageRoleReplicasPerGroup(ctx, ms, groupName, role, 0, revision, nil, true))
				}
			}
			// All instances are updated and Ready, but marked 3 is not a final replica.
			require.NoError(t, c.UpdateModelServingStatus(ms, revision))
			current, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(ctx, ms.Name, metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, "old", current.Status.CurrentRevision)
			found := false
			for _, condition := range current.Status.Conditions {
				if condition.Type == string(workloadv1alpha1.ModelServingUpdateInProgress) {
					assert.Equal(t, metav1.ConditionTrue, condition.Status)
					found = true
				}
			}
			require.True(t, found, "missing UpdateInProgress condition")
			sync()
			pods, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			require.Len(t, pods.Items, 4)
			for i := range pods.Items {
				require.NoError(t, c.podsInformer.GetIndexer().Update(&pods.Items[i]))
			}
			assertSurgeStatus := func(wantDeleting bool) {
				if scope == surgeServingGroup {
					status := c.store.GetServingGroupStatus(key, utils.GenerateServingGroupName(ms.Name, 3))
					assert.Equal(t, wantDeleting, status == datastore.ServingGroupDeleting)
				} else {
					status := c.store.GetRoleStatus(key, groupName, role.Name, utils.GenerateRoleID(role.Name, 3))
					roles, _ := c.store.GetRoleList(key, groupName, role.Name)
					assert.Equal(t, wantDeleting, status == datastore.RoleDeleting, "roles: %+v", roles)
				}
			}
			sync()
			assertSurgeStatus(false)
			if scope == surgeServingGroup {
				require.NoError(t, c.store.UpdateServingGroupStatus(key, groupName, datastore.ServingGroupRunning))
			} else {
				require.NoError(t, c.store.UpdateRoleStatus(key, groupName, role.Name, utils.GenerateRoleID(role.Name, 0), datastore.RoleRunning))
			}
			sync()
			assertSurgeStatus(true)
		})
	}
}

func TestSurgeAdoptionPreservesPodsAndIgnoresOtherOwners(t *testing.T) {
	for _, scope := range []string{surgeServingGroup, surgeRole} {
		t.Run(scope, func(t *testing.T) {
			c, ms, _ := newSurgeCompletionController(t, scope)
			pods, err := c.surgePods(ms, scope, "", "")
			require.NoError(t, err)
			require.Len(t, pods, 1)
			marked := pods[0]
			// Markers on workers must also disappear on adoption.
			worker := marked.DeepCopy()
			worker.Name += "-worker"
			worker.UID = types.UID(worker.Name)
			_, err = c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(context.Background(), worker, metav1.CreateOptions{})
			require.NoError(t, err)
			require.NoError(t, c.podsInformer.GetIndexer().Add(worker))
			other := marked.DeepCopy()
			other.Name += "-other-owner"
			other.OwnerReferences[0].UID = types.UID("other-owner")
			_, err = c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(context.Background(), other, metav1.CreateOptions{})
			require.NoError(t, err)
			require.NoError(t, c.podsInformer.GetIndexer().Add(other))
			require.NoError(t, c.adoptSurgeReplicas(context.Background(), ms, scope, "", "", 4))
			for _, pod := range []*corev1.Pod{marked, worker} {
				current, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
				require.NoError(t, err)
				assert.Equal(t, pod.UID, current.UID)
				assert.Empty(t, current.Annotations[surgeAnnotation])
			}
			current, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Get(context.Background(), other.Name, metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, scope, current.Annotations[surgeAnnotation])
		})
	}
}

func TestUnmarkedSparseReplicasRemainStableWithSurgeConfigured(t *testing.T) {
	for _, scope := range []string{surgeServingGroup, surgeRole} {
		t.Run(scope, func(t *testing.T) {
			c, ms, revision := newSurgeCompletionController(t, scope)
			pods, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).List(context.Background(), metav1.ListOptions{})
			require.NoError(t, err)
			for i := range pods.Items {
				pod := pods.Items[i].DeepCopy()
				delete(pod.Annotations, surgeAnnotation)
				_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Update(context.Background(), pod, metav1.UpdateOptions{})
				require.NoError(t, err)
				require.NoError(t, c.podsInformer.GetIndexer().Update(pod))
			}
			c.kubeClientSet.(*kubefake.Clientset).ClearActions()
			if scope == surgeServingGroup {
				require.NoError(t, c.syncServingGroupReplicas(context.Background(), ms, revision))
			} else {
				require.NoError(t, c.manageRoleReplicasPerGroup(context.Background(), ms, utils.GenerateServingGroupName(ms.Name, 0), ms.Spec.Template.Roles[0], 0, revision, nil, true))
			}
			for _, action := range c.kubeClientSet.(*kubefake.Clientset).Actions() {
				if action.GetResource().Resource == "pods" {
					assert.NotContains(t, []string{"create", "delete", "delete-collection", "patch", "update"}, action.GetVerb())
				}
			}
			pending, err := c.hasPendingSurge(ms)
			require.NoError(t, err)
			assert.False(t, pending)
		})
	}
}

func TestSurgeMarkerDoesNotChangeRoleTemplateOrRevision(t *testing.T) {
	c, ms, revision := newSurgeCompletionController(t, surgeRole)
	role := ms.Spec.Template.Roles[0]
	role.WorkerReplicas = 1
	role.WorkerTemplate = role.EntryTemplate.DeepCopy()
	before := role.DeepCopy()
	hash := utils.CalRoleTemplateHash(role)
	require.NoError(t, c.CreatePodsByRole(context.Background(), *role.DeepCopy(), ms, 4, 0, revision, hash, surgeRole))
	assert.Equal(t, *before, role)
	assert.Equal(t, hash, utils.CalRoleTemplateHash(role))
	for _, name := range []string{"completion-0-inference-4-0", "completion-0-inference-4-1"} {
		pod, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Get(context.Background(), name, metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, surgeRole, pod.Annotations[surgeAnnotation])
		assert.Equal(t, hash, utils.ObjectRoleTemplateHash(pod))
		assert.Equal(t, surgeRole, inheritedSurgeScope([]*corev1.Pod{pod}))
	}
}

// Sparse high ordinals must use their identity, not their position in the list,
// when deciding whether partition prevents cleanup.
func TestSurgeCleanupUsesAbsolutePartition(t *testing.T) {
	for _, scope := range []string{surgeServingGroup, surgeRole} {
		t.Run(scope, func(t *testing.T) {
			c, ms, revision := newSurgeCompletionController(t, scope)
			ctx := context.Background()
			key := utils.GetNamespaceName(ms)
			role := ms.Spec.Template.Roles[0]
			if scope == surgeServingGroup {
				ms.Spec.Replicas = ptr.To[int32](2)
				ms.Spec.RolloutStrategy.RollingUpdateConfiguration.Partition = ptr.To(intstr.FromInt(2))
				ms.Spec.RolloutStrategy.RollingUpdateConfiguration.MaxSurge = ptr.To(intstr.FromInt(0))
				ms.Spec.RolloutStrategy.RollingUpdateConfiguration.MaxUnavailable = ptr.To(intstr.FromInt(1))
				c.store.DeleteServingGroup(key, "completion-2")
				groups, err := c.store.GetServingGroupByModelServing(key)
				require.NoError(t, err)
				handled, err := c.finishServingGroupSurge(ctx, ms, groups, revision, 0, 2)
				require.NoError(t, err)
				require.True(t, handled)
				assert.Equal(t, datastore.ServingGroupDeleting, c.store.GetServingGroupStatus(key, "completion-3"))
				assert.Equal(t, datastore.ServingGroupRunning, c.store.GetServingGroupStatus(key, "completion-1"))
			} else {
				role.Replicas = ptr.To[int32](2)
				role.Partition = ptr.To(intstr.FromInt(2))
				role.MaxSurge = ptr.To(intstr.FromInt(0))
				role.MaxUnavailable = ptr.To(intstr.FromInt(1))
				c.store.DeleteRole(key, "completion-0", role.Name, "inference-2")
				roles, err := c.store.GetRoleList(key, "completion-0", role.Name)
				require.NoError(t, err)
				handled, err := c.finishRoleSurge(ctx, ms, "completion-0", role, roles, 0, revision, true)
				require.NoError(t, err)
				require.True(t, handled)
				assert.Equal(t, datastore.RoleDeleting, c.store.GetRoleStatus(key, "completion-0", role.Name, "inference-3"))
				assert.Equal(t, datastore.RoleRunning, c.store.GetRoleStatus(key, "completion-0", role.Name, "inference-1"))
			}
		})
	}
}

func TestSurgeCreationPreservesSparseRetainedIdentities(t *testing.T) {
	for _, tt := range []struct {
		name        string
		stableSlots int
		existing    []int
		want        int
	}{
		{"SG-S03 keeps the low hole for the existing in-range identity", 0, []int{0, 3}, 2},
		{"healthy high identity occupies the first surge ordinal", 0, []int{0, 2}, 3},
		{"SG-S02 and SG-P11 replace outdated high identity in the low hole", 1, []int{0, 3}, 1},
		{"missing stable capacity is restored before surge", 1, []int{3}, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var created []int
			forEachRolloutOrdinal(2, tt.stableSlots, tt.existing, 1, func(ordinal int) bool {
				created = append(created, ordinal)
				return true
			})
			assert.Equal(t, []int{tt.want}, created)
		})
	}
}
