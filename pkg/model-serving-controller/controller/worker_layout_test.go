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
	"time"

	"github.com/stretchr/testify/require"
	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func workerLayoutFixture(t *testing.T, before, after int32, strategy workloadv1alpha1.RolloutStrategyType) (*ModelServingController, *workloadv1alpha1.ModelServing, []*corev1.Pod) {
	t.Helper()
	old := createStandardModelServing("layout", 1, 1)
	old.UID = "layout-uid"
	old.Spec.Template.Roles[0].WorkerReplicas = before
	old.Spec.Template.Roles[0].WorkerTemplate = old.Spec.Template.Roles[0].EntryTemplate.DeepCopy()
	ms := old.DeepCopy()
	ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: strategy}
	ms.Spec.Template.Roles[0].WorkerReplicas = after
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "v2"
	ms.Spec.Template.Roles[0].WorkerTemplate.Spec.Containers[0].Image = "v2"
	c := newRevisionTestController(t, ms)
	t.Cleanup(c.workqueue.ShutDown)
	ctx := context.Background()
	_, err := utils.CreateControllerRevision(ctx, c.kubeClientSet, ms, "legacy", old.Spec.Template.Roles)
	require.NoError(t, err)
	var pods []*corev1.Pod
	role := old.Spec.Template.Roles[0]
	for i := 0; i <= int(before); i++ {
		var pod *corev1.Pod
		if i == 0 {
			pod = utils.GenerateEntryPod(role, ms, "layout-0", "prefill-0", "legacy", "legacy-hash")
		} else {
			pod = utils.GenerateWorkerPod(role, ms, "layout-0", "prefill-0", i, "legacy", "legacy-hash")
		}
		pod.UID = types.UID(pod.Name)
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
		require.NoError(t, c.podsInformer.GetIndexer().Add(pod))
		_, err = c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(ctx, pod.DeepCopy(), metav1.CreateOptions{})
		require.NoError(t, err)
		c.store.AddRunningPodToServingGroup(utils.GetNamespaceName(ms), "layout-0", pod.Name, "legacy", "legacy-hash", "prefill", "prefill-0")
		pods = append(pods, pod)
	}
	require.NoError(t, c.store.UpdateRoleStatus(utils.GetNamespaceName(ms), "layout-0", "prefill", "prefill-0", datastore.RoleRunning))
	require.NoError(t, c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), "layout-0", datastore.ServingGroupRunning))
	return c, ms, pods
}
func TestWorkerLayoutKeepsUnselectedInstance(t *testing.T) {
	for _, strategy := range []workloadv1alpha1.RolloutStrategyType{workloadv1alpha1.ServingGroupRollingUpdate, workloadv1alpha1.RoleRollingUpdate} {
		for _, sizes := range [][2]int32{{0, 1}, {1, 2}, {2, 1}, {1, 0}} {
			t.Run(fmt.Sprintf("%s/%d-%d", strategy, sizes[0], sizes[1]), func(t *testing.T) {
				c, ms, _ := workerLayoutFixture(t, sizes[0], sizes[1], strategy)
				client := c.kubeClientSet.(*kubefake.Clientset)
				client.ClearActions()
				ctx := c.withRevisionHistory(context.Background(), ms)
				require.NoError(t, c.manageRoleReplicasPerGroup(ctx, ms, "layout-0", ms.Spec.Template.Roles[0], 0, utils.ModelServingRevision(ms), nil, true, false))
				for _, a := range client.Actions() {
					if a.GetResource().Resource == "pods" {
						require.NotContains(t, []string{"create", "delete", "delete-collection"}, a.GetVerb())
					}
				}
				ready, err := c.checkRoleReady(ms, "layout-0", "prefill", "prefill-0")
				require.NoError(t, err)
				require.True(t, ready, "old healthy layout remains ready")
			})
		}
	}
}
func TestWorkerLayoutRecoversMissingOrdinalWithOldTemplate(t *testing.T) {
	c, ms, pods := workerLayoutFixture(t, 2, 3, workloadv1alpha1.ServingGroupRollingUpdate)
	ms.Spec.RecoveryPolicy = workloadv1alpha1.NoneRestartPolicy
	require.NoError(t, c.podsInformer.GetIndexer().Delete(pods[1]))
	require.NoError(t, c.kubeClientSet.CoreV1().Pods(ms.Namespace).Delete(context.Background(), pods[1].Name, metav1.DeleteOptions{}))
	// An extra Pod must not conceal the missing worker ordinal.
	extra := pods[2].DeepCopy()
	extra.Name = "layout-0-prefill-0-9"
	extra.UID = "extra"
	require.NoError(t, c.podsInformer.GetIndexer().Add(extra))
	require.NoError(t, c.manageRoleReplicasPerGroup(context.Background(), ms, "layout-0", ms.Spec.Template.Roles[0], 0, utils.ModelServingRevision(ms), nil, true, false))
	restored, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Get(context.Background(), pods[1].Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "legacy", utils.ObjectRevision(restored))
	require.Equal(t, "test-image:latest", restored.Spec.Containers[0].Image)
}

func TestWorkerLayoutPodGroupUsesInstanceTemplate(t *testing.T) {
	for _, strategy := range []workloadv1alpha1.RolloutStrategyType{workloadv1alpha1.ServingGroupRollingUpdate, workloadv1alpha1.RoleRollingUpdate} {
		t.Run(string(strategy), func(t *testing.T) {
			c, ms, _ := workerLayoutFixture(t, 1, 2, strategy)
			replicas := int32(2)
			ms.Spec.Template.Roles[0].Replicas = &replicas
			var projected *workloadv1alpha1.ModelServing
			c.podGroupManager = &fakePodGroupManager{createOrUpdateFunc: func(_ context.Context, p *workloadv1alpha1.ModelServing, _ string) (error, time.Duration) {
				projected = p
				return nil, 0
			}}
			require.NoError(t, c.createOrUpdatePodGroupByServingGroup(context.Background(), ms, "layout-0"))
			require.NotNil(t, projected)
			require.Equal(t, int32(1), projected.Spec.Template.Roles[0].WorkerReplicas)
			require.Equal(t, int32(2), *projected.Spec.Template.Roles[0].Replicas)
			require.Equal(t, ms.Spec.SchedulerName, projected.Spec.SchedulerName)
			require.Equal(t, int32(2), ms.Spec.Template.Roles[0].WorkerReplicas, "projection must not mutate desired spec")
		})
	}
}
func TestWorkerLayoutMixedWorkerCannotPromoteInstance(t *testing.T) {
	c, ms, pods := workerLayoutFixture(t, 1, 2, workloadv1alpha1.ServingGroupRollingUpdate)
	ctx := context.Background()
	target := utils.ModelServingRevision(ms)
	_, err := utils.CreateControllerRevision(ctx, c.kubeClientSet, ms, target, ms.Spec.Template.Roles)
	require.NoError(t, err)
	changed := pods[1].DeepCopy()
	changed.Labels[workloadv1alpha1.RevisionLabelKey] = target
	changed.Labels[workloadv1alpha1.RoleTemplateHashLabelKey] = utils.CalRoleTemplateHash(ms.Spec.Template.Roles[0])
	require.NoError(t, c.podsInformer.GetIndexer().Update(changed))
	c.store.AddRole(utils.GetNamespaceName(ms), "layout-0", "prefill", "prefill-0", target, utils.CalRoleTemplateHash(ms.Spec.Template.Roles[0]))
	groups, err := c.store.GetServingGroupByModelServing(utils.GetNamespaceName(ms))
	require.NoError(t, err)
	require.Equal(t, templateDifferent, c.compareServingGroupTemplate(ctx, ms, groups[0], target))
	ready, err := c.checkRoleReady(ms, "layout-0", "prefill", "prefill-0")
	require.NoError(t, err)
	require.False(t, ready)
}
