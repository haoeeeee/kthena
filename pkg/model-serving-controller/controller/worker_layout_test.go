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
	kthenafake "github.com/volcano-sh/kthena/client-go/clientset/versioned/fake"
	api "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
	corev1 "k8s.io/api/core/v1"
	apiextfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func workerLayoutFixture(t *testing.T, before, after int32, strategy api.RolloutStrategyType) (*ModelServingController, *api.ModelServing, *api.ModelServing) {
	t.Helper()
	old := createStandardModelServing("layout", 1, 3)
	old.UID = "layout-owner"
	old.Spec.RolloutStrategy = &api.RolloutStrategy{Type: strategy}
	old.Spec.Template.Roles[0].WorkerReplicas = before
	old.Spec.Template.Roles[0].WorkerTemplate = old.Spec.Template.Roles[0].EntryTemplate.DeepCopy()
	old.Spec.Template.Roles[0].MaxUnavailable = ptr.To(intstr.FromInt(1))
	ms := old.DeepCopy()
	ms.Spec.Template.Roles[0].WorkerReplicas = after
	ms.Status.CurrentRevision = utils.ModelServingRevision(old)
	ms.Status.UpdateRevision = utils.ModelServingRevision(ms)
	c, err := newControllerWithRevisionHistory(t, kubefake.NewSimpleClientset(), kthenafake.NewSimpleClientset(ms.DeepCopy()), nil, apiextfake.NewSimpleClientset())
	require.NoError(t, err)
	c.podGroupManager = &fakePodGroupManager{}
	t.Cleanup(c.workqueue.ShutDown)
	require.NoError(t, c.modelServingsInformer.GetIndexer().Add(ms))
	for _, version := range []*api.ModelServing{old, ms} {
		_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, revisiontest.ControllerRevisions(c.kubeClientSet), ms, utils.ModelServingRevision(version), version.Spec.Template.Roles)
		require.NoError(t, err)
	}
	for i := 0; i < 3; i++ {
		addWorkerLayoutInstance(t, c, old, i, true)
	}
	require.NoError(t, c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), "layout-0", datastore.ServingGroupRunning))
	return c, old, ms
}

func addWorkerLayoutInstance(t *testing.T, c *ModelServingController, ms *api.ModelServing, ordinal int, ready bool) []*corev1.Pod {
	t.Helper()
	role := ms.Spec.Template.Roles[0]
	revision, hash := utils.ModelServingRevision(ms), utils.CalRoleTemplateHash(role)
	id := utils.GenerateRoleID(role.Name, ordinal)
	entry := utils.GenerateEntryPod(*role.DeepCopy(), ms, "layout-0", id, revision, hash)
	pods := []*corev1.Pod{entry}
	for i := 1; i <= int(role.WorkerReplicas); i++ {
		pods = append(pods, utils.GenerateWorkerPod(*role.DeepCopy(), ms, "layout-0", id, i, revision, hash))
	}
	for _, pod := range pods {
		pod.UID = types.UID(pod.Name + "-" + revision)
		pod.Status.Phase = corev1.PodRunning
		if ready {
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		}
		require.NoError(t, c.podsInformer.GetIndexer().Add(pod))
		_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(context.Background(), pod.DeepCopy(), metav1.CreateOptions{})
		require.NoError(t, err)
		c.store.AddRunningPodToServingGroup(utils.GetNamespaceName(ms), "layout-0", pod.Name, revision, hash, role.Name, id)
	}
	if ready {
		require.NoError(t, c.store.UpdateRoleStatus(utils.GetNamespaceName(ms), "layout-0", role.Name, id, datastore.RoleRunning))
	}
	return pods
}

func TestWorkerLayoutKeepsUnselectedInstances(t *testing.T) {
	for _, strategy := range []api.RolloutStrategyType{api.RoleRollingUpdate, api.ServingGroupRollingUpdate} {
		for _, sizes := range [][2]int32{{0, 1}, {1, 2}, {2, 1}, {1, 0}} {
			for _, partition := range []int{0, 1, 3} {
				t.Run(fmt.Sprintf("%s/%d-%d/partition-%d", strategy, sizes[0], sizes[1], partition), func(t *testing.T) {
					c, old, ms := workerLayoutFixture(t, sizes[0], sizes[1], strategy)
					ms.Spec.Template.Roles[0].Partition = ptr.To(intstr.FromInt(partition))
					client := c.kubeClientSet.(*kubefake.Clientset)
					client.ClearActions()
					require.NoError(t, c.manageRoleReplicasPerGroup(context.Background(), ms, "layout-0", ms.Spec.Template.Roles[0], 0, utils.ModelServingRevision(ms), nil, true))
					for _, action := range client.Actions() {
						if action.GetResource().Resource == "pods" {
							require.NotContains(t, []string{"create", "delete", "delete-collection"}, action.GetVerb())
						}
					}
					for i := 0; i < 3; i++ {
						ready, err := c.checkRoleReady(ms, "layout-0", "prefill", utils.GenerateRoleID("prefill", i))
						require.NoError(t, err)
						require.True(t, ready)
					}
					projected, err := c.modelServingForPodGroup(context.Background(), ms, "layout-0")
					require.NoError(t, err)
					require.Equal(t, sizes[0], projected.Spec.Template.Roles[0].WorkerReplicas)
					require.Equal(t, sizes[1], ms.Spec.Template.Roles[0].WorkerReplicas)
					cr, err := utils.GetControllerRevision(context.Background(), c.kubeClientSet, ms, utils.ModelServingRevision(old))
					require.NoError(t, err)
					roles, err := utils.GetRolesFromControllerRevision(cr)
					require.NoError(t, err)
					require.Equal(t, old.Spec.Template.Roles, roles, "history must remain immutable")
				})
			}
		}
	}
}

func TestWorkerLayoutRestoresMissingOrdinal(t *testing.T) {
	c, old, ms := workerLayoutFixture(t, 2, 1, api.RoleRollingUpdate)
	ms.Spec.RecoveryPolicy = api.NoneRestartPolicy
	ctx := context.Background()
	missing, err := c.podsLister.Pods(ms.Namespace).Get("layout-0-prefill-0-1")
	require.NoError(t, err)
	require.NoError(t, c.podsInformer.GetIndexer().Delete(missing))
	require.NoError(t, c.kubeClientSet.CoreV1().Pods(ms.Namespace).Delete(ctx, missing.Name, metav1.DeleteOptions{}))
	extra := missing.DeepCopy()
	extra.Name = "layout-0-prefill-0-9"
	require.NoError(t, c.podsInformer.GetIndexer().Add(extra))
	require.NoError(t, c.manageRoleReplicasPerGroup(ctx, ms, "layout-0", ms.Spec.Template.Roles[0], 0, utils.ModelServingRevision(ms), nil, true))
	restored, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Get(ctx, missing.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, utils.ModelServingRevision(old), utils.ObjectRevision(restored))
	require.Equal(t, utils.CalRoleTemplateHash(old.Spec.Template.Roles[0]), utils.ObjectRoleTemplateHash(restored))
}

func TestWorkerLayoutUsesEntryAfterRestart(t *testing.T) {
	c, old, ms := workerLayoutFixture(t, 1, 2, api.RoleRollingUpdate)
	// Replaying the worker first must not replace the entry's instance identity.
	c.store = datastore.New()
	worker, err := c.podsLister.Pods(ms.Namespace).Get("layout-0-prefill-0-1")
	require.NoError(t, err)
	worker = worker.DeepCopy()
	worker.Labels[api.RevisionLabelKey] = utils.ModelServingRevision(ms)
	worker.Labels[api.RoleTemplateHashLabelKey] = utils.CalRoleTemplateHash(ms.Spec.Template.Roles[0])
	require.NoError(t, c.podsInformer.GetIndexer().Update(worker))
	c.store.AddRole(utils.GetNamespaceName(ms), "layout-0", "prefill", "prefill-0", utils.ModelServingRevision(ms), utils.CalRoleTemplateHash(ms.Spec.Template.Roles[0]))
	roles, err := c.store.GetRoleList(utils.GetNamespaceName(ms), "layout-0", "prefill")
	require.NoError(t, err)
	pods, err := c.getPodsByIndex(RoleIDKey, ms.Namespace+"/layout-0/prefill/prefill-0")
	require.NoError(t, err)
	template, revision, _, err := c.roleTemplateForInstance(context.Background(), ms, "layout-0", "prefill", roles[0], pods)
	require.NoError(t, err)
	require.Equal(t, int32(1), template.WorkerReplicas)
	require.Equal(t, utils.ModelServingRevision(old), revision)
	require.Equal(t, templateDifferent, c.compareRoleTemplate(context.Background(), ms, datastore.ServingGroup{Name: "layout-0", Revision: utils.ModelServingRevision(ms)}, "prefill", roles[0]))
	ready, err := c.checkRoleReady(ms, "layout-0", "prefill", "prefill-0")
	require.NoError(t, err)
	require.False(t, ready, "mixed-version pods are not a healthy old or new Role")
}

func TestWorkerLayoutUnknownHistoryBlocksRepair(t *testing.T) {
	for _, failure := range []string{"missing", "foreign", "malformed"} {
		t.Run(failure, func(t *testing.T) {
			c, old, ms := workerLayoutFixture(t, 0, 1, api.RoleRollingUpdate)
			ctx := context.Background()
			cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, utils.ModelServingRevision(old))
			require.NoError(t, err)
			switch failure {
			case "missing":
				require.NoError(t, c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Delete(ctx, cr.Name, metav1.DeleteOptions{}))
			case "foreign":
				cr.OwnerReferences[0].UID = "another-owner"
			case "malformed":
				cr.Data.Raw = []byte(`{"data":"invalid"}`)
			}
			if failure != "missing" {
				_, err = c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, cr, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			client := c.kubeClientSet.(*kubefake.Clientset)
			client.ClearActions()
			require.Error(t, c.manageRoleReplicasPerGroup(ctx, ms, "layout-0", ms.Spec.Template.Roles[0], 0, utils.ModelServingRevision(ms), nil, true))
			for _, action := range client.Actions() {
				require.NotEqual(t, "pods", action.GetResource().Resource)
			}
		})
	}
}

func TestWorkerLayoutMixedSchedulingAndBudget(t *testing.T) {
	for _, sizes := range [][2]int32{{0, 1}, {2, 1}, {1, 0}} {
		t.Run(fmt.Sprint(sizes), func(t *testing.T) {
			c, _, ms := workerLayoutFixture(t, sizes[0], sizes[1], api.RoleRollingUpdate)
			ms.Spec.Template.Roles[0].MaxSurge = ptr.To(intstr.FromInt(1))
			ms.Spec.Template.Roles[0].MaxUnavailable = ptr.To(intstr.FromInt(0))
			addWorkerLayoutInstance(t, c, ms, 3, false)
			projected, err := c.modelServingForPodGroup(context.Background(), ms, "layout-0")
			require.NoError(t, err)
			require.Equal(t, min(sizes[0], sizes[1]), projected.Spec.Template.Roles[0].WorkerReplicas)
			require.Equal(t, int32(3), *projected.Spec.Template.Roles[0].Replicas)
			groups, err := c.store.GetServingGroupByModelServing(utils.GetNamespaceName(ms))
			require.NoError(t, err)
			selected, outdated, err := c.rolesToDeleteForRoleRollingUpdate(context.Background(), ms, groups[0], nil)
			require.NoError(t, err)
			require.True(t, outdated)
			require.Empty(t, selected, "the new incomplete Role must consume the surge budget")
		})
	}
}

func TestWorkerLayoutBudgetAfterGroupRevisionReplay(t *testing.T) {
	c, _, ms := workerLayoutFixture(t, 0, 1, api.RoleRollingUpdate)
	key := utils.GetNamespaceName(ms)
	require.NoError(t, c.store.UpdateServingGroupRevision(key, "layout-0", utils.ModelServingRevision(ms)))
	for attempt := 0; attempt < 2; attempt++ {
		require.NoError(t, c.manageRollingUpdate(context.Background(), ms, utils.ModelServingRevision(ms), &roleRolloutPolicy{}))
		roles, err := c.store.GetRoleList(key, "layout-0", "prefill")
		require.NoError(t, err)
		deleting := 0
		for _, role := range roles {
			if role.Status == datastore.RoleDeleting {
				deleting++
			}
		}
		require.Equal(t, 1, deleting, "the first replayed group revision cannot skip rollout or spend its budget twice")
	}
}

func TestWorkerLayoutReadinessRetriesAfterHistoryRecovery(t *testing.T) {
	c, old, ms := workerLayoutFixture(t, 0, 1, api.RoleRollingUpdate)
	ctx := context.Background()
	cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, utils.ModelServingRevision(old))
	require.NoError(t, err)
	require.NoError(t, c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Delete(ctx, cr.Name, metav1.DeleteOptions{}))
	require.NoError(t, c.store.UpdateRoleStatus(utils.GetNamespaceName(ms), "layout-0", "prefill", "prefill-0", datastore.RoleCreating))
	ready, err := c.checkRoleReady(ms, "layout-0", "prefill", "prefill-0")
	require.Error(t, err)
	require.False(t, ready)
	_, err = c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Create(ctx, cr, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, c.manageRoleReplicasPerGroup(ctx, ms, "layout-0", ms.Spec.Template.Roles[0], 0, utils.ModelServingRevision(ms), nil, true))
	require.Equal(t, datastore.RoleRunning, c.store.GetRoleStatus(utils.GetNamespaceName(ms), "layout-0", "prefill", "prefill-0"))
}

func TestWorkerLayoutKeepsReplicaScalingIndependent(t *testing.T) {
	for _, replicas := range []int32{0, 1, 4} {
		t.Run(fmt.Sprint(replicas), func(t *testing.T) {
			c, _, ms := workerLayoutFixture(t, 0, 1, api.RoleRollingUpdate)
			revision := utils.ModelServingRevision(ms)
			ms.Spec.Template.Roles[0].Replicas = &replicas
			require.Equal(t, revision, utils.ModelServingRevision(ms))
			require.NoError(t, c.manageRoleReplicasPerGroup(context.Background(), ms, "layout-0", ms.Spec.Template.Roles[0], 0, revision, nil, true))
			roles, err := c.store.GetRoleList(utils.GetNamespaceName(ms), "layout-0", "prefill")
			require.NoError(t, err)
			active := 0
			for _, role := range roles {
				if role.Status != datastore.RoleDeleting {
					active++
				}
			}
			require.Equal(t, int(replicas), active)
		})
	}
}

func TestWorkerLayoutPersistsRoleRevisionBeforeRollout(t *testing.T) {
	c, _, ms := workerLayoutFixture(t, 0, 1, api.RoleRollingUpdate)
	ctx := context.Background()
	revision := utils.ModelServingRevision(ms)
	require.NoError(t, c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Delete(ctx, utils.GenerateControllerRevisionName(ms.Name, revision), metav1.DeleteOptions{}))
	require.NoError(t, c.syncModelServing(ctx, ms.Namespace+"/"+ms.Name))
	cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, revision)
	require.NoError(t, err)
	require.NotNil(t, cr)
	roles, err := utils.GetRolesFromControllerRevision(cr)
	require.NoError(t, err)
	require.Equal(t, int32(1), roles[0].WorkerReplicas)
}

func TestWorkerLayoutPreservesLiveHistoryAfterGroupRevisionReplay(t *testing.T) {
	c, old, ms := workerLayoutFixture(t, 1, 2, api.RoleRollingUpdate)
	ctx := context.Background()
	// History cleanup waits for rollout completion. The old instance
	// revision must survive even when the SG has advanced ahead of its Roles.
	unused := old.DeepCopy()
	unused.Spec.Template.Roles[0].WorkerReplicas = 0
	_, err := utils.CreateControllerRevision(ctx, c.kubeClientSet, revisiontest.ControllerRevisions(c.kubeClientSet), ms, utils.ModelServingRevision(unused), unused.Spec.Template.Roles)
	require.NoError(t, err)
	revision := utils.ModelServingRevision(ms)
	require.NoError(t, c.store.UpdateServingGroupRevision(utils.GetNamespaceName(ms), "layout-0", revision))
	require.NoError(t, c.UpdateModelServingStatus(ms, revision))
	latest, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(ctx, ms.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, utils.ModelServingRevision(old), latest.Status.CurrentRevision, "old live Roles prevent rollout completion")
	cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, utils.ModelServingRevision(old))
	require.NoError(t, err)
	require.NotNil(t, cr, "live old Pods still need their layout snapshot")
	cr, err = utils.GetControllerRevision(ctx, c.kubeClientSet, ms, utils.ModelServingRevision(unused))
	require.NoError(t, err)
	require.NotNil(t, cr, "production defers history cleanup until the rollout completes")
}

func TestNewGroupRolePartitionMatchesRoleScaleUp(t *testing.T) {
	for _, sizes := range [][2]int32{{1, 0}, {0, 1}} {
		for _, partition := range []intstr.IntOrString{intstr.FromInt(0), intstr.FromInt(1), intstr.FromString("50%"), intstr.FromString("100%")} {
			for _, newGroup := range []bool{false, true} {
				t.Run(fmt.Sprintf("workers-%d-%d/partition-%s/new-group-%t", sizes[0], sizes[1], partition.String(), newGroup), func(t *testing.T) {
					ctx := context.Background()
					old := createStandardModelServing("partition-create", 1, 0)
					old.UID = "partition-create-owner"
					old.Spec.RolloutStrategy = &api.RolloutStrategy{Type: api.RoleRollingUpdate}
					old.Spec.Template.Roles[0].WorkerReplicas = sizes[0]
					old.Spec.Template.Roles[0].WorkerTemplate = old.Spec.Template.Roles[0].EntryTemplate.DeepCopy()
					ms := old.DeepCopy()
					ms.Spec.Replicas = ptr.To[int32](2)
					role := &ms.Spec.Template.Roles[0]
					role.Replicas = ptr.To[int32](3)
					role.Partition = &partition
					role.WorkerReplicas = sizes[1]
					role.EntryTemplate.Spec.Containers[0].Image = "test-image:target"
					oldRevision, targetRevision := utils.ModelServingRevision(old), utils.ModelServingRevision(ms)
					ms.Status.CurrentRevision = oldRevision
					ms.Status.UpdateRevision = targetRevision
					c := newRevisionTestController(t, ms)
					t.Cleanup(c.workqueue.ShutDown)
					var projected *api.ModelServing
					c.podGroupManager = &fakePodGroupManager{createOrUpdateFunc: func(_ context.Context, pgMS *api.ModelServing, _ string) (error, time.Duration) {
						projected = pgMS.DeepCopy()
						return nil, 0
					}}
					for _, version := range []*api.ModelServing{old, ms} {
						_, err := utils.CreateControllerRevision(ctx, c.kubeClientSet, revisiontest.ControllerRevisions(c.kubeClientSet), version, utils.ModelServingRevision(version), version.Spec.Template.Roles)
						require.NoError(t, err)
					}
					key := utils.GetNamespaceName(ms)
					c.store.AddServingGroup(key, 0, oldRevision)
					groupName := "partition-create-0"
					if newGroup {
						groupName = "partition-create-1"
						require.NoError(t, c.scaleUpServingGroups(ctx, ms, []datastore.ServingGroup{{Name: "partition-create-0", Revision: oldRevision}}, 2, targetRevision))
					} else {
						require.NoError(t, c.scaleUpRoles(ctx, ms, groupName, *role, nil, 3, 0, targetRevision, true))
					}
					boundary, _, err := c.getPartition(&partition, 3)
					require.NoError(t, err)
					for ordinal := 0; ordinal < 3; ordinal++ {
						applied, revision := *role, targetRevision
						if ordinal < boundary {
							applied, revision = old.Spec.Template.Roles[0], oldRevision
						}
						roleID := utils.GenerateRoleID(role.Name, ordinal)
						pods, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).List(ctx, metav1.ListOptions{LabelSelector: api.GroupNameLabelKey + "=" + groupName + "," + api.RoleIDKey + "=" + roleID})
						require.NoError(t, err)
						require.Len(t, pods.Items, 1+int(applied.WorkerReplicas))
						for _, pod := range pods.Items {
							require.Equal(t, revision, utils.ObjectRevision(&pod))
							require.Equal(t, utils.CalRoleTemplateHash(applied), utils.ObjectRoleTemplateHash(&pod))
							if pod.Name == utils.GeneratePodName(groupName, roleID, 0) {
								require.Equal(t, applied.EntryTemplate.Spec.Containers[0].Image, pod.Spec.Containers[0].Image)
							}
						}
					}
					instances, err := c.store.GetRoleList(key, groupName, role.Name)
					require.NoError(t, err)
					require.Len(t, instances, 3, "historical replica count zero must not replace the current scale target")
					for _, instance := range instances {
						_, ordinal := utils.GetParentNameAndOrdinal(instance.Name)
						applied, revision := *role, targetRevision
						if ordinal < boundary {
							applied, revision = old.Spec.Template.Roles[0], oldRevision
						}
						require.Equal(t, revision, instance.Revision)
						require.Equal(t, utils.CalRoleTemplateHash(applied), instance.RoleTemplateHash)
					}
					if newGroup {
						expectedWorkers := sizes[1]
						if boundary == 3 {
							expectedWorkers = sizes[0]
						} else if boundary > 0 {
							expectedWorkers = min(sizes[0], sizes[1])
						}
						require.NotNil(t, projected)
						require.Equal(t, expectedWorkers, projected.Spec.Template.Roles[0].WorkerReplicas)
						require.Equal(t, int32(3), *projected.Spec.Template.Roles[0].Replicas)
					}
				})
			}
		}
	}
}

func TestNewGroupRolePartitionHistory(t *testing.T) {
	for _, baseline := range []string{"missing", "completed", "initial"} {
		t.Run(baseline, func(t *testing.T) {
			ms := createStandardModelServing("partition-history", 1, 1)
			ms.UID = "partition-history-owner"
			ms.Spec.RolloutStrategy = &api.RolloutStrategy{Type: api.RoleRollingUpdate}
			ms.Spec.Template.Roles[0].Partition = ptr.To(intstr.FromInt(1))
			revision := utils.ModelServingRevision(ms)
			switch baseline {
			case "missing":
				ms.Status.CurrentRevision = "missing-history"
			case "completed":
				ms.Status.CurrentRevision = revision
			}
			c := newRevisionTestController(t, ms)
			t.Cleanup(c.workqueue.ShutDown)
			c.podGroupManager = &fakePodGroupManager{}
			ctx := context.Background()
			err := c.scaleUpServingGroups(ctx, ms, nil, 1, revision)
			if baseline == "missing" {
				require.Error(t, err, "unknown protected history must not create target-version Pods")
			} else {
				require.NoError(t, err)
			}
			pods, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			if baseline == "missing" {
				require.Empty(t, pods.Items)
			} else {
				require.Len(t, pods.Items, 1)
				require.Equal(t, revision, utils.ObjectRevision(&pods.Items[0]))
			}
		})
	}
}
