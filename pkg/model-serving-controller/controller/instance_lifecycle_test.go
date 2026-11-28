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
	api "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
	corev1 "k8s.io/api/core/v1"
	extensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
)

func TestInstanceLifecycle_GraceRevalidatesLiveState(t *testing.T) {
	for _, change := range []string{"none", "infinite", "extend", "ready", "plain-notready", "pod-uid", "ms-uid", "get-failure", "delete-conflict", "recover"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			ms := lifecycleMS("live-grace", 1, 1, 0)
			ms.ResourceVersion = "1"
			c := lifecycleController(t, ms)
			pod := lifecyclePod(t, c, ms, ms, 0, "failed-uid")
			pod.ResourceVersion = "10"
			pod.Status.Conditions[0].Status = corev1.ConditionFalse
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", RestartCount: 1}}
			require.NoError(t, c.podsInformer.GetIndexer().Update(pod.DeepCopy()))
			latestPod, latestMS := pod.DeepCopy(), ms.DeepCopy()
			switch change {
			case "none":
				latestMS.Spec.RecoveryPolicy = api.NoneRestartPolicy
			case "infinite":
				latestMS.Spec.Template.RestartGracePeriodSeconds = ptr.To[int64](-1)
			case "extend":
				latestMS.Spec.Template.RestartGracePeriodSeconds = ptr.To[int64](120)
			case "ready":
				latestPod.Status.Conditions[0].Status = corev1.ConditionTrue
			case "plain-notready":
				latestPod.Status.ContainerStatuses = nil
			case "pod-uid":
				latestPod.UID = "replacement"
			case "ms-uid":
				latestMS.UID = "replacement"
			}
			_, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Update(ctx, latestMS, metav1.UpdateOptions{})
			require.NoError(t, err)
			_, err = c.kubeClientSet.CoreV1().Pods(ms.Namespace).Update(ctx, latestPod, metav1.UpdateOptions{})
			require.NoError(t, err)
			kube := c.kubeClientSet.(*kubefake.Clientset)
			if change == "get-failure" {
				kube.PrependReactor("get", "pods", func(kubetesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewServiceUnavailable("injected GET failure")
				})
			}
			if change == "delete-conflict" {
				kube.PrependReactor("delete", "pods", func(a kubetesting.Action) (bool, runtime.Object, error) {
					pre := a.(kubetesting.DeleteAction).GetDeleteOptions().Preconditions
					require.Equal(t, pod.UID, *pre.UID)
					require.Equal(t, pod.ResourceVersion, *pre.ResourceVersion)
					return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, pod.Name, fmt.Errorf("Ready changed after GET"))
				})
			}
			started := time.Now().Add(-time.Minute)
			c.graceMap.Store(getPodGracePeriodKey(pod), started)
			kube.ClearActions()
			delay, err := c.recoverPodAfterGrace(ctx, ms, pod, started)
			if change == "get-failure" || change == "delete-conflict" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if change == "extend" {
				require.Greater(t, delay, time.Duration(0))
			}
			require.Equal(t, change == "recover" || change == "delete-conflict", lifecycleDeletionMatches(c, pod))
		})
	}
}

func TestInstanceLifecycle_GraceStopsWithController(t *testing.T) {
	ms := lifecycleMS("stopped-grace", 1, 1, 0)
	c := lifecycleController(t, ms)
	pod := lifecyclePod(t, c, ms, ms, 0, "failed-uid")
	ctx, cancel := context.WithCancel(context.Background())
	c.recoveryCtx = ctx
	started := time.Now().Add(-time.Minute)
	c.graceMap.Store(getPodGracePeriodKey(pod), started)
	cancel()
	c.handlePodAfterGraceTime(ms, pod, started)
	require.False(t, lifecycleDeletionMatches(c, pod))
	_, pending := c.graceMap.Load(getPodGracePeriodKey(pod))
	require.False(t, pending)
}

func TestInstanceLifecycle_RenderingCannotSupplyLifecycleEvidence(t *testing.T) {
	for _, plugin := range []bool{false, true} {
		t.Run(fmt.Sprintf("plugin=%t", plugin), func(t *testing.T) {
			ctx := context.Background()
			ms := lifecycleMS("metadata-intent", 1, 1, 0)
			ms.ResourceVersion = "1"
			role := &ms.Spec.Template.Roles[0]
			role.WorkerReplicas = 1
			role.EntryTemplate.Metadata = &api.Metadata{Annotations: map[string]string{
				roleCreatedAnnotation:   "true",
				deletionScopeAnnotation: deleteGroupScope,
				groupInstanceAnnotation: "template-group", roleInstanceAnnotation: "template-role",
				surgeAnnotation: surgeServingGroup, "custom.example/config": "preserved",
			}}
			role.WorkerTemplate = role.EntryTemplate.DeepCopy()
			if plugin {
				ms.Spec.Plugins = []api.PluginSpec{{Name: "demo-pod-tweaks", Type: api.PluginTypeBuiltIn, Config: &extensions.JSON{Raw: []byte(`{"annotations":{"workload.kthena.io/deletion-scope":"ServingGroup","workload.kthena.io/role-created":"true","workload.kthena.io/group-instance":"plugin-group","workload.kthena.io/role-instance":"plugin-role","modelserving.volcano.sh/surge":"serving-group","custom.example/plugin":"preserved"}}`)}}}
			}
			c := lifecycleController(t, ms)
			require.NoError(t, c.CreatePodsByRole(ctx, *role.DeepCopy(), ms, 0, 0, utils.ModelServingRevision(ms), utils.CalRoleTemplateHash(*role), ""))
			created := 0
			for _, action := range c.kubeClientSet.(*kubefake.Clientset).Actions() {
				if !action.Matches("create", "pods") {
					continue
				}
				created++
				pod := action.(kubetesting.CreateAction).GetObject().(*corev1.Pod)
				require.Empty(t, pod.Annotations[roleCreatedAnnotation], "completion must follow actual creation")
				require.Empty(t, pod.Annotations[deletionScopeAnnotation], "templates/plugins cannot authorize deletion")
				require.Empty(t, pod.Annotations[surgeAnnotation])
				require.NotEmpty(t, pod.Annotations[groupInstanceAnnotation])
				require.NotContains(t, []string{"template-group", "plugin-group"}, pod.Annotations[groupInstanceAnnotation])
				require.NotContains(t, []string{"template-role", "plugin-role"}, pod.Annotations[roleInstanceAnnotation])
				require.Equal(t, "preserved", pod.Annotations["custom.example/config"])
				if plugin {
					require.Equal(t, "preserved", pod.Annotations["custom.example/plugin"])
				}
			}
			require.Equal(t, 2, created, "both entry and worker must be checked")
		})
	}
}

func lifecycleMS(name string, n, u, s int32) *api.ModelServing {
	ms := createStandardModelServing(name, n, 1)
	ms.UID = types.UID(name + "-uid")
	ms.Generation = 1
	ms.Spec.RolloutStrategy = &api.RolloutStrategy{Type: api.ServingGroupRollingUpdate,
		RollingUpdateConfiguration: &api.RollingUpdateConfiguration{
			MaxUnavailable: ptr.To(intstr.FromInt32(u)), MaxSurge: ptr.To(intstr.FromInt32(s)),
		}}
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "test:v1"
	return ms
}

func lifecycleController(t *testing.T, ms *api.ModelServing, histories ...*api.ModelServing) *ModelServingController {
	t.Helper()
	c := newRevisionTestController(t, ms)
	c.recorder = record.NewFakeRecorder(1000)
	c.initialSync.Store(true)
	t.Cleanup(c.workqueue.ShutDown)
	for _, historical := range append(histories, ms) {
		_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, revisiontest.ControllerRevisions(c.kubeClientSet), historical, utils.ModelServingRevision(historical), historical.Spec.Template.Roles)
		require.NoError(t, err)
	}
	return c
}

func lifecyclePod(t *testing.T, c *ModelServingController, ms, template *api.ModelServing, group int, uid string) *corev1.Pod {
	t.Helper()
	role := *template.Spec.Template.Roles[0].DeepCopy()
	groupName := utils.GenerateServingGroupName(ms.Name, group)
	hash, revision := utils.CalRoleTemplateHash(role), utils.ModelServingRevision(template)
	pod := utils.GenerateEntryPod(role, ms, groupName, "prefill-0", revision, hash)
	pod.UID = types.UID(uid)
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
	_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, c.podsInformer.GetIndexer().Add(pod))
	key := utils.GetNamespaceName(ms)
	c.store.AddRunningPodToServingGroup(key, groupName, pod.Name, revision, hash, "prefill", "prefill-0")
	require.NoError(t, c.store.UpdateRoleStatus(key, groupName, "prefill", "prefill-0", datastore.RoleRunning))
	require.NoError(t, c.store.UpdateServingGroupStatus(key, groupName, datastore.ServingGroupRunning))
	return pod
}

func lifecycleDeletionMatches(c *ModelServingController, pod *corev1.Pod) bool {
	for _, action := range c.kubeClientSet.(*kubefake.Clientset).Actions() {
		if action.Matches("delete", "pods") && action.(kubetesting.DeleteAction).GetName() == pod.Name {
			return true
		}
		if action.Matches("delete-collection", "pods") {
			if action.(kubetesting.DeleteCollectionAction).GetListRestrictions().Labels.Matches(labels.Set(pod.Labels)) {
				return true
			}
		}
	}
	return false
}

func TestInstanceLifecycle_LateDeleteTargetsHealthyReplacement(t *testing.T) {
	for _, policy := range []api.RecoveryPolicy{api.RoleRecreate, api.ServingGroupRecreate, api.NoneRestartPolicy} {
		for _, sameRevision := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/same-revision=%t", policy, sameRevision), func(t *testing.T) {
				ms := lifecycleMS("late-delete", 1, 0, 1)
				ms.Spec.RecoveryPolicy = policy
				c := lifecycleController(t, ms)
				current := lifecyclePod(t, c, ms, ms, 0, "new-healthy-uid")
				old := current.DeepCopy()
				old.UID = "old-deleted-uid"
				if !sameRevision {
					old.Labels[api.RevisionLabelKey] = "previous-revision"
				}
				c.kubeClientSet.(*kubefake.Clientset).ClearActions()
				c.deletePod(old)
				matched := lifecycleDeletionMatches(c, current)
				require.False(t, matched)
				count, err := c.store.GetRunningPodNumByServingGroup(utils.GetNamespaceName(ms), "late-delete-0")
				require.NoError(t, err)
				require.Equal(t, 1, count, "old event must preserve the replacement Ready membership")
				t.Logf("oldUID=%s currentUID=%s currentReady=true currentTarget=true U=0 destructiveSelectorMatchesNewUID=%t", old.UID, current.UID, matched)
			})
		}
	}
}

func TestInstanceLifecycle_LateReadyResurrectsOldIdentityThenRollsNewPod(t *testing.T) {
	for _, mode := range []api.RolloutStrategyType{api.ServingGroupRollingUpdate, api.RoleRollingUpdate} {
		t.Run(string(mode), func(t *testing.T) {
			oldMS := lifecycleMS("late-ready", 1, 1, 0)
			ms := oldMS.DeepCopy()
			ms.Spec.RolloutStrategy.Type = mode
			oldMS.Spec.RolloutStrategy.Type = mode
			ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "test:v2"
			ms.Spec.Template.Roles[0].MaxUnavailable = ptr.To(intstr.FromInt(1))
			c := lifecycleController(t, ms, oldMS)
			current := lifecyclePod(t, c, ms, ms, 0, "new-v2-uid")
			old := current.DeepCopy()
			old.UID = "old-v1-uid"
			old.Labels[api.RevisionLabelKey] = utils.ModelServingRevision(oldMS)
			old.Labels[api.RoleTemplateHashLabelKey] = utils.CalRoleTemplateHash(oldMS.Spec.Template.Roles[0])
			key := utils.GetNamespaceName(ms)
			c.store.DeleteServingGroup(key, "late-ready-0")
			// The informer already contains the replacement, but an old Ready callback is queued.
			require.NoError(t, c.handleReadyPod(ms, "late-ready-0", old))
			c.store.AddServingGroup(key, 0, utils.ModelServingRevision(ms))
			c.store.AddRole(key, "late-ready-0", "prefill", "prefill-0", utils.ModelServingRevision(ms), utils.CalRoleTemplateHash(ms.Spec.Template.Roles[0]))
			groups, err := c.store.GetServingGroupByModelServing(key)
			require.NoError(t, err)
			require.Equal(t, utils.ModelServingRevision(ms), groups[0].Revision)
			require.Equal(t, templateEquivalent, c.compareServingGroupTemplate(context.Background(), ms, groups[0], utils.ModelServingRevision(ms)))
			c.kubeClientSet.(*kubefake.Clientset).ClearActions()
			require.NoError(t, c.manageRollingUpdate(context.Background(), ms, utils.ModelServingRevision(ms), nil))
			require.False(t, lifecycleDeletionMatches(c, current))
			t.Logf("liveUID=%s liveVersion=v2 cachedVersion=v2 comparison=Equivalent deletion=false", current.UID)
		})
	}
}

func TestInstanceLifecycle_QueuedReadyRevivesDeletingGroup(t *testing.T) {
	ms := lifecycleMS("revive", 1, 1, 0)
	c := lifecycleController(t, ms)
	oldReady := lifecyclePod(t, c, ms, ms, 0, "terminating-uid")
	terminating := oldReady.DeepCopy()
	now := metav1.Now()
	terminating.DeletionTimestamp = &now
	require.NoError(t, c.podsInformer.GetIndexer().Update(terminating))
	key := utils.GetNamespaceName(ms)
	require.NoError(t, c.store.UpdateServingGroupStatus(key, "revive-0", datastore.ServingGroupDeleting))
	require.NoError(t, c.handleReadyPod(ms, "revive-0", oldReady))
	require.Equal(t, datastore.ServingGroupDeleting, c.store.GetServingGroupStatus(key, "revive-0"))
	t.Log("latest Pod terminating; queued Ready event preserves Deleting lifecycle fence")
}

func TestInstanceLifecycleRemovedWorkerCannotRecoverReplacement(t *testing.T) {
	for _, policy := range []api.RecoveryPolicy{api.RoleRecreate, api.ServingGroupRecreate} {
		t.Run(string(policy), func(t *testing.T) {
			ms := lifecycleMS("old-worker", 1, 1, 0)
			ms.Spec.RecoveryPolicy = policy
			c := lifecycleController(t, ms)
			current := lifecyclePod(t, c, ms, ms, 0, "new-entry")
			current.Annotations = map[string]string{groupInstanceAnnotation: "same-group", roleInstanceAnnotation: "new-role"}
			require.NoError(t, c.podsInformer.GetIndexer().Update(current))
			oldWorker := current.DeepCopy()
			oldWorker.Name = "old-worker-0-prefill-0-1"
			oldWorker.UID = "retired-worker"
			oldWorker.Annotations[roleInstanceAnnotation] = "old-role"
			c.kubeClientSet.(*kubefake.Clientset).ClearActions()
			c.deletePod(oldWorker)
			require.False(t, lifecycleDeletionMatches(c, current), "changing recovery scope cannot make an old Role incarnation fault a replacement")
		})
	}
}

func TestInstanceLifecycleSnapshotExpiresWhenSlotIsRefilled(t *testing.T) {
	for _, mode := range []api.RolloutStrategyType{api.ServingGroupRollingUpdate, api.RoleRollingUpdate} {
		for _, ready := range []bool{false, true} {
			for _, alreadyExists := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/ready=%t/already-exists=%t", mode, ready, alreadyExists), func(t *testing.T) {
					oldMS := lifecycleMS("refilled", 1, 1, 0)
					oldMS.Spec.RolloutStrategy.Type = mode
					oldMS.Spec.Template.Roles[0].MaxUnavailable = ptr.To(intstr.FromInt(1))
					ms := oldMS.DeepCopy()
					ms.ResourceVersion = "1"
					ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "test:v2"
					c := lifecycleController(t, ms, oldMS)
					old := lifecyclePod(t, c, ms, oldMS, 0, "old-v1")
					ctx, err := c.withRolloutPodSnapshot(context.Background(), ms)
					require.NoError(t, err)
					// The old Pod disappears after the round's initial List. Scaling
					// can now reuse its ordinal before rolling update in the same round.
					require.NoError(t, c.kubeClientSet.CoreV1().Pods(ms.Namespace).Delete(ctx, old.Name, metav1.DeleteOptions{}))
					require.NoError(t, c.podsInformer.GetIndexer().Delete(old))
					key, group := utils.GetNamespaceName(ms), "refilled-0"
					c.store.DeleteServingGroup(key, group)
					c.store.AddServingGroup(key, 0, utils.ModelServingRevision(ms))
					kube := c.kubeClientSet.(*kubefake.Clientset)
					kube.PrependReactor("create", "pods", func(a kubetesting.Action) (bool, runtime.Object, error) {
						pod := a.(kubetesting.CreateAction).GetObject().(*corev1.Pod)
						pod.UID = "new-v2"
						if ready {
							pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
						}
						return false, nil, nil
					})
					if alreadyExists {
						role := *ms.Spec.Template.Roles[0].DeepCopy()
						pod := utils.GenerateEntryPod(role, ms, group, "prefill-0", utils.ModelServingRevision(ms), utils.CalRoleTemplateHash(role))
						_, err = kube.CoreV1().Pods(ms.Namespace).Create(ctx, pod, metav1.CreateOptions{})
						require.NoError(t, err)
					}
					require.NoError(t, c.CreatePodsForServingGroup(ctx, ms, 0, utils.ModelServingRevision(ms), ms.Spec.Template.Roles))
					current, err := kube.CoreV1().Pods(ms.Namespace).Get(ctx, old.Name, metav1.GetOptions{})
					require.NoError(t, err)
					kube.ClearActions()
					require.NoError(t, c.refreshRolloutAvailability(ctx, ms))
					require.NoError(t, c.manageRollingUpdate(ctx, ms, utils.ModelServingRevision(ms), nil))
					require.False(t, lifecycleDeletionMatches(c, current), "a replacement already at the target must not inherit the retired Pod's version")
					observed, err := c.podsForRoleObservation(ctx, ms, group, "prefill", "prefill-0")
					require.NoError(t, err)
					require.Len(t, observed, 1)
					require.Equal(t, current.UID, observed[0].UID)
				})
			}
		}
	}
}

func TestInstanceLifecycleSnapshotReadErrorCannotRestoreDeletedCapacity(t *testing.T) {
	ms := lifecycleMS("snapshot-error", 1, 1, 0)
	ms.ResourceVersion = "1"
	c := lifecycleController(t, ms)
	pod := lifecyclePod(t, c, ms, ms, 0, "retired")
	ctx, err := c.withRolloutPodSnapshot(context.Background(), ms)
	require.NoError(t, err)
	op, err := c.preparePodDeletionPlan(ctx, ms, labels.SelectorFromSet(pod.Labels), deleteRoleScope)
	require.NoError(t, err)
	require.NoError(t, c.deletePlannedPods(ctx, ms, op))
	failed := false
	c.kubeClientSet.(*kubefake.Clientset).PrependReactor("list", "pods", func(kubetesting.Action) (bool, runtime.Object, error) {
		if !failed {
			failed = true
			return true, nil, fmt.Errorf("API observation unavailable")
		}
		return false, nil, nil
	})
	observed, err := c.podsForRoleObservation(ctx, ms, "snapshot-error-0", "prefill", "prefill-0")
	require.Error(t, err)
	require.Empty(t, observed, "the old Ready Pod cannot grant capacity when refreshing fails")
	observed, err = c.podsForRoleObservation(ctx, ms, "snapshot-error-0", "prefill", "prefill-0")
	require.NoError(t, err)
	require.Empty(t, observed, "a failed refresh must remain invalid until API observation succeeds")
}

func TestInstanceLifecycleCreationBeforeReadyControlsMissingPodRecovery(t *testing.T) {
	for _, cold := range []bool{false, true} {
		for _, policy := range []api.RecoveryPolicy{api.RoleRecreate, api.ServingGroupRecreate, api.NoneRestartPolicy} {
			for _, missingIndex := range []int{0, 1} {
				t.Run(fmt.Sprintf("cold=%t/%s/missing=%d", cold, policy, missingIndex), func(t *testing.T) {
					ms := lifecycleMS("cold-scope", 1, 1, 0)
					ms.ResourceVersion, ms.Spec.RecoveryPolicy = "1", policy
					ms.Spec.Template.Roles[0].WorkerReplicas = 1
					ms.Spec.Template.Roles[0].WorkerTemplate = ms.Spec.Template.Roles[0].EntryTemplate.DeepCopy()
					other := *ms.Spec.Template.Roles[0].DeepCopy()
					other.Name, other.WorkerReplicas = "other", 0
					ms.Spec.Template.Roles = append(ms.Spec.Template.Roles, other)
					c := lifecycleController(t, ms)
					key, group, revision := utils.GetNamespaceName(ms), "cold-scope-0", utils.ModelServingRevision(ms)
					c.store.AddServingGroup(key, 0, revision)
					kube := c.kubeClientSet.(*kubefake.Clientset)
					uid := 0
					kube.PrependReactor("create", "pods", func(a kubetesting.Action) (bool, runtime.Object, error) {
						uid++
						pod := a.(kubetesting.CreateAction).GetObject().(*corev1.Pod)
						pod.UID = types.UID(fmt.Sprint(uid))
						return false, nil, nil
					})
					require.NoError(t, c.CreatePodsForServingGroup(context.Background(), ms, 0, revision, ms.Spec.Template.Roles))
					original, err := kube.CoreV1().Pods(ms.Namespace).List(context.Background(), metav1.ListOptions{})
					require.NoError(t, err)
					for i := range original.Items {
						require.False(t, utils.IsPodRunningAndReady(&original.Items[i]))
					}
					roles, err := c.store.GetRoleList(key, group, "prefill")
					require.NoError(t, err)
					require.Len(t, roles, 1)
					require.True(t, roles[0].Initialized, "all Pods were created, independently of Ready")
					require.Equal(t, datastore.RoleCreating, roles[0].Status)
					missing := utils.GeneratePodName(group, "prefill-0", missingIndex)
					require.NoError(t, kube.CoreV1().Pods(ms.Namespace).Delete(context.Background(), missing, metav1.DeleteOptions{}))
					// Reconcile a physical absence without relying on the delete callback.
					// None of these Pods has become Ready; creation is a separate fact.
					if cold {
						c.store = datastore.New()
					}
					for i := range original.Items {
						pod := &original.Items[i]
						require.NotContains(t, pod.Annotations, roleCreatedAnnotation)
						if pod.Name == missing {
							continue
						}
						require.NoError(t, c.podsInformer.GetIndexer().Add(pod))
						c.store.AddServingGroupAndRole(key, group, revision, utils.ObjectRoleTemplateHash(pod), utils.GetRoleName(pod), utils.GetRoleID(pod))
					}
					kube.ClearActions()
					ctx, err := c.withRolloutPodSnapshot(context.Background(), ms)
					require.NoError(t, err)
					require.NoError(t, c.refreshRolloutAvailability(ctx, ms))
					require.NoError(t, c.manageRoleReplicasPerGroup(ctx, ms, group, ms.Spec.Template.Roles[0], 0, revision, nil, true))
					for i := range original.Items {
						pod := &original.Items[i]
						if pod.Name == missing {
							continue
						}
						wantDelete := !cold && (policy == api.ServingGroupRecreate || (policy == api.RoleRecreate && utils.GetRoleName(pod) == "prefill"))
						require.Equal(t, wantDelete, lifecycleDeletionMatches(c, pod), "original member %s", pod.Name)
					}
				})
			}
		}
	}
}

func TestInstanceLifecyclePartialCreationIsCompletedWithoutRecoveryChurn(t *testing.T) {
	for _, policy := range []api.RecoveryPolicy{api.RoleRecreate, api.ServingGroupRecreate} {
		t.Run(string(policy), func(t *testing.T) {
			ms := lifecycleMS("partial-create", 1, 1, 0)
			ms.ResourceVersion, ms.Spec.RecoveryPolicy = "1", policy
			ms.Spec.Template.Roles[0].WorkerReplicas = 1
			ms.Spec.Template.Roles[0].WorkerTemplate = ms.Spec.Template.Roles[0].EntryTemplate.DeepCopy()
			c := lifecycleController(t, ms)
			kube := c.kubeClientSet.(*kubefake.Clientset)
			failWorker := true
			kube.PrependReactor("create", "pods", func(a kubetesting.Action) (bool, runtime.Object, error) {
				pod := a.(kubetesting.CreateAction).GetObject().(*corev1.Pod)
				pod.UID = types.UID(pod.Name)
				if failWorker && pod.Name == "partial-create-0-prefill-0-1" {
					return true, nil, apierrors.NewServiceUnavailable("worker create held")
				}
				return false, nil, nil
			})
			revision, group := utils.ModelServingRevision(ms), "partial-create-0"
			require.Error(t, c.CreatePodsForServingGroup(context.Background(), ms, 0, revision, ms.Spec.Template.Roles))
			entry, err := kube.CoreV1().Pods(ms.Namespace).Get(context.Background(), group+"-prefill-0-0", metav1.GetOptions{})
			require.NoError(t, err)
			require.Empty(t, entry.Annotations[roleCreatedAnnotation])
			c.store = datastore.New()
			c.store.AddServingGroupAndRole(utils.GetNamespaceName(ms), group, revision, utils.ObjectRoleTemplateHash(entry), "prefill", "prefill-0")
			require.NoError(t, c.podsInformer.GetIndexer().Add(entry))
			failWorker = false
			kube.ClearActions()
			require.NoError(t, c.manageRoleReplicasPerGroup(context.Background(), ms, group, ms.Spec.Template.Roles[0], 0, revision, nil, true))
			require.False(t, lifecycleDeletionMatches(c, entry))
			pods, err := kube.CoreV1().Pods(ms.Namespace).List(context.Background(), metav1.ListOptions{})
			require.NoError(t, err)
			require.Len(t, pods.Items, 2)
			for _, pod := range pods.Items {
				require.NotContains(t, pod.Annotations, roleCreatedAnnotation)
			}
		})
	}
}

func TestInstanceLifecycle_ColdStartRetainsFailedInstanceForRollout(t *testing.T) {
	for _, changed := range []bool{false, true} {
		for _, mode := range []api.RolloutStrategyType{api.ServingGroupRollingUpdate, api.RoleRollingUpdate} {
			for _, recovery := range []string{"none", "grace-minus-one"} {
				for _, health := range []string{"failed", "restarting", "plain-not-ready"} {
					t.Run(fmt.Sprintf("changed=%t/%s/%s/%s", changed, mode, recovery, health), func(t *testing.T) {
						ctx := context.Background()
						old := lifecycleMS("cold-fault", 1, 1, 0)
						old.ResourceVersion = "1"
						old.Spec.RolloutStrategy.Type = mode
						old.Spec.Template.Roles[0].MaxUnavailable = ptr.To(intstr.FromInt(1))
						old.Spec.RecoveryPolicy = api.NoneRestartPolicy
						if recovery == "grace-minus-one" {
							old.Spec.RecoveryPolicy = api.RoleRecreate
							old.Spec.Template.RestartGracePeriodSeconds = ptr.To[int64](-1)
						}
						ms := old.DeepCopy()
						if changed {
							ms.Generation++
							ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "test:v2"
						}
						ms.Status.CurrentRevision = utils.ModelServingRevision(old)
						ms.Status.UpdateRevision = utils.ModelServingRevision(ms)
						c := lifecycleController(t, ms, old)
						role := *old.Spec.Template.Roles[0].DeepCopy()
						pod := utils.GenerateEntryPod(role, ms, "cold-fault-0", "prefill-0", utils.ModelServingRevision(old), utils.CalRoleTemplateHash(role))
						pod.UID, pod.ResourceVersion = "old-fault", "5"
						pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}}
						if health == "failed" {
							pod.Status.Phase = corev1.PodFailed
						} else if health == "restarting" {
							pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: role.EntryTemplate.Spec.Containers[0].Name, RestartCount: 1}}
						}
						_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(ctx, pod, metav1.CreateOptions{})
						require.NoError(t, err)
						require.NoError(t, c.podsInformer.GetIndexer().Add(pod))
						c.initialSync.Store(false)
						c.syncAll()
						c.kubeClientSet.(*kubefake.Clientset).ClearActions()
						var lastErr error
						for attempt := 0; attempt < 3; attempt++ {
							lastErr = c.syncModelServing(ctx, namespacedKey(ms.Namespace, ms.Name))
							if lifecycleDeletionMatches(c, pod) {
								break
							}
						}
						t.Logf("health=%s observedGroup=%s reconcileError=%v", health, c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), "cold-fault-0"), lastErr)
						require.Equal(t, changed, lifecycleDeletionMatches(c, pod), "rollout must recognize an existing unhealthy old instance after restart; None/-1 only disable proactive failure recovery")
					})
				}
			}
		}
	}
}

func TestInstanceLifecycle_EnablingRecoveryRevisitsExistingFault(t *testing.T) {
	for _, initial := range []string{"none", "infinite", "long-grace"} {
		for _, health := range []string{"failed", "restarting", "ready-restarted", "plain-notready"} {
			t.Run(initial+"/"+health, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				ms := lifecycleMS("enable-recovery", 1, 1, 0)
				ms.ResourceVersion = "1"
				ms.Spec.RecoveryPolicy = api.NoneRestartPolicy
				if initial != "none" {
					ms.Spec.RecoveryPolicy = api.RoleRecreate
					ms.Spec.Template.RestartGracePeriodSeconds = ptr.To[int64](-1)
				}
				if initial == "long-grace" {
					ms.Spec.Template.RestartGracePeriodSeconds = ptr.To[int64](10000)
				}
				c := lifecycleController(t, ms)
				c.recoveryCtx = ctx
				pod := lifecyclePod(t, c, ms, ms, 0, "existing-fault")
				pod.Status.Conditions[0].Status = corev1.ConditionFalse
				if health == "failed" {
					pod.Status.Phase = corev1.PodFailed
				}
				if health == "restarting" || health == "ready-restarted" {
					pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "server", RestartCount: 1}}
				}
				if health == "ready-restarted" {
					pod.Status.Conditions[0].Status = corev1.ConditionTrue
				}
				_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
				require.NoError(t, err)
				require.NoError(t, c.podsInformer.GetIndexer().Update(pod))
				// Replay the observation under the original disabled/long grace setting.
				c.updatePod(nil, pod)
				latest := ms.DeepCopy()
				latest.Generation++
				latest.Spec.RecoveryPolicy = api.RoleRecreate
				latest.Spec.Template.RestartGracePeriodSeconds = ptr.To[int64](100)
				_, err = c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Update(ctx, latest, metav1.UpdateOptions{})
				require.NoError(t, err)
				require.NoError(t, c.modelServingsInformer.GetIndexer().Update(latest))
				require.NoError(t, c.syncModelServing(ctx, namespacedKey(ms.Namespace, ms.Name)))
				started, scheduled := c.graceMap.Load(getPodGracePeriodKey(pod))
				fault := health == "failed" || health == "restarting"
				require.Equal(t, fault, scheduled, "MS reconcile must revisit actual faults only")
				for i := 0; i < 3; i++ {
					require.NoError(t, c.revisitPodRecovery(context.Background(), latest))
				}
				again, ok := c.graceMap.Load(getPodGracePeriodKey(pod))
				require.Equal(t, scheduled, ok)
				if scheduled {
					require.True(t, started.(time.Time).Equal(again.(time.Time)), "reconcile must not reset or duplicate an episode")
				}
				if fault {
					latest = latest.DeepCopy()
					latest.Generation++
					latest.Spec.Template.RestartGracePeriodSeconds = ptr.To[int64](0)
					_, err = c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Update(ctx, latest, metav1.UpdateOptions{})
					require.NoError(t, err)
					require.NoError(t, c.modelServingsInformer.GetIndexer().Update(latest))
					require.NoError(t, c.syncModelServing(ctx, namespacedKey(ms.Namespace, ms.Name)))
					require.Eventually(t, func() bool { return lifecycleDeletionMatches(c, pod) }, 3*time.Second, 10*time.Millisecond)
				} else {
					require.False(t, lifecycleDeletionMatches(c, pod))
				}
			})
		}
	}
}

func TestInstanceLifecycle_CanaryKeepsLastCompletedBaseline(t *testing.T) {
	ctx := context.Background()
	a := lifecycleMS("canary-baseline", 1, 1, 0)
	a.ResourceVersion = "1"
	b := a.DeepCopy()
	b.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "test:v2"
	ms := b.DeepCopy()
	ms.Generation++
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "test:v3"
	ms.Spec.RolloutStrategy.RollingUpdateConfiguration.Partition = ptr.To(intstr.FromInt(1))
	ms.Spec.RevisionHistoryLimit = ptr.To[int32](0)
	ms.Status.CurrentRevision = utils.ModelServingRevision(a)
	ms.Status.UpdateRevision = utils.ModelServingRevision(ms)
	c := lifecycleController(t, ms, a, b)
	pod := lifecyclePod(t, c, ms, b, 0, "b-ready-after-c-request")
	require.NoError(t, c.updateModelServingStatus(c.withRevisionHistory(ctx, ms), ms, utils.ModelServingRevision(ms), nil))
	actual, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(ctx, ms.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, utils.ModelServingRevision(a), actual.Status.CurrentRevision, "B never completed; protected B must not become the A-to-C recovery baseline")

	// Losing the last physical group is not full completion either. Preserve
	// the baseline and its history while a protected replacement is missing.
	require.NoError(t, c.kubeClientSet.CoreV1().Pods(ms.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}))
	require.NoError(t, c.podsInformer.GetIndexer().Delete(pod))
	c.store.DeleteServingGroup(utils.GetNamespaceName(ms), ms.Name+"-0")
	require.NoError(t, c.modelServingsInformer.GetIndexer().Update(actual))
	require.NoError(t, c.updateModelServingStatus(c.withRevisionHistory(ctx, actual), actual, utils.ModelServingRevision(ms), nil))
	actual, err = c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(ctx, ms.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, utils.ModelServingRevision(a), actual.Status.CurrentRevision)
	history, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, utils.ModelServingRevision(a))
	require.NoError(t, err)
	require.NotNil(t, history, "historyLimit=0 must retain the protected baseline")
	require.NoError(t, c.scaleUpServingGroups(ctx, actual, nil, 1, utils.ModelServingRevision(ms)))
	restored, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "test:v1", restored.Spec.Containers[0].Image)
}

func TestInstanceLifecycle_SurgeContractionRespectsTotalDeletionBudget(t *testing.T) {
	for _, mode := range []api.RolloutStrategyType{api.ServingGroupRollingUpdate, api.RoleRollingUpdate} {
		for _, targetUnavailable := range []int{2, 1} {
			for _, pending := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/targetUnavailable=%d/pending=%t", mode, targetUnavailable, pending), func(t *testing.T) {
					ctx := context.Background()
					old := lifecycleMS("surge-contraction", 2, 1, 2)
					old.ResourceVersion = "1"
					old.Spec.RolloutStrategy.Type = mode
					if mode == api.RoleRollingUpdate {
						old.Spec.Replicas = ptr.To[int32](1)
						old.Spec.Template.Roles[0].Replicas = ptr.To[int32](2)
						old.Spec.Template.Roles[0].MaxUnavailable = ptr.To(intstr.FromInt(1))
						old.Spec.Template.Roles[0].MaxSurge = ptr.To(intstr.FromInt(2))
					}
					ms := old.DeepCopy()
					ms.Generation++
					ms.Spec.RolloutStrategy.RollingUpdateConfiguration.MaxSurge = ptr.To(intstr.FromInt(0))
					if mode == api.RoleRollingUpdate {
						ms.Spec.Template.Roles[0].MaxSurge = ptr.To(intstr.FromInt(0))
					}
					ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "test:v2"
					c := lifecycleController(t, ms, old)
					var extra []*corev1.Pod
					for i := 0; i < 4; i++ {
						template := ms
						if i >= 2 {
							template = old
						}
						group, id, scope := fmt.Sprintf("surge-contraction-%d", i), "prefill-0", surgeServingGroup
						if mode == api.RoleRollingUpdate {
							group, id, scope = "surge-contraction-0", fmt.Sprintf("prefill-%d", i), surgeRole
						}
						role := *template.Spec.Template.Roles[0].DeepCopy()
						revision, hash := utils.ModelServingRevision(template), utils.CalRoleTemplateHash(role)
						pod := utils.GenerateEntryPod(role, ms, group, id, revision, hash)
						pod.UID, pod.ResourceVersion = types.UID(fmt.Sprintf("pod-%d", i)), "1"
						if pending && i == 3 {
							now := metav1.Now()
							pod.DeletionTimestamp = &now
						}
						ready := i < 2 && i >= targetUnavailable
						condition, roleState, groupState := corev1.ConditionFalse, datastore.RoleCreating, datastore.ServingGroupCreating
						if ready {
							condition, roleState, groupState = corev1.ConditionTrue, datastore.RoleRunning, datastore.ServingGroupRunning
						}
						pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: condition}}}
						if i >= 2 {
							setSurgeScope(pod, scope)
							extra = append(extra, pod)
						}
						_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(ctx, pod, metav1.CreateOptions{})
						require.NoError(t, err)
						require.NoError(t, c.podsInformer.GetIndexer().Add(pod))
						c.store.AddServingGroupAndRole(utils.GetNamespaceName(ms), group, revision, hash, "prefill", id)
						if ready {
							c.store.AddRunningPodToServingGroup(utils.GetNamespaceName(ms), group, pod.Name, revision, hash, "prefill", id)
						}
						require.NoError(t, c.store.UpdateRoleStatus(utils.GetNamespaceName(ms), group, "prefill", id, roleState))
						require.NoError(t, c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), group, groupState))
					}
					c.kubeClientSet.(*kubefake.Clientset).ClearActions()
					if mode == api.ServingGroupRollingUpdate {
						require.NoError(t, c.syncServingGroupReplicas(ctx, ms, utils.ModelServingRevision(ms)))
					} else {
						require.NoError(t, c.syncRoleReplicas(ctx, ms, utils.ModelServingRevision(ms), nil))
					}
					deleted := 0
					for _, pod := range extra {
						if lifecycleDeletionMatches(c, pod) {
							deleted++
						}
					}
					budget := 4 - 1 - targetUnavailable
					if pending {
						budget--
					}
					t.Logf("temporary deletions=%d; contract maxScaleDown=4-1-%d=%d", deleted, targetUnavailable, budget)
					require.Equal(t, min(2, budget), deleted)
				})
			}
		}
	}
}

func TestInstanceLifecycle_CoordinatedSparseStableOrdinalProgresses(t *testing.T) {
	for _, partition := range []int{0, 2} {
		for _, skew := range []string{"50%", "100%"} {
			t.Run(fmt.Sprintf("P=%d/skew=%s", partition, skew), func(t *testing.T) {
				ctx := context.Background()
				old := lifecycleMS("sparse-coordination", 1, 1, 0)
				old.ResourceVersion = "1"
				old.Spec.Template.Roles[0].Replicas = ptr.To[int32](2)
				decode := *old.Spec.Template.Roles[0].DeepCopy()
				decode.Name = "decode"
				old.Spec.Template.Roles = append(old.Spec.Template.Roles, decode)
				old.Spec.RolloutStrategy = &api.RolloutStrategy{Type: api.RoleRollingUpdate, RoleCoordination: &api.RoleCoordination{MaxSkew: ptr.To(intstr.FromString(skew))}}
				ms := old.DeepCopy()
				ms.Status.CurrentRevision = utils.ModelServingRevision(old)
				ms.Generation++
				for i := range ms.Spec.Template.Roles {
					ms.Spec.Template.Roles[i].EntryTemplate.Spec.Containers[0].Image = "test:v2"
					ms.Spec.Template.Roles[i].MaxUnavailable = ptr.To(intstr.FromInt(1))
					ms.Spec.Template.Roles[i].Partition = ptr.To(intstr.FromInt(partition))
				}
				c := lifecycleController(t, ms, old)
				key := utils.GetNamespaceName(ms)
				group := ms.Name + "-0"
				for _, r := range old.Spec.Template.Roles {
					for _, n := range []int{0, 3} {
						id := utils.GenerateRoleID(r.Name, n)
						pod := utils.GenerateEntryPod(*r.DeepCopy(), ms, group, id, utils.ModelServingRevision(old), utils.CalRoleTemplateHash(r))
						pod.UID = types.UID(id)
						pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
						_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(ctx, pod, metav1.CreateOptions{})
						require.NoError(t, err)
						require.NoError(t, c.podsInformer.GetIndexer().Add(pod))
						c.store.AddRole(key, group, r.Name, id, utils.ModelServingRevision(old), utils.CalRoleTemplateHash(r))
						require.NoError(t, c.store.UpdateRoleStatus(key, group, r.Name, id, datastore.RoleRunning))
					}
				}
				policy, err := c.resolveRoleRolloutPolicy(ctx, ms, utils.ModelServingRevision(ms))
				require.NoError(t, err)
				groups, err := c.store.GetServingGroupByModelServing(key)
				require.NoError(t, err)
				selected, _, err := c.rolesToDeleteForRoleRollingUpdate(ctx, ms, groups[0], policy.group(group))
				require.NoError(t, err)
				require.Contains(t, selected, roleToDelete{roleName: "prefill", roleID: "prefill-3"}, "healthy retained high ordinal is a stable candidate after a legal sparse scale-down")
			})
		}
	}
}

func TestInstanceLifecycle_SurgeCompletionRetainsOldDependency(t *testing.T) {
	ctx := context.Background()
	old := lifecycleMS("surge-dependency", 1, 1, 0)
	old.ResourceVersion = "1"
	old.Spec.RolloutStrategy = &api.RolloutStrategy{Type: api.RoleRollingUpdate, RoleCoordination: &api.RoleCoordination{MaxSkew: ptr.To(intstr.FromString("100%")), Dependencies: []api.RoleRolloutDependency{{Role: "decode", DependsOn: []string{"prefill"}}}}}
	decode := *old.Spec.Template.Roles[0].DeepCopy()
	decode.Name = "decode"
	old.Spec.Template.Roles = append(old.Spec.Template.Roles, decode)
	for i := range old.Spec.Template.Roles {
		old.Spec.Template.Roles[i].MaxUnavailable = ptr.To(intstr.FromInt(1))
		old.Spec.Template.Roles[i].MaxSurge = ptr.To(intstr.FromInt(0))
	}
	ms := old.DeepCopy()
	ms.Generation++
	ms.Status.CurrentRevision = utils.ModelServingRevision(old)
	for i := range ms.Spec.Template.Roles {
		ms.Spec.Template.Roles[i].EntryTemplate.Spec.Containers[0].Image = "test:v2"
	}
	c := lifecycleController(t, ms, old)
	key := utils.GetNamespaceName(ms)
	group := ms.Name + "-0"
	add := func(template *api.ModelServing, roleIndex, ordinal int, temporary bool) *corev1.Pod {
		role := *template.Spec.Template.Roles[roleIndex].DeepCopy()
		id := utils.GenerateRoleID(role.Name, ordinal)
		revision, hash := utils.ModelServingRevision(template), utils.CalRoleTemplateHash(role)
		pod := utils.GenerateEntryPod(role, ms, group, id, revision, hash)
		pod.UID = types.UID(id)
		status := datastore.RoleRunning
		if temporary {
			setSurgeScope(pod, surgeRole)
			status = datastore.RoleCreating
		}
		_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		require.NoError(t, err)
		require.NoError(t, c.podsInformer.GetIndexer().Add(pod))
		c.store.AddServingGroupAndRole(key, group, revision, hash, role.Name, id)
		require.NoError(t, c.store.UpdateRoleStatus(key, group, role.Name, id, status))
		return pod
	}
	add(ms, 0, 0, false)
	extra := add(old, 0, 1, true)
	caller := add(old, 1, 0, false)
	finish := func() {
		roles, err := c.store.GetRoleList(key, group, "prefill")
		require.NoError(t, err)
		handled, err := c.finishRoleSurge(ctx, ms, group, ms.Spec.Template.Roles[0], roles, 0, utils.ModelServingRevision(ms), true)
		require.NoError(t, err)
		require.True(t, handled)
	}
	c.kubeClientSet.(*kubefake.Clientset).ClearActions()
	finish()
	require.False(t, lifecycleDeletionMatches(c, extra), "old caller still needs the last old dependency")
	require.NoError(t, c.kubeClientSet.CoreV1().Pods(ms.Namespace).Delete(ctx, caller.Name, metav1.DeleteOptions{}))
	require.NoError(t, c.podsInformer.GetIndexer().Delete(caller))
	c.store.DeleteRole(key, group, "decode", "decode-0")
	add(ms, 1, 0, false)
	c.kubeClientSet.(*kubefake.Clientset).ClearActions()
	finish()
	require.True(t, lifecycleDeletionMatches(c, extra), "obsolete temporary dependency clears after the old caller is gone")
}

func TestWholeRoleLossUsesProcessObservation(t *testing.T) {
	for _, cold := range []bool{false, true} {
		for _, policy := range []api.RecoveryPolicy{api.ServingGroupRecreate, api.NoneRestartPolicy} {
			t.Run(fmt.Sprintf("cold=%t/policy=%s", cold, policy), func(t *testing.T) {
				ctx := context.Background()
				ms := lifecycleMS("lost-role", 1, 1, 0)
				ms.ResourceVersion = "1"
				ms.Spec.RecoveryPolicy = policy
				other := *ms.Spec.Template.Roles[0].DeepCopy()
				other.Name = "decode"
				ms.Spec.Template.Roles = append(ms.Spec.Template.Roles, other)
				c := lifecycleController(t, ms)
				lost := lifecyclePod(t, c, ms, ms, 0, "lost-entry")
				revision, hash := utils.ModelServingRevision(ms), utils.CalRoleTemplateHash(other)
				survivor := utils.GenerateEntryPod(*other.DeepCopy(), ms, "lost-role-0", "decode-0", revision, hash)
				survivor.UID, survivor.ResourceVersion = "survivor", "2"
				survivor.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
				_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(ctx, survivor, metav1.CreateOptions{})
				require.NoError(t, err)
				require.NoError(t, c.podsInformer.GetIndexer().Add(survivor))
				c.store.AddRunningPodToServingGroup(utils.GetNamespaceName(ms), "lost-role-0", survivor.Name, revision, hash, "decode", "decode-0")
				require.NoError(t, c.store.UpdateRoleStatus(utils.GetNamespaceName(ms), "lost-role-0", "decode", "decode-0", datastore.RoleRunning))
				require.NoError(t, c.markRoleCreated(ctx, ms, "lost-role-0", "prefill", "prefill-0"))
				require.NoError(t, c.markRoleCreated(ctx, ms, "lost-role-0", "decode", "decode-0"))
				// Physical loss happens without a PodDeleted callback. A cold
				// controller retains API objects but loses process observations.
				require.NoError(t, c.kubeClientSet.CoreV1().Pods(ms.Namespace).Delete(ctx, lost.Name, metav1.DeleteOptions{}))
				require.NoError(t, c.podsInformer.GetIndexer().Delete(lost))
				if cold {
					fresh := lifecycleController(t, ms)
					fresh.kubeClientSet = c.kubeClientSet
					require.NoError(t, fresh.podsInformer.GetIndexer().Add(survivor))
					fresh.initialSync.Store(false)
					fresh.syncAll()
					c = fresh
				}
				c.kubeClientSet.(*kubefake.Clientset).ClearActions()
				err = c.syncModelServing(ctx, namespacedKey(ms.Namespace, ms.Name))
				require.NoError(t, err)
				deleted := lifecycleDeletionMatches(c, survivor)
				t.Logf("cold=%t policy=%s survivingRoleDeleted=%t", cold, policy, deleted)
				require.Equal(t, !cold && policy == api.ServingGroupRecreate, deleted, "only completion observed in this process can broaden missing Role recovery")
			})
		}
	}
}
