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

package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	api "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
)

func TestEvictionMustUseObservedHistoricalLayout(t *testing.T) {
	ctx := context.Background()
	old := modelServingWithValidPodTemplate()
	old.Name, old.Namespace, old.UID = "test-ms", "default", "review-ms"
	old.Spec.Replicas = ptr.To[int32](2)
	old.Spec.Template.Roles[0].WorkerReplicas = 1
	old.Spec.Template.Roles[0].WorkerTemplate = old.Spec.Template.Roles[0].EntryTemplate.DeepCopy()
	ms := old.DeepCopy()
	ms.Spec.Template.Roles[0].WorkerReplicas = 0
	ms.Spec.RolloutStrategy = &api.RolloutStrategy{Type: api.ServingGroupRollingUpdate, EvictionStrategy: &api.EvictionStrategySpec{ProtectionLevel: api.ProtectionLevelServingGroup, MinAvailable: ptr.To(intstr.FromInt(1))}}
	// Group 0 still uses entry+worker, but its worker is missing.
	// Group 1 is the only complete Ready group and must not be evicted.
	oldEntry := utils.GenerateEntryPod(*old.Spec.Template.Roles[0].DeepCopy(), ms, "test-ms-0", "inference-0", utils.ModelServingRevision(old), utils.CalRoleTemplateHash(old.Spec.Template.Roles[0]))
	target := utils.GenerateEntryPod(*ms.Spec.Template.Roles[0].DeepCopy(), ms, "test-ms-1", "inference-0", utils.ModelServingRevision(ms), utils.CalRoleTemplateHash(ms.Spec.Template.Roles[0]))
	for _, p := range []*corev1.Pod{oldEntry, target} {
		p.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
	}
	h, kube := newTestEvictionHandlerWithLivePods(ms, []*corev1.Pod{oldEntry, target}, []*corev1.Pod{oldEntry, target})
	_, err := utils.CreateControllerRevision(ctx, kube, revisiontest.ControllerRevisions(kube), old, utils.ModelServingRevision(old), old.Spec.Template.Roles)
	require.NoError(t, err)
	allowed, reason := h.checkEvictionWithTracker(ctx, ms, target)
	require.False(t, allowed, "old missing worker cannot provide eviction credit: %s", reason)
}

func evictionLayoutPods(ms *api.ModelServing, group, id string) []*corev1.Pod {
	role := ms.Spec.Template.Roles[0]
	revision, hash := utils.ModelServingRevision(ms), utils.CalRoleTemplateHash(role)
	pods := []*corev1.Pod{utils.GenerateEntryPod(*role.DeepCopy(), ms, group, id, revision, hash)}
	for i := 1; i <= int(role.WorkerReplicas); i++ {
		pods = append(pods, utils.GenerateWorkerPod(*role.DeepCopy(), ms, group, id, i, revision, hash))
	}
	for _, pod := range pods {
		pod.UID = types.UID(pod.Name + "-uid")
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
	}
	return pods
}

func TestEvictionHistoricalCompleteness(t *testing.T) {
	for _, level := range []api.ProtectionLevelType{api.ProtectionLevelServingGroup, api.ProtectionLevelRole} {
		for _, tc := range []struct {
			name                   string
			oldWorkers, newWorkers int32
			defect                 string
			allowed                bool
		}{
			{"worker-shrink-complete", 1, 0, "", true},
			{"worker-shrink-missing", 1, 0, "missing-worker", false},
			{"worker-grow-complete", 0, 1, "", true},
			{"worker-grow-new-incomplete", 0, 1, "target-missing-worker", false},
			{"missing-entry", 1, 0, "missing-entry", false},
			{"wrong-worker-ordinal", 1, 0, "wrong-ordinal", false},
			{"mixed-revision", 1, 0, "mixed-revision", false},
			{"mixed-role-hash", 1, 0, "mixed-hash", false},
			{"unknown-history", 1, 0, "unknown-history", false},
			{"history-read-error", 1, 0, "read-error", false},
		} {
			t.Run(string(level)+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				old := modelServingWithValidPodTemplate()
				old.Name, old.Namespace, old.UID = "test-ms", "default", "layout-ms"
				old.Spec.Replicas = ptr.To[int32](2)
				old.Spec.Template.Roles[0].Name = "inference"
				old.Spec.Template.Roles[0].Replicas = ptr.To[int32](1)
				old.Spec.Template.Roles[0].WorkerReplicas = tc.oldWorkers
				old.Spec.Template.Roles[0].WorkerTemplate = old.Spec.Template.Roles[0].EntryTemplate.DeepCopy()
				old.Spec.RolloutStrategy = &api.RolloutStrategy{Type: api.ServingGroupRollingUpdate, EvictionStrategy: &api.EvictionStrategySpec{ProtectionLevel: level, MinAvailable: ptr.To(intstr.FromInt(1)), RoleMinAvailable: map[string]intstr.IntOrString{"inference": intstr.FromInt(1)}}}
				targetGroup, targetID := "test-ms-1", "inference-0"
				if level == api.ProtectionLevelRole {
					old.Spec.RolloutStrategy.Type = api.RoleRollingUpdate
					old.Spec.Replicas = ptr.To[int32](1)
					old.Spec.Template.Roles[0].Replicas = ptr.To[int32](2)
					targetGroup, targetID = "test-ms-0", "inference-1"
				}
				ms := old.DeepCopy()
				ms.Spec.Template.Roles[0].WorkerReplicas = tc.newWorkers
				previous := evictionLayoutPods(old, "test-ms-0", "inference-0")
				current := evictionLayoutPods(ms, targetGroup, targetID)
				target := current[0]
				switch tc.defect {
				case "missing-worker":
					previous = previous[:1]
				case "target-missing-worker":
					current = current[:1]
				case "missing-entry":
					previous = previous[1:]
				case "wrong-ordinal":
					previous[1].Name = utils.GeneratePodName("test-ms-0", "inference-0", 2)
				case "mixed-revision":
					previous[1].Labels[api.RevisionLabelKey] = utils.ModelServingRevision(ms)
				case "mixed-hash":
					previous[1].Labels[api.RoleTemplateHashLabelKey] = "other-role-hash"
				case "unknown-history":
					for _, pod := range previous {
						pod.Labels[api.RevisionLabelKey] = "unknown"
					}
				}
				pods := append(previous, current...)
				h, kube := newTestEvictionHandlerWithLivePods(ms, pods, pods)
				_, err := utils.CreateControllerRevision(ctx, kube, revisiontest.ControllerRevisions(kube), old, utils.ModelServingRevision(old), old.Spec.Template.Roles)
				require.NoError(t, err)
				if tc.defect == "read-error" {
					kube.PrependReactor("get", "controllerrevisions", func(clienttesting.Action) (bool, runtime.Object, error) {
						return true, nil, fmt.Errorf("history unavailable")
					})
				}
				allowed, reason := h.checkEvictionWithTracker(ctx, ms, target)
				require.Equal(t, tc.allowed, allowed, reason)
			})
		}
	}
}

func TestEvictionUsesLatestMembersDespiteStalePersistedCounts(t *testing.T) {
	for _, applied := range []int32{1, 2} {
		t.Run(fmt.Sprintf("applied-%d", applied), func(t *testing.T) {
			ctx := context.Background()
			ms := modelServingWithValidPodTemplate()
			ms.Name, ms.Namespace, ms.UID = "test-ms", "default", "applied-ms"
			ms.Spec.Replicas = ptr.To[int32](2)
			ms.Spec.Template.Roles[0].Name = "inference"
			ms.Spec.Template.Roles[0].Replicas = ptr.To[int32](2)
			ms.Spec.RolloutStrategy = &api.RolloutStrategy{Type: api.ServingGroupRollingUpdate, EvictionStrategy: &api.EvictionStrategySpec{MinAvailable: ptr.To(intstr.FromInt(1))}}
			old := evictionLayoutPods(ms, "test-ms-0", "inference-0")
			target := evictionLayoutPods(ms, "test-ms-1", "inference-0")
			pods := append(old, target...)
			raw, err := json.Marshal(map[string]map[string]int32{"test-ms-0": {"inference": applied}, "test-ms-1": {"inference": 1}})
			require.NoError(t, err)
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "retired-member-state", Namespace: ms.Namespace, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ms, api.SchemeGroupVersion.WithKind("ModelServing"))}}, Data: map[string]string{"targets.json": string(raw)}}
			h, _ := newTestEvictionHandlerWithLivePods(ms, pods, pods, cm)
			allowed, reason := h.checkEvictionWithTracker(ctx, ms, target[0])
			require.False(t, allowed, reason)
			// A replacement UID is not enough to clear a disruption whose member target
			// still requires another complete instance.
			unit := servingGroupUnit(ms, "test-ms-0")
			entries := disruptionEntries{unit.key(): {expiresAt: time.Now().Add(time.Minute), triggerPodUID: "gone"}}
			h.cleanupCompleteDisruptionEntries(ms, entries, old)
			require.Len(t, entries, 1)
			pods = append(pods, evictionLayoutPods(ms, "test-ms-0", "inference-1")...)
			h.cleanupCompleteDisruptionEntries(ms, entries, pods)
			require.Empty(t, entries)
		})
	}
}

func TestEvictionTrackerRetainsIncompleteHistoricalRole(t *testing.T) {
	ctx := context.Background()
	old := modelServingWithValidPodTemplate()
	old.Name, old.Namespace, old.UID = "test-ms", "default", "tracker-ms"
	old.Spec.Template.Roles[0].WorkerReplicas = 1
	old.Spec.Template.Roles[0].WorkerTemplate = old.Spec.Template.Roles[0].EntryTemplate.DeepCopy()
	ms := old.DeepCopy()
	ms.Spec.Template.Roles[0].WorkerReplicas = 0
	pods := evictionLayoutPods(old, "test-ms-0", "inference-0")
	h, kube := newTestEvictionHandlerWithLivePods(ms, pods, pods)
	_, err := utils.CreateControllerRevision(ctx, kube, revisiontest.ControllerRevisions(kube), old, utils.ModelServingRevision(old), old.Spec.Template.Roles)
	require.NoError(t, err)
	for _, unit := range []disruptionUnit{servingGroupUnit(ms, "test-ms-0"), roleUnit(ms, "test-ms-0", "inference", "inference-0")} {
		entries := disruptionEntries{unit.key(): {expiresAt: time.Now().Add(time.Minute), triggerPodUID: "gone"}}
		h.cleanupCompleteDisruptionEntries(ms, entries, pods[:1])
		require.Len(t, entries, 1)
		h.cleanupCompleteDisruptionEntries(ms, entries, pods)
		require.Empty(t, entries)
	}
}
