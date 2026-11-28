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

package ranktableutil

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/plugins"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
)

type fixture struct {
	req    *plugins.HookRequest
	scope  Scope
	client *fake.Clientset
	pods   []*corev1.Pod
	role   workloadv1alpha1.Role
}

func newFixture(t *testing.T, workers int32, indices ...int) *fixture {
	t.Helper()
	role := workloadv1alpha1.Role{Name: "worker", Replicas: ptr.To[int32](1), WorkerReplicas: workers,
		EntryTemplate:  workloadv1alpha1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "a"}}}},
		WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "a"}}}},
	}
	ms := &workloadv1alpha1.ModelServing{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "test", UID: "owner"},
		Spec: workloadv1alpha1.ModelServingSpec{Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{role}}}}
	f := &fixture{client: fake.NewSimpleClientset(), role: role}
	f.req = &plugins.HookRequest{ModelServing: ms, ServingGroup: "model-0", RoleName: role.Name, RoleID: "worker-0", KubeClient: f.client}
	f.scope = NewScope(f.req, true, "ranktable.json")
	_, err := utils.CreateControllerRevision(context.Background(), f.client, revisiontest.ControllerRevisions(f.client), ms, "history", []workloadv1alpha1.Role{role})
	require.NoError(t, err)
	for _, i := range indices {
		pod := utils.GenerateEntryPod(*role.DeepCopy(), ms, "model-0", "worker-0", "history", utils.CalRoleTemplateHash(role))
		if i != 0 {
			pod = utils.GenerateWorkerPod(*role.DeepCopy(), ms, "model-0", "worker-0", i, "history", utils.CalRoleTemplateHash(role))
		}
		pod.UID = types.UID(fmt.Sprintf("pod-%d", i))
		pod.Annotations = map[string]string{GroupInstance: "group-a", RoleInstance: "role-a", "test/ranktable": "data"}
		pod.Status.Phase = corev1.PodRunning
		pod.Status.PodIP = fmt.Sprintf("192.0.2.%d", i+1)
		_, err := f.client.CoreV1().Pods(ms.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
		require.NoError(t, err)
		f.pods = append(f.pods, pod)
	}
	return f
}

func renderFixture(complete bool, pods []*corev1.Pod, timestamp string) (string, error) {
	names := make([]string, 0, len(pods))
	if complete {
		for _, pod := range pods {
			names = append(names, pod.Name)
		}
	}
	data, err := json.Marshal(struct {
		Complete  bool
		Members   []string
		Timestamp string
	}{complete, names, timestamp})
	return string(data), err
}

func (f *fixture) sync(t *testing.T, mode Mode) {
	t.Helper()
	require.NoError(t, Sync(context.Background(), f.req, f.scope, mode, "test/ranktable", renderFixture))
}

func (f *fixture) cm(t *testing.T) *corev1.ConfigMap {
	t.Helper()
	cm, err := f.client.CoreV1().ConfigMaps(f.req.ModelServing.Namespace).Get(context.Background(), f.scope.Name, metav1.GetOptions{})
	require.NoError(t, err)
	return cm
}

func (f *fixture) seed(t *testing.T) {
	t.Helper()
	f.req.Pod = f.pods[0]
	f.sync(t, Prepare)
	cm := f.cm(t)
	cm.UID, cm.ResourceVersion = "cm-old", "1"
	_, err := f.client.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	f.sync(t, Refresh)
}

func completeCM(t *testing.T, cm *corev1.ConfigMap) bool {
	t.Helper()
	var data struct{ Complete bool }
	require.NoError(t, json.Unmarshal([]byte(cm.Data["ranktable.json"]), &data))
	return data.Complete
}

func noWrites(t *testing.T, client *fake.Clientset) {
	t.Helper()
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "configmaps" {
			require.NotContains(t, []string{"create", "update", "patch", "delete"}, action.GetVerb())
		}
	}
}

func TestDeleteCannotRecreateConfigMap(t *testing.T) {
	for _, group := range []bool{false, true} {
		t.Run(fmt.Sprint("group=", group), func(t *testing.T) {
			f := newFixture(t, 0, 0)
			f.scope = NewScope(f.req, !group, "ranktable.json")
			f.seed(t)
			cm := f.cm(t)
			require.NoError(t, f.client.CoreV1().ConfigMaps(cm.Namespace).Delete(context.Background(), cm.Name, metav1.DeleteOptions{}))
			require.NoError(t, f.client.CoreV1().Pods(cm.Namespace).Delete(context.Background(), f.pods[0].Name, metav1.DeleteOptions{}))
			f.client.ClearActions()
			f.sync(t, UpdateExisting)
			f.sync(t, UpdateExisting)
			noWrites(t, f.client)
			f.req.Pod = nil // Group-level RoleDelete is also update-only.
			f.sync(t, UpdateExisting)
			noWrites(t, f.client)
		})
	}
}

func TestPartialDeletionKeepsInitializingConfigMap(t *testing.T) {
	for _, deleted := range []int{0, 1} {
		t.Run(fmt.Sprint("deleted=", deleted), func(t *testing.T) {
			f := newFixture(t, 1, 0, 1)
			f.seed(t)
			require.True(t, completeCM(t, f.cm(t)))
			f.req.Pod = f.pods[deleted]
			require.NoError(t, f.client.CoreV1().Pods("test").Delete(context.Background(), f.req.Pod.Name, metav1.DeleteOptions{}))
			f.sync(t, UpdateExisting)
			require.False(t, completeCM(t, f.cm(t)))
			require.Equal(t, types.UID("cm-old"), f.cm(t).UID)
		})
	}
	f := newFixture(t, 0, 0)
	f.seed(t)
	require.NoError(t, f.client.CoreV1().Pods("test").Delete(context.Background(), f.req.Pod.Name, metav1.DeleteOptions{}))
	f.sync(t, UpdateExisting)
	require.False(t, completeCM(t, f.cm(t))) // W0/None: temporary empty is not retirement.
}

func TestAppliedWorkerLayoutAndExactMembership(t *testing.T) {
	tests := []struct {
		name     string
		applied  int32
		latest   int32
		indices  []int
		complete bool
	}{
		{"protected W1 with latest W2", 1, 2, []int{0, 1}, true},
		{"historical W2 with latest W1", 2, 1, []int{0, 1}, false},
		{"historical W2 complete", 2, 1, []int{0, 1, 2}, true},
		{"entry missing but count matches", 1, 1, []int{1, 2}, false},
		{"middle worker missing", 2, 2, []int{0, 1, 3}, false},
		{"extra worker", 1, 1, []int{0, 1, 2}, false},
		{"entry only", 0, 2, []int{0}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, tt.applied, tt.indices...)
			f.req.ModelServing.Spec.Template.Roles[0].WorkerReplicas = tt.latest
			f.seed(t)
			require.Equal(t, tt.complete, completeCM(t, f.cm(t)))
		})
	}
}

func TestMissingHistoryDoesNotGuessLatest(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(fmt.Sprint("foreign=", foreign), func(t *testing.T) {
			f := newFixture(t, 1, 0, 1)
			f.seed(t)
			cr, err := f.client.AppsV1().ControllerRevisions("test").Get(context.Background(), "model-history", metav1.GetOptions{})
			require.NoError(t, err)
			if foreign {
				cr.OwnerReferences[0].UID = "foreign"
				_, err = f.client.AppsV1().ControllerRevisions("test").Update(context.Background(), cr, metav1.UpdateOptions{})
			} else {
				err = f.client.AppsV1().ControllerRevisions("test").Delete(context.Background(), cr.Name, metav1.DeleteOptions{})
			}
			require.NoError(t, err)
			require.Error(t, Sync(context.Background(), f.req, f.scope, Refresh, "test/ranktable", renderFixture))
			require.False(t, completeCM(t, f.cm(t)))
			require.NoError(t, f.client.CoreV1().ConfigMaps("test").Delete(context.Background(), f.scope.Name, metav1.DeleteOptions{}))
			f.client.ClearActions()
			require.Error(t, Sync(context.Background(), f.req, f.scope, Refresh, "test/ranktable", renderFixture))
			noWrites(t, f.client)
		})
	}
}

func TestEquivalentRolesAcrossGlobalRevisions(t *testing.T) {
	f := newFixture(t, 1, 0, 1)
	_, err := utils.CreateControllerRevision(context.Background(), f.client, revisiontest.ControllerRevisions(f.client), f.req.ModelServing, "other", []workloadv1alpha1.Role{f.role, {Name: "unchanged-other-role"}})
	require.NoError(t, err)
	pod := f.pods[1].DeepCopy()
	pod.Labels[workloadv1alpha1.RevisionLabelKey] = "other"
	pod.Labels[workloadv1alpha1.RoleTemplateHashLabelKey] = "legacy-hash"
	_, err = f.client.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{})
	require.NoError(t, err)
	f.seed(t)
	require.True(t, completeCM(t, f.cm(t)))
}

func TestCMWriteRaceDoesNotFollowReplacement(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint("replacement=", replacement), func(t *testing.T) {
			f := newFixture(t, 1, 0, 1)
			f.seed(t)
			f.req.Pod = f.pods[1]
			require.NoError(t, f.client.CoreV1().Pods("test").Delete(context.Background(), f.req.Pod.Name, metav1.DeleteOptions{}))
			cm := f.cm(t)
			f.client.ClearActions()
			f.client.PrependReactor("update", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if replacement {
					newCM := cm.DeepCopy()
					newCM.UID = "cm-new"
					newCM.Data["ranktable.json"] = "new-incarnation-content"
					require.NoError(t, f.client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), newCM, "test"))
					return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, cm.Name, fmt.Errorf("replacement"))
				}
				require.NoError(t, f.client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("configmaps"), "test", cm.Name))
				return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, cm.Name)
			})
			f.sync(t, UpdateExisting)
			updates := 0
			for _, action := range f.client.Actions() {
				if action.GetResource().Resource == "configmaps" {
					require.NotEqual(t, "create", action.GetVerb())
					if action.GetVerb() == "update" {
						updates++
					}
				}
			}
			require.Equal(t, 1, updates)
			if replacement {
				require.Equal(t, "new-incarnation-content", f.cm(t).Data["ranktable.json"])
			}
		})
	}
}

func TestActiveSyncSelfHealsAndRemainsIdempotent(t *testing.T) {
	f := newFixture(t, 1, 0, 1)
	f.req.Role, f.req.Pod = f.role.DeepCopy(), nil
	f.sync(t, Refresh)
	require.True(t, completeCM(t, f.cm(t)))
	f.client.ClearActions()
	f.sync(t, Refresh)
	f.sync(t, Refresh)
	noWrites(t, f.client)
	for _, pod := range f.pods {
		pod = pod.DeepCopy()
		now := metav1.Now()
		pod.DeletionTimestamp = &now
		_, err := f.client.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{})
		require.NoError(t, err)
	}
	require.NoError(t, f.client.CoreV1().ConfigMaps("test").Delete(context.Background(), f.scope.Name, metav1.DeleteOptions{}))
	f.client.ClearActions()
	f.sync(t, Refresh)
	noWrites(t, f.client)
}

func TestOldPodEventCannotChangeNewIncarnation(t *testing.T) {
	for _, group := range []bool{false, true} {
		t.Run(fmt.Sprint("group=", group), func(t *testing.T) {
			f := newFixture(t, 1, 0, 1)
			f.scope = NewScope(f.req, !group, "ranktable.json")
			f.seed(t)
			old := f.pods[1].DeepCopy()
			require.NoError(t, f.client.CoreV1().Pods("test").Delete(context.Background(), old.Name, metav1.DeleteOptions{}))
			for _, pod := range f.pods[:1] {
				pod = pod.DeepCopy()
				pod.Annotations[RoleInstance] = "role-new"
				_, err := f.client.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			f.req.Pod = old // No same-name replacement; sibling identity fences it.
			f.client.ClearActions()
			f.sync(t, UpdateExisting)
			noWrites(t, f.client)
		})
	}
}

func TestForeignAndLegacyConfigMaps(t *testing.T) {
	f := newFixture(t, 0, 0)
	f.seed(t)
	cm := f.cm(t)
	cm.OwnerReferences[0].UID = "foreign"
	_, err := f.client.CoreV1().ConfigMaps("test").Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	f.client.ClearActions()
	require.Error(t, Sync(context.Background(), f.req, f.scope, Refresh, "test/ranktable", renderFixture))
	noWrites(t, f.client)
	cm.OwnerReferences[0].UID = "owner"
	cm.Annotations = nil
	_, err = f.client.CoreV1().ConfigMaps("test").Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	f.req.Pod = nil
	f.client.ClearActions()
	f.sync(t, UpdateExisting)
	noWrites(t, f.client)
	f.sync(t, Refresh)
	require.Equal(t, "v1", f.cm(t).Annotations[identityBound])
	require.True(t, completeCM(t, f.cm(t)))
}

func TestUnannotatedMembersStillFenceLifecycleAndHistory(t *testing.T) {
	for _, mismatch := range []string{"history", "cohort", "retiring"} {
		t.Run(mismatch, func(t *testing.T) {
			f := newFixture(t, 1, 0, 1)
			f.seed(t)
			pod := f.pods[1].DeepCopy()
			delete(pod.Annotations, "test/ranktable")
			switch mismatch {
			case "history":
				pod.Labels[workloadv1alpha1.RevisionLabelKey] = "unknown"
			case "cohort":
				pod.Annotations[RoleInstance] = "other"
			case "retiring":
				f.req.Pod = f.pods[0].DeepCopy()
				f.req.Pod.Annotations[RoleInstance] = "new"
			}
			_, err := f.client.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{})
			require.NoError(t, err)
			mode := Refresh
			if mismatch == "retiring" {
				mode = Prepare
			}
			require.Error(t, Sync(context.Background(), f.req, f.scope, mode, "test/ranktable", renderFixture))
			if mismatch == "history" {
				require.False(t, completeCM(t, f.cm(t)))
			}
		})
	}
}
