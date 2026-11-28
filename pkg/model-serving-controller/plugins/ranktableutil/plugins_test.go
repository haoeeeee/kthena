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

package ranktableutil_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/plugins"
	podranktable "github.com/volcano-sh/kthena/pkg/model-serving-controller/plugins/pod-ranktable"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/plugins/ranktable"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/plugins/ranktableutil"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
)

func TestBothPluginsMaintainAppliedMembers(t *testing.T) {
	for _, podIP := range []bool{false, true} {
		for _, group := range []bool{false, true} {
			t.Run(fmt.Sprintf("podIP=%v/group=%v", podIP, group), func(t *testing.T) {
				t.Setenv("POD_NAMESPACE", "templates")
				ctx := context.Background()
				role := workloadv1alpha1.Role{Name: "worker", Replicas: ptr.To[int32](1), WorkerReplicas: 1,
					EntryTemplate:  workloadv1alpha1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "a"}}}},
					WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "a"}}}},
				}
				ms := &workloadv1alpha1.ModelServing{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "test", UID: "owner"},
					Spec: workloadv1alpha1.ModelServingSpec{Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{role}}}}
				client := fake.NewSimpleClientset()
				_, err := utils.CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "history", []workloadv1alpha1.Role{role})
				require.NoError(t, err)
				// The latest spec is deliberately incompatible with applied W1.
				ms.Spec.Template.Roles[0].WorkerReplicas = 2
				level := "role"
				if group {
					level = "group"
				}
				index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
				require.NoError(t, index.Add(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "templates"}, Data: map[string]string{
					"ranktable-level": level, "pod-parser-template": "parser", "filename": "ranktable.json", "mount-path": "/ranktable",
					"ranktable-template": `{"status":"{{ .Status }}","count":{{ .ServerCount }},"timestamp":"{{ .Timestamp }}","servers":{{ .Servers | toJson }}}`,
				}}))
				require.NoError(t, index.Add(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "parser", Namespace: "templates"}, Data: map[string]string{
					"annotation-name": "test/ranktable", "parser-template": "{{ $d := . | fromJson }}\nserverId: {{ $d.server | quote }}\ndevices: []",
				}}))
				completed, initializing := ranktable.RanktableStatusCompleted, ranktable.RanktableStatusInitializing
				factory := ranktable.NewRanktablePlugin
				if podIP {
					factory = podranktable.NewPodRanktablePlugin
					completed, initializing = podranktable.RanktableStatusCompleted, podranktable.RanktableStatusInitializing
				}
				plugin, err := factory(workloadv1alpha1.PluginSpec{Config: &apiextv1.JSON{Raw: []byte(`{"template":"template"}`)}})
				require.NoError(t, err)
				req := &plugins.HookRequest{ModelServing: ms, ServingGroup: "model-0", RoleName: "worker", RoleID: "worker-0", Role: role.DeepCopy(), KubeClient: client, ConfigMapLister: listerv1.NewConfigMapLister(index)}
				pods := []*corev1.Pod{
					utils.GenerateEntryPod(*role.DeepCopy(), ms, req.ServingGroup, req.RoleID, "history", utils.CalRoleTemplateHash(role)),
					utils.GenerateWorkerPod(*role.DeepCopy(), ms, req.ServingGroup, req.RoleID, 1, "history", utils.CalRoleTemplateHash(role)),
				}
				for i, pod := range pods {
					pod.UID = types.UID(fmt.Sprintf("pod-%d", i))
					pod.Annotations = map[string]string{ranktableutil.GroupInstance: "group-a", ranktableutil.RoleInstance: "role-a", "test/ranktable": `{"server":"shared-server"}`}
					pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, PodIP: fmt.Sprintf("192.0.2.%d", i+1), Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}}
					req.Pod = pod
					require.NoError(t, plugin.OnPodCreate(ctx, req))
					require.Equal(t, "/ranktable", pod.Spec.Containers[0].VolumeMounts[0].MountPath)
					_, err := client.CoreV1().Pods(ms.Namespace).Create(ctx, pod, metav1.CreateOptions{})
					require.NoError(t, err)
				}
				scope := ranktableutil.NewScope(req, !group, "ranktable.json")
				read := func() (*corev1.ConfigMap, string, int) {
					cm, err := client.CoreV1().ConfigMaps(ms.Namespace).Get(ctx, scope.Name, metav1.GetOptions{})
					require.NoError(t, err)
					var data struct {
						Status string
						Count  int
					}
					require.NoError(t, json.Unmarshal([]byte(cm.Data[scope.Filename]), &data))
					return cm, data.Status, data.Count
				}
				require.NoError(t, plugin.OnPodRunning(ctx, req))
				cm, status, count := read()
				require.Equal(t, completed, status) // Running/NotReady still publishes.
				expected := 1
				if podIP {
					expected = 2
				}
				require.Equal(t, expected, count) // Annotation IDs aggregate; IP IDs differ.
				req.Pod = nil
				client.ClearActions()
				require.NoError(t, plugin.OnRoleSync(ctx, req))
				require.NoError(t, plugin.OnRoleSync(ctx, req))
				for _, action := range client.Actions() {
					if action.GetResource().Resource == "configmaps" {
						require.NotEqual(t, "update", action.GetVerb())
					}
				}
				// Parser failures and missing IP block publication with no false count.
				worker := pods[1].DeepCopy()
				worker.Annotations["test/ranktable"] = "invalid-json"
				_, err = client.CoreV1().Pods(ms.Namespace).Update(ctx, worker, metav1.UpdateOptions{})
				require.NoError(t, err)
				require.NoError(t, plugin.OnRoleSync(ctx, req))
				_, status, count = read()
				require.Equal(t, initializing, status)
				require.Zero(t, count)
				worker.Annotations["test/ranktable"] = pods[1].Annotations["test/ranktable"]
				if podIP {
					worker.Status.PodIP = ""
				}
				_, err = client.CoreV1().Pods(ms.Namespace).Update(ctx, worker, metav1.UpdateOptions{})
				require.NoError(t, err)
				if podIP {
					require.NoError(t, plugin.OnRoleSync(ctx, req))
					_, status, count = read()
					require.Equal(t, initializing, status)
					require.Zero(t, count)
				}
				_, err = client.CoreV1().Pods(ms.Namespace).Update(ctx, pods[1], metav1.UpdateOptions{})
				require.NoError(t, err)
				require.NoError(t, client.CoreV1().ConfigMaps(ms.Namespace).Delete(ctx, cm.Name, metav1.DeleteOptions{}))
				require.NoError(t, plugin.OnRoleSync(ctx, req))
				_, status, count = read()
				require.Equal(t, completed, status)
				require.Equal(t, expected, count)
				require.NoError(t, plugin.OnServingGroupDelete(ctx, req))
				for _, pod := range pods {
					require.NoError(t, client.CoreV1().Pods(ms.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}))
					req.Pod = pod
					require.NoError(t, plugin.OnPodDelete(ctx, req))
				}
				list, err := client.CoreV1().ConfigMaps(ms.Namespace).List(ctx, metav1.ListOptions{})
				require.NoError(t, err)
				require.Empty(t, list.Items)
			})
		}
	}
}

type memberFixture struct {
	plugin       plugins.Plugin
	req          *plugins.HookRequest
	client       *fake.Clientset
	pods         []*corev1.Pod
	scope        ranktableutil.Scope
	completed    string
	initializing string
}

func newMemberFixture(t *testing.T, podIP, group bool, workers int32, annotatedEntry bool) *memberFixture {
	t.Helper()
	t.Setenv("POD_NAMESPACE", "templates")
	role := workloadv1alpha1.Role{Name: "worker", Replicas: ptr.To[int32](1), WorkerReplicas: workers,
		EntryTemplate:  workloadv1alpha1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "a"}}}},
		WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "a"}}}},
	}
	ms := &workloadv1alpha1.ModelServing{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "test", UID: "owner"},
		Spec: workloadv1alpha1.ModelServingSpec{Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{role}}}}
	f := &memberFixture{client: fake.NewSimpleClientset(), completed: ranktable.RanktableStatusCompleted, initializing: ranktable.RanktableStatusInitializing}
	_, err := utils.CreateControllerRevision(context.Background(), f.client, revisiontest.ControllerRevisions(f.client), ms, "history", []workloadv1alpha1.Role{role})
	require.NoError(t, err)
	level := "role"
	if group {
		level = "group"
	}
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, index.Add(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "templates"}, Data: map[string]string{
		"ranktable-level": level, "pod-parser-template": "parser", "filename": "ranktable.json", "mount-path": "/ranktable",
		"ranktable-template": `{"status":"{{ .Status }}","count":{{ .ServerCount }},"servers":{{ .Servers | toJson }}}`,
	}}))
	require.NoError(t, index.Add(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "parser", Namespace: "templates"}, Data: map[string]string{
		"annotation-name": "test/ranktable", "parser-template": "{{ $d := . | fromJson }}\nserverId: {{ $d.server | quote }}\ndevices: []",
	}}))
	factory := ranktable.NewRanktablePlugin
	if podIP {
		factory = podranktable.NewPodRanktablePlugin
		f.completed, f.initializing = podranktable.RanktableStatusCompleted, podranktable.RanktableStatusInitializing
	}
	f.plugin, err = factory(workloadv1alpha1.PluginSpec{Config: &apiextv1.JSON{Raw: []byte(`{"template":"template"}`)}})
	require.NoError(t, err)
	f.req = &plugins.HookRequest{ModelServing: ms, ServingGroup: "model-0", RoleName: role.Name, RoleID: "worker-0", Role: role.DeepCopy(), KubeClient: f.client, ConfigMapLister: listerv1.NewConfigMapLister(index)}
	f.scope = ranktableutil.NewScope(f.req, !group, "ranktable.json")
	for i := 0; i <= int(workers); i++ {
		pod := utils.GenerateEntryPod(role, ms, f.req.ServingGroup, f.req.RoleID, "history", utils.CalRoleTemplateHash(role))
		if i > 0 {
			pod = utils.GenerateWorkerPod(role, ms, f.req.ServingGroup, f.req.RoleID, i, "history", utils.CalRoleTemplateHash(role))
		}
		pod.UID = types.UID(fmt.Sprintf("pod-%d", i))
		pod.Annotations = map[string]string{ranktableutil.GroupInstance: "group-a", ranktableutil.RoleInstance: "role-a"}
		if (i == 0) == annotatedEntry {
			// Annotation membership does not require a standard resource name.
			pod.Annotations["test/ranktable"] = `{"server":"shared-server"}`
			pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, PodIP: fmt.Sprintf("192.0.2.%d", i+1)}
		} else {
			pod.Annotations[ranktable.PodRanktableAnnotation] = `{"server":"other-annotation"}`
			pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{"huawei.com/ascend-1980": resource.MustParse("1")}
			pod.Status.Phase = corev1.PodPending // No configured annotation, IP or Running state.
		}
		f.pods = append(f.pods, pod)
	}
	return f
}

func (f *memberFixture) create(t *testing.T) {
	t.Helper()
	for _, pod := range f.pods {
		f.req.Pod = pod
		f.client.ClearActions()
		require.NoError(t, f.plugin.OnPodCreate(context.Background(), f.req))
		require.Len(t, pod.Spec.Volumes, 1)
		require.Len(t, pod.Spec.Containers[0].VolumeMounts, 1)
		_, err := f.client.CoreV1().Pods("test").Create(context.Background(), pod, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	f.req.Pod = nil
}

func (f *memberFixture) check(t *testing.T, status string, count int) *corev1.ConfigMap {
	t.Helper()
	cm, err := f.client.CoreV1().ConfigMaps("test").Get(context.Background(), f.scope.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var data struct {
		Status string
		Count  int
	}
	require.NoError(t, json.Unmarshal([]byte(cm.Data[f.scope.Filename]), &data))
	require.Equal(t, status, data.Status)
	require.Equal(t, count, data.Count)
	return cm
}

func TestBothPluginsSkipUnannotatedMembers(t *testing.T) {
	for _, podIP := range []bool{false, true} {
		for _, group := range []bool{false, true} {
			for _, entry := range []bool{false, true} {
				for _, workers := range []int32{1, 2} {
					t.Run(fmt.Sprintf("podIP=%v/group=%v/annotatedEntry=%v/W%d", podIP, group, entry, workers), func(t *testing.T) {
						f := newMemberFixture(t, podIP, group, workers, entry)
						f.create(t)
						// Force history resolution rather than using the latest layout.
						f.req.ModelServing.Spec.Template.Roles[0].WorkerReplicas = 3 - workers
						f.req.Role = f.req.ModelServing.Spec.Template.Roles[0].DeepCopy()
						require.NoError(t, f.plugin.OnRoleSync(context.Background(), f.req))
						count := 1
						if podIP && !entry {
							count = int(workers)
						}
						cm := f.check(t, f.completed, count)
						f.client.ClearActions()
						// Even a callback for a Pending nonparticipant must not block.
						nonparticipant := f.pods[0]
						if entry {
							nonparticipant = f.pods[1]
						}
						f.req.Pod = nonparticipant
						require.NoError(t, f.plugin.OnPodRunning(context.Background(), f.req))
						f.req.Pod = nil
						require.NoError(t, f.plugin.OnRoleSync(context.Background(), f.req))
						for _, action := range f.client.Actions() {
							if action.GetResource().Resource == "configmaps" {
								require.NotContains(t, []string{"create", "update", "patch", "delete"}, action.GetVerb())
							}
						}
						require.Equal(t, cm.Data, f.check(t, f.completed, count).Data)
						// A physically missing Pod still violates full Role membership.
						if !group {
							require.NoError(t, f.client.CoreV1().Pods("test").Delete(context.Background(), nonparticipant.Name, metav1.DeleteOptions{}))
							require.NoError(t, f.plugin.OnRoleSync(context.Background(), f.req))
							f.check(t, f.initializing, 0)
						}
					})
				}
			}
		}
	}
}

func TestBothPluginsKeepAnnotatedDataRequirements(t *testing.T) {
	for _, podIP := range []bool{false, true} {
		for _, group := range []bool{false, true} {
			for _, invalid := range []string{"", "invalid-json"} {
				t.Run(fmt.Sprintf("podIP=%v/group=%v/value=%q", podIP, group, invalid), func(t *testing.T) {
					f := newMemberFixture(t, podIP, group, 1, true)
					f.pods[0].Annotations["test/ranktable"] = invalid
					f.create(t)
					require.NoError(t, f.plugin.OnRoleSync(context.Background(), f.req))
					f.check(t, f.initializing, 0)
					pod := f.pods[0].DeepCopy()
					pod.Annotations["test/ranktable"] = `{"server":"shared-server"}`
					pod.Status.Phase = corev1.PodPending
					_, err := f.client.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{})
					require.NoError(t, err)
					require.NoError(t, f.plugin.OnRoleSync(context.Background(), f.req))
					f.check(t, f.initializing, 0)
					pod.Status.Phase, pod.Status.PodIP = corev1.PodRunning, ""
					_, err = f.client.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{})
					require.NoError(t, err)
					require.NoError(t, f.plugin.OnRoleSync(context.Background(), f.req))
					if podIP {
						f.check(t, f.initializing, 0)
					} else {
						f.check(t, f.completed, 1)
					}
				})
			}
		}
	}
}

func TestBothPluginsPrepareMountBeforeAnnotationAndRefreshParticipants(t *testing.T) {
	for _, podIP := range []bool{false, true} {
		for _, group := range []bool{false, true} {
			t.Run(fmt.Sprintf("podIP=%v/group=%v", podIP, group), func(t *testing.T) {
				f := newMemberFixture(t, podIP, group, 0, false)
				f.pods[0].Spec.InitContainers = []corev1.Container{{Name: "init", Image: "a"}}
				f.req.Role.EntryTemplate.Spec.InitContainers = f.pods[0].Spec.InitContainers
				f.req.ModelServing.Spec.Template.Roles[0] = *f.req.Role.DeepCopy()
				f.pods[0].Labels[workloadv1alpha1.RoleTemplateHashLabelKey] = utils.CalRoleTemplateHash(*f.req.Role)
				f.create(t)
				require.Len(t, f.pods[0].Spec.InitContainers[0].VolumeMounts, 1)
				require.Equal(t, f.scope.Name, f.pods[0].Spec.Volumes[0].ConfigMap.Name)
				require.NoError(t, f.plugin.OnRoleSync(context.Background(), f.req))
				f.check(t, f.initializing, 0)
				// The device annotation arrives after Pod creation. The mount is
				// already present while communication membership begins here.
				pod := f.pods[0].DeepCopy()
				pod.Annotations["test/ranktable"] = `{"server":"shared-server"}`
				pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "192.0.2.1"}
				_, err := f.client.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{})
				require.NoError(t, err)
				f.req.Pod = pod
				require.NoError(t, f.plugin.OnPodRunning(context.Background(), f.req))
				f.req.Pod = nil
				require.NoError(t, f.plugin.OnRoleSync(context.Background(), f.req))
				cm := f.check(t, f.completed, 1)
				// Repeated creation hooks, including an unannotated request, must
				// not reset the published data or duplicate any mount.
				f.req.Pod = f.pods[0]
				require.NoError(t, f.plugin.OnPodCreate(context.Background(), f.req))
				require.NoError(t, f.plugin.OnPodCreate(context.Background(), f.req))
				require.Equal(t, cm.Data, f.check(t, f.completed, 1).Data)
				require.Len(t, f.req.Pod.Spec.Volumes, 1)
				require.Len(t, f.req.Pod.Spec.Containers[0].VolumeMounts, 1)
				require.Len(t, f.req.Pod.Spec.InitContainers[0].VolumeMounts, 1)
				f.req.Pod = nil
				// Refresh still consumes updated annotation data / Pod IP.
				pod.Annotations["test/ranktable"] = `{"server":"updated-server"}`
				pod.Status.PodIP = "192.0.2.2"
				_, err = f.client.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{})
				require.NoError(t, err)
				require.NoError(t, f.plugin.OnRoleSync(context.Background(), f.req))
				updated := f.check(t, f.completed, 1)
				server := "updated-server"
				if podIP {
					server = "192.0.2.2"
				}
				require.Contains(t, updated.Data[f.scope.Filename], server)
				// Removing the final participant empties the existing table;
				// Refresh alone still must not create a table with no participants.
				delete(pod.Annotations, "test/ranktable")
				_, err = f.client.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{})
				require.NoError(t, err)
				require.NoError(t, f.plugin.OnRoleSync(context.Background(), f.req))
				f.check(t, f.initializing, 0)
				require.NoError(t, f.client.CoreV1().ConfigMaps("test").Delete(context.Background(), cm.Name, metav1.DeleteOptions{}))
				require.NoError(t, f.plugin.OnRoleSync(context.Background(), f.req))
				_, err = f.client.CoreV1().ConfigMaps("test").Get(context.Background(), cm.Name, metav1.GetOptions{})
				require.True(t, apierrors.IsNotFound(err))
			})
		}
	}
}
