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

	"github.com/stretchr/testify/require"
	kthenafake "github.com/volcano-sh/kthena/client-go/clientset/versioned/fake"
	api "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
)

const rolloutGuardAcceptedRevision = "accepted-v1"

func rolloutGuardFixture() *api.ModelServing {
	ms := createStandardModelServing("rollout-guard", 1, 1)
	ms.UID = "rollout-guard-owner"
	ms.Generation = 2
	ms.ResourceVersion = "10"
	ms.Annotations = map[string]string{rolloutVersionAnnotation: "A"}
	ms.Status.CurrentRevision = rolloutGuardAcceptedRevision
	ms.Status.UpdateRevision = rolloutGuardAcceptedRevision
	ms.Status.ObservedGeneration = 1
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "entry:v1"
	ms.Spec.Template.Roles[0].WorkerReplicas = 1
	ms.Spec.Template.Roles[0].WorkerTemplate = &api.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: "worker", Image: "worker:v1"}},
	}}
	return ms
}

func newRolloutGuardController(t *testing.T, ms, accepted *api.ModelServing, version *string) *ModelServingController {
	t.Helper()
	c := newRevisionTestController(t, ms)
	c.recorder = record.NewFakeRecorder(100)
	c.initialSync.Store(true)
	t.Cleanup(c.workqueue.ShutDown)
	if accepted != nil {
		cr, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, accepted, rolloutGuardAcceptedRevision, accepted.Spec.Template.Roles)
		require.NoError(t, err)
		cr.Annotations = map[string]string{"alpha.example/keep": "unchanged"}
		if version != nil {
			cr.Annotations[rolloutVersionAnnotation] = *version
		}
		_, err = c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Update(context.Background(), cr, metav1.UpdateOptions{})
		require.NoError(t, err)
	}
	c.kubeClientSet.(*kubefake.Clientset).ClearActions()
	c.modelServingClient.(*kthenafake.Clientset).ClearActions()
	return c
}

func TestRolloutGuardUpdateEvents(t *testing.T) {
	tests := []struct {
		name       string
		oldVersion *string
		newVersion *string
		mutate     func(*api.ModelServing)
		queued     bool
	}{
		{name: "unchanged", oldVersion: ptr.To("A"), newVersion: ptr.To("A")},
		{name: "both absent"},
		{name: "both empty", oldVersion: ptr.To(""), newVersion: ptr.To("")},
		{name: "version changed", oldVersion: ptr.To("A"), newVersion: ptr.To("B"), queued: true},
		{name: "key added", newVersion: ptr.To("A"), queued: true},
		{name: "key removed", oldVersion: ptr.To("A"), queued: true},
		{name: "absent to empty", newVersion: ptr.To(""), queued: true},
		{name: "empty to absent", oldVersion: ptr.To(""), queued: true},
		{name: "empty to nonempty", oldVersion: ptr.To(""), newVersion: ptr.To("B"), queued: true},
		{name: "other annotation", oldVersion: ptr.To("A"), newVersion: ptr.To("A"), mutate: func(ms *api.ModelServing) {
			ms.Annotations["alpha.example/note"] = "changed"
		}},
		{name: "status only", oldVersion: ptr.To("A"), newVersion: ptr.To("A"), mutate: func(ms *api.ModelServing) {
			ms.Status.ObservedGeneration++
		}},
		{name: "template changed", oldVersion: ptr.To("A"), newVersion: ptr.To("A"), queued: true, mutate: func(ms *api.ModelServing) {
			ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "entry:v2"
		}},
		{name: "group replicas changed", queued: true, mutate: func(ms *api.ModelServing) { ms.Spec.Replicas = ptr.To[int32](3) }},
		{name: "role replicas changed", queued: true, mutate: func(ms *api.ModelServing) { ms.Spec.Template.Roles[0].Replicas = ptr.To[int32](3) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := rolloutGuardFixture()
			old.Annotations = map[string]string{}
			if tt.oldVersion != nil {
				old.Annotations[rolloutVersionAnnotation] = *tt.oldVersion
			}
			current := old.DeepCopy()
			delete(current.Annotations, rolloutVersionAnnotation)
			if tt.newVersion != nil {
				current.Annotations[rolloutVersionAnnotation] = *tt.newVersion
			}
			if tt.mutate != nil {
				tt.mutate(current)
			}
			c := newRolloutGuardController(t, old, nil, nil)
			c.updateModelServing(old, current)
			if !tt.queued {
				require.Zero(t, c.workqueue.Len())
				return
			}
			require.Equal(t, 1, c.workqueue.Len())
			item, shutdown := c.workqueue.Get()
			require.False(t, shutdown)
			require.Equal(t, namespacedKey(old.Namespace, old.Name), item)
			c.workqueue.Done(item)
		})
	}
}

func TestRolloutGuardSelectTemplate(t *testing.T) {
	tests := []struct {
		name       string
		version    *string
		recorded   *string
		changed    bool
		blocked    bool
		initial    bool
		observed   bool
		inProgress bool
	}{
		{name: "unprotected template", recorded: ptr.To("A"), changed: true},
		{name: "same version blocks", version: ptr.To("A"), recorded: ptr.To("A"), changed: true, blocked: true},
		{name: "empty version blocks", version: ptr.To(""), recorded: ptr.To(""), changed: true, blocked: true},
		{name: "new version permits", version: ptr.To("B"), recorded: ptr.To("A"), changed: true},
		{name: "first enable permits", version: ptr.To("A"), changed: true},
		{name: "replicas and policy only", version: ptr.To("A"), recorded: ptr.To("A")},
		{name: "version only keeps revision", version: ptr.To("B"), recorded: ptr.To("A")},
		{name: "unprotected unchanged template", recorded: ptr.To("A")},
		{name: "empty is a different version", version: ptr.To(""), recorded: ptr.To("A"), changed: true},
		{name: "restart keeps block", version: ptr.To("A"), recorded: ptr.To("A"), changed: true, blocked: true, observed: true},
		{name: "version permits without new generation", version: ptr.To("B"), recorded: ptr.To("A"), changed: true, observed: true},
		{name: "initial creation", version: ptr.To("A"), changed: true, initial: true},
		{name: "in progress blocks against update revision", version: ptr.To("A"), recorded: ptr.To("A"), changed: true, blocked: true, inProgress: true},
		{name: "in progress new version permits", version: ptr.To("B"), recorded: ptr.To("A"), changed: true, inProgress: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			accepted := rolloutGuardFixture()
			second := *accepted.Spec.Template.Roles[0].DeepCopy()
			second.Name = "decode"
			accepted.Spec.Template.Roles = append(accepted.Spec.Template.Roles, second)
			ms := accepted.DeepCopy()
			ms.Annotations = map[string]string{}
			if tt.version != nil {
				ms.Annotations[rolloutVersionAnnotation] = *tt.version
			}
			ms.Spec.Replicas = ptr.To[int32](3)
			ms.Spec.RolloutStrategy = &api.RolloutStrategy{Type: api.ServingGroupRollingUpdate,
				RollingUpdateConfiguration: &api.RollingUpdateConfiguration{MaxSurge: ptr.To(intstr.FromInt(2)), Partition: ptr.To(intstr.FromInt(1))}}
			for i := range ms.Spec.Template.Roles {
				role := &ms.Spec.Template.Roles[i]
				role.Replicas = ptr.To(int32(i + 3))
				role.MaxSurge = ptr.To(intstr.FromInt(2))
				role.Partition = ptr.To(intstr.FromInt(1))
				if tt.changed {
					role.EntryTemplate.Spec.Containers[0].Image = "entry:v2"
					role.WorkerTemplate.Spec.Containers[0].Image = "worker:v2"
					role.WorkerReplicas = 2
				}
			}
			// Match templates by name even when API Role order differs from history.
			ms.Spec.Template.Roles[0], ms.Spec.Template.Roles[1] = ms.Spec.Template.Roles[1], ms.Spec.Template.Roles[0]
			if tt.observed {
				ms.Status.ObservedGeneration = ms.Generation
			}
			if tt.inProgress {
				ms.Status.CurrentRevision = "previous-running-revision"
			}
			if tt.initial {
				ms.Status = api.ModelServingStatus{}
				accepted = nil
			}
			before := ms.DeepCopy()
			c := newRolloutGuardController(t, ms, accepted, tt.recorded)
			var originalCR *appsv1.ControllerRevision
			if accepted != nil {
				var err error
				originalCR, err = utils.GetControllerRevision(ctx, c.kubeClientSet, accepted, rolloutGuardAcceptedRevision)
				require.NoError(t, err)
				require.NotNil(t, originalCR)
			}
			selected, revision, err := c.resolveModelServingRollout(ctx, ms)
			require.NoError(t, err)
			require.Equal(t, before.Spec, ms.Spec, "must not mutate submitted spec")
			require.Equal(t, before.Spec.Replicas, selected.Spec.Replicas)
			require.Equal(t, before.Spec.RolloutStrategy, selected.Spec.RolloutStrategy)
			for i, role := range selected.Spec.Template.Roles {
				require.Equal(t, before.Spec.Template.Roles[i].Replicas, role.Replicas)
				require.Equal(t, before.Spec.Template.Roles[i].MaxSurge, role.MaxSurge)
				require.Equal(t, before.Spec.Template.Roles[i].Partition, role.Partition)
				if tt.blocked {
					require.Equal(t, "entry:v1", role.EntryTemplate.Spec.Containers[0].Image)
					require.Equal(t, "worker:v1", role.WorkerTemplate.Spec.Containers[0].Image)
					require.EqualValues(t, 1, role.WorkerReplicas)
				} else {
					require.Equal(t, before.Spec.Template.Roles[i], role)
				}
			}
			if tt.version != nil && !tt.initial && (!tt.changed || tt.blocked) {
				require.Equal(t, rolloutGuardAcceptedRevision, revision)
			} else {
				require.Empty(t, revision, "leave new or unprotected target selection to the original resolver")
			}
			if originalCR != nil {
				historical, getErr := utils.GetControllerRevision(ctx, c.kubeClientSet, selected, rolloutGuardAcceptedRevision)
				require.NoError(t, getErr)
				require.Equal(t, originalCR, historical, "selection must not write snapshot data or annotations")
			}
			stored, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(ctx, ms.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, before.Status, stored.Status, "selection must not persist status")
			for _, action := range c.kubeClientSet.(*kubefake.Clientset).Actions() {
				require.Equal(t, "get", action.GetVerb(), "protection selection must not create snapshots or resolve a new target")
			}
			for _, action := range c.modelServingClient.(*kthenafake.Clientset).Actions() {
				require.Equal(t, "get", action.GetVerb(), "selection must not write API spec or status")
			}
			if tt.blocked {
				select {
				case event := <-c.recorder.(*record.FakeRecorder).Events:
					require.Contains(t, event, "RolloutBlocked")
				default:
					t.Fatal("missing RolloutBlocked event")
				}
			}
		})
	}
}

func TestRolloutGuardPersistSelectedTarget(t *testing.T) {
	tests := []struct {
		name         string
		version      *string
		recorded     *string
		newTarget    bool
		missing      bool
		wantError    bool
		getError     bool
		recordWrites int
		statusWrites int
	}{
		{name: "unchanged is idempotent", version: ptr.To("A"), recorded: ptr.To("A")},
		{name: "version only", version: ptr.To("B"), recorded: ptr.To("A"), recordWrites: 1},
		{name: "first version record", version: ptr.To("A"), recordWrites: 1},
		{name: "new target", version: ptr.To("B"), recorded: ptr.To("A"), newTarget: true, recordWrites: 1, statusWrites: 1},
		{name: "disabled clears record", recorded: ptr.To("A"), recordWrites: 1},
		{name: "empty value is a record", version: ptr.To(""), recorded: ptr.To("A"), recordWrites: 1},
		{name: "protected missing target", version: ptr.To("A"), recorded: ptr.To("A"), missing: true, wantError: true},
		{name: "unprotected missing target", recorded: ptr.To("A"), missing: true},
		{name: "target read failure", version: ptr.To("A"), recorded: ptr.To("A"), getError: true, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			old := rolloutGuardFixture()
			ms := old.DeepCopy()
			ms.Annotations = map[string]string{}
			if tt.version != nil {
				ms.Annotations[rolloutVersionAnnotation] = *tt.version
			}
			c := newRolloutGuardController(t, ms, old, tt.recorded)
			revision := rolloutGuardAcceptedRevision
			if tt.newTarget {
				ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "entry:v2"
				revision = utils.ModelServingRevision(ms)
				// The original resolver must have persisted the target snapshot first.
				_, err := utils.CreateControllerRevision(ctx, c.kubeClientSet, ms, revision, ms.Spec.Template.Roles)
				require.NoError(t, err)
			}
			if tt.missing {
				revision = "missing-target"
			}
			before, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, revision)
			require.NoError(t, err)
			client := c.kubeClientSet.(*kubefake.Clientset)
			client.ClearActions()
			injected := errors.New("target read failed")
			if tt.getError {
				client.PrependReactor("get", "controllerrevisions", func(kubetesting.Action) (bool, runtime.Object, error) {
					return true, nil, injected
				})
			}
			err = c.saveRolloutProtectionState(ctx, ms, revision)
			if tt.wantError {
				if tt.getError {
					require.ErrorIs(t, err, injected)
				} else {
					require.ErrorContains(t, err, "target ControllerRevision")
				}
			} else {
				require.NoError(t, err)
				// Retry using a fresh round's reads, without selecting a target again.
				require.NoError(t, c.saveRolloutProtectionState(ctx, ms, revision))
			}
			recordWrites := 0
			for _, action := range client.Actions() {
				require.Contains(t, []string{"get", "update"}, action.GetVerb(), "persistence must not select or create revisions")
				if action.GetVerb() == "update" {
					recordWrites++
				}
			}
			require.Equal(t, tt.recordWrites, recordWrites)
			statusWrites := 0
			for _, action := range c.modelServingClient.(*kthenafake.Clientset).Actions() {
				if action.GetVerb() == "update" {
					require.Equal(t, "status", action.GetSubresource(), "never write spec")
					statusWrites++
				}
			}
			require.Equal(t, tt.statusWrites, statusWrites)
			stored, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(ctx, ms.Name, metav1.GetOptions{})
			require.NoError(t, err)
			if tt.statusWrites != 0 {
				require.Equal(t, revision, stored.Status.UpdateRevision)
			} else {
				require.Equal(t, rolloutGuardAcceptedRevision, stored.Status.UpdateRevision)
			}
			require.Equal(t, old.Status.ObservedGeneration, stored.Status.ObservedGeneration)
			if before != nil {
				if tt.getError {
					client.ReactionChain = client.ReactionChain[1:]
				}
				after, getErr := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, revision)
				require.NoError(t, getErr)
				require.Equal(t, before.Data, after.Data)
				require.Equal(t, before.Annotations["alpha.example/keep"], after.Annotations["alpha.example/keep"])
				value, exists := after.Annotations[rolloutVersionAnnotation]
				require.Equal(t, tt.version != nil, exists)
				if tt.version != nil {
					require.Equal(t, *tt.version, value)
				}
			}
		})
	}
}

func TestRolloutGuardRejectsInvalidHistory(t *testing.T) {
	for _, scenario := range []string{"missing reference", "missing snapshot", "corrupt data", "empty roles", "wrong owner", "missing role"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			old := rolloutGuardFixture()
			ms := old.DeepCopy()
			ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "entry:v2"
			if scenario == "missing reference" {
				ms.Status.UpdateRevision = ""
			}
			c := newRolloutGuardController(t, ms, old, ptr.To("A"))
			cr, err := utils.GetControllerRevision(ctx, c.kubeClientSet, old, rolloutGuardAcceptedRevision)
			require.NoError(t, err)
			switch scenario {
			case "missing snapshot":
				err = c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Delete(ctx, cr.Name, metav1.DeleteOptions{})
			case "corrupt data", "empty roles", "missing role", "wrong owner":
				switch scenario {
				case "corrupt data":
					cr.Data.Raw = []byte(`{"data":"invalid"}`)
				case "empty roles":
					cr.Data.Raw = []byte(`{"data":[]}`)
				case "missing role":
					cr.Data.Raw = []byte(`{"data":[{"name":"other"}]}`)
				case "wrong owner":
					cr.OwnerReferences[0].UID = "different-owner"
				}
				_, err = c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, cr, metav1.UpdateOptions{})
			}
			require.NoError(t, err)
			client := c.kubeClientSet.(*kubefake.Clientset)
			client.ClearActions()
			err = c.syncModelServing(ctx, namespacedKey(ms.Namespace, ms.Name))
			require.ErrorContains(t, err, "resolve rollout protection")
			for _, action := range client.Actions() {
				require.NotEqual(t, "create", action.GetVerb(), "invalid accepted history must not create a replacement snapshot or Pod")
			}
		})
	}
}

func TestRolloutGuardPersistenceFailureStopsWorkloadActions(t *testing.T) {
	for _, stage := range []string{"list", "create", "record", "status", "record conflict", "status conflict"} {
		t.Run(stage, func(t *testing.T) {
			old := rolloutGuardFixture()
			ms := old.DeepCopy()
			ms.Annotations[rolloutVersionAnnotation] = "B"
			ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "entry:v2"
			c := newRolloutGuardController(t, ms, old, ptr.To("A"))
			injected := errors.New("injected " + stage + " failure")
			if stage == "record conflict" {
				injected = apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "controllerrevisions"}, "target", injected)
			}
			if stage == "status conflict" {
				injected = apierrors.NewConflict(schema.GroupResource{Group: "workload.serving.volcano.sh", Resource: "modelservings"}, ms.Name, injected)
			}
			failing := true
			fail := func(kubetesting.Action) (bool, runtime.Object, error) {
				if failing {
					return true, nil, injected
				}
				return false, nil, nil
			}
			if stage == "status" || stage == "status conflict" {
				c.modelServingClient.(*kthenafake.Clientset).PrependReactor("update", "modelservings", fail)
			} else {
				verb := stage
				if stage == "record" || stage == "record conflict" {
					verb = "update"
				}
				c.kubeClientSet.(*kubefake.Clientset).PrependReactor(verb, "controllerrevisions", fail)
			}
			err := c.syncModelServing(context.Background(), namespacedKey(ms.Namespace, ms.Name))
			require.ErrorIs(t, err, injected)
			for _, action := range c.kubeClientSet.(*kubefake.Clientset).Actions() {
				if action.GetResource().Resource == "pods" {
					require.Contains(t, []string{"get", "list"}, action.GetVerb(), "no workload mutation before target persistence succeeds")
				}
			}
			stored, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(context.Background(), ms.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, rolloutGuardAcceptedRevision, stored.Status.UpdateRevision)
			failing = false
			require.NoError(t, c.syncModelServing(context.Background(), namespacedKey(ms.Namespace, ms.Name)), "retry must recover from partial persistence")
			stored, err = c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(context.Background(), ms.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, utils.ModelServingRevision(ms), stored.Status.UpdateRevision)
			cr, err := utils.GetControllerRevision(context.Background(), c.kubeClientSet, ms, stored.Status.UpdateRevision)
			require.NoError(t, err)
			require.Equal(t, "B", cr.Annotations[rolloutVersionAnnotation])
		})
	}
}

func TestRolloutGuardRetainsStatusResponseResourceVersion(t *testing.T) {
	ms := rolloutGuardFixture()
	old := ms.DeepCopy()
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "entry:v2"
	ms.Annotations[rolloutVersionAnnotation] = "B"
	c := newRolloutGuardController(t, ms, old, ptr.To("A"))
	revision := utils.ModelServingRevision(ms)
	_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, revision, ms.Spec.Template.Roles)
	require.NoError(t, err)
	ctx := c.withRevisionHistory(context.Background(), ms)
	c.revisionHistory(ctx, ms).target = revision
	statusWritten := false
	c.modelServingClient.(*kthenafake.Clientset).PrependReactor("update", "modelservings", func(action kubetesting.Action) (bool, runtime.Object, error) {
		require.Equal(t, "status", action.GetSubresource())
		response := action.(kubetesting.UpdateAction).GetObject().(*api.ModelServing).DeepCopy()
		response.ResourceVersion = "11"
		statusWritten = true
		return true, response, nil
	})
	require.NoError(t, c.saveRolloutProtectionState(ctx, ms, revision))
	require.True(t, statusWritten)
	require.Equal(t, "11", ms.ResourceVersion)
	require.Equal(t, revision, ms.Status.UpdateRevision)
	require.Equal(t, old.Status.ObservedGeneration, ms.Status.ObservedGeneration)
	history := c.revisionHistory(ctx, ms)
	require.Same(t, ms, history.ms)
	require.Equal(t, revision, history.target, "history remains available after the status resourceVersion advances")
}

func seedRolloutGuardPods(t *testing.T, c *ModelServingController, owner, template *api.ModelServing) []*corev1.Pod {
	t.Helper()
	var pods []*corev1.Pod
	role := template.Spec.Template.Roles[0]
	key := utils.GetNamespaceName(owner)
	for group := 0; group < int(*template.Spec.Replicas); group++ {
		groupName := utils.GenerateServingGroupName(owner.Name, group)
		for ordinal := 0; ordinal < int(*role.Replicas); ordinal++ {
			roleID := utils.GenerateRoleID(role.Name, ordinal)
			hash := utils.CalRoleTemplateHash(role)
			pod := utils.GenerateEntryPod(*role.DeepCopy(), owner, groupName, roleID, rolloutGuardAcceptedRevision, hash)
			pod.UID = types.UID(fmt.Sprintf("%s-uid", pod.Name))
			pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
			_, err := c.kubeClientSet.CoreV1().Pods(owner.Namespace).Create(context.Background(), pod.DeepCopy(), metav1.CreateOptions{})
			require.NoError(t, err)
			require.NoError(t, c.podsInformer.GetIndexer().Add(pod))
			c.store.AddRunningPodToServingGroup(key, groupName, pod.Name, rolloutGuardAcceptedRevision, hash, role.Name, roleID)
			require.NoError(t, c.store.UpdateRoleStatus(key, groupName, role.Name, roleID, datastore.RoleRunning))
			pods = append(pods, pod)
		}
		require.NoError(t, c.store.UpdateServingGroupStatus(key, groupName, datastore.ServingGroupRunning))
	}
	return pods
}

func TestRolloutGuardSyncKeepsAcceptedTemplate(t *testing.T) {
	for _, mode := range []api.RolloutStrategyType{api.ServingGroupRollingUpdate, api.RoleRollingUpdate} {
		for _, scenario := range []string{"expand", "group shrink", "role shrink", "repair"} {
			t.Run(string(mode)+"/"+scenario, func(t *testing.T) {
				ctx := context.Background()
				old := rolloutGuardFixture()
				old.Spec.Template.Roles[0].WorkerReplicas = 0
				old.Spec.Template.Roles[0].WorkerTemplate = nil
				old.Spec.RecoveryPolicy = api.NoneRestartPolicy
				old.Spec.RolloutStrategy = &api.RolloutStrategy{Type: mode}
				if scenario == "group shrink" {
					old.Spec.Replicas = ptr.To[int32](2)
				}
				if scenario == "role shrink" {
					old.Spec.Template.Roles[0].Replicas = ptr.To[int32](2)
				}
				ms := old.DeepCopy()
				ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "entry:v2"
				ms.Spec.Replicas = ptr.To[int32](1)
				ms.Spec.Template.Roles[0].Replicas = ptr.To[int32](1)
				expectedPods := 1
				if scenario == "expand" {
					ms.Spec.Replicas = ptr.To[int32](2)
					ms.Spec.Template.Roles[0].Replicas = ptr.To[int32](2)
					expectedPods = 4
				}
				c := newRolloutGuardController(t, ms, old, ptr.To("A"))
				originalPods := seedRolloutGuardPods(t, c, ms, old)
				if scenario == "repair" {
					pod := originalPods[0]
					require.NoError(t, c.kubeClientSet.CoreV1().Pods(ms.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}))
					require.NoError(t, c.podsInformer.GetIndexer().Delete(pod))
					c.deletePod(pod)
				}
				client := c.kubeClientSet.(*kubefake.Clientset)
				client.ClearActions()
				require.NoError(t, c.syncModelServing(ctx, namespacedKey(ms.Namespace, ms.Name)))
				pods, err := client.CoreV1().Pods(ms.Namespace).List(ctx, metav1.ListOptions{})
				require.NoError(t, err)
				require.Len(t, pods.Items, expectedPods)
				for _, pod := range pods.Items {
					require.Equal(t, "entry:v1", pod.Spec.Containers[0].Image)
					require.Equal(t, rolloutGuardAcceptedRevision, utils.ObjectRevision(&pod))
				}
				if scenario != "repair" {
					retained, getErr := client.CoreV1().Pods(ms.Namespace).Get(ctx, originalPods[0].Name, metav1.GetOptions{})
					require.NoError(t, getErr)
					require.Equal(t, originalPods[0].UID, retained.UID, "ordinary scaling must not roll retained Pods")
				}
				for _, action := range client.Actions() {
					if action.GetResource().Resource == "controllerrevisions" {
						require.NotEqual(t, "create", action.GetVerb(), "blocked template must not create a new revision")
					}
				}
			})
		}
	}
}

func TestRolloutGuardStatusUsesSelectedTemplate(t *testing.T) {
	for _, mode := range []api.RolloutStrategyType{api.ServingGroupRollingUpdate, api.RoleRollingUpdate} {
		t.Run(string(mode), func(t *testing.T) {
			old := rolloutGuardFixture()
			old.Spec.Replicas = ptr.To[int32](2)
			old.Spec.Template.Roles[0].WorkerReplicas = 0
			old.Spec.Template.Roles[0].WorkerTemplate = nil
			old.Spec.RolloutStrategy = &api.RolloutStrategy{Type: mode}
			ms := old.DeepCopy()
			ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "entry:v2"
			c := newRolloutGuardController(t, ms, old, ptr.To("A"))
			seedRolloutGuardPods(t, c, ms, old)
			selected, revision, err := c.resolveModelServingRollout(context.Background(), ms)
			require.NoError(t, err)
			ctx := c.withRevisionHistory(context.Background(), selected)
			c.revisionHistory(ctx, selected).target = revision
			require.NoError(t, c.updateModelServingStatus(ctx, selected, revision, nil))
			stored, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(ctx, ms.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, rolloutGuardAcceptedRevision, stored.Status.UpdateRevision)
			require.EqualValues(t, 2, stored.Status.UpdatedReplicas, "v1 Pods are updated relative to accepted v1, although API spec still declares v2")
			require.EqualValues(t, 2, stored.Status.AvailableReplicas)
			cached, err := c.modelServingLister.ModelServings(ms.Namespace).Get(ms.Name)
			require.NoError(t, err)
			require.Equal(t, "entry:v2", cached.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image)
		})
	}
}
