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
	kthenafake "github.com/volcano-sh/kthena/client-go/clientset/versioned/fake"
	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	corev1 "k8s.io/api/core/v1"
	apiextfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

// recordDifferentRevision gives rollout-budget fixtures actual historical
// template evidence. A made-up old hash without history is now unknown, not
// permission to delete. The changed annotation is a real Pod-template change.
func recordDifferentRevision(t *testing.T, c *ModelServingController, ms *workloadv1alpha1.ModelServing, revision string) {
	t.Helper()
	old := ms.DeepCopy()
	if len(old.Spec.Template.Roles) == 0 {
		old.Spec.Template.Roles = []workloadv1alpha1.Role{{Name: "historical-role"}}
	}
	for i := range old.Spec.Template.Roles {
		old.Spec.Template.Roles[i].EntryTemplate.Metadata = &workloadv1alpha1.Metadata{
			Annotations: map[string]string{"test.kthena.io/template-version": revision},
		}
	}
	_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, revision, old.Spec.Template.Roles)
	require.NoError(t, err)
}

func newRevisionTestController(t *testing.T, ms *workloadv1alpha1.ModelServing) *ModelServingController {
	t.Helper()
	c, err := NewModelServingController(kubefake.NewSimpleClientset(), kthenafake.NewSimpleClientset(ms.DeepCopy()), nil, apiextfake.NewSimpleClientset())
	require.NoError(t, err)
	require.NoError(t, c.modelServingsInformer.GetIndexer().Add(ms))
	return c
}

func TestRevisionComparisonPartialRoleUpdate(t *testing.T) {
	old := createStandardModelServing("partial-role", 1, 1)
	old.UID = "partial-role-uid"
	decode := *old.Spec.Template.Roles[0].DeepCopy()
	decode.Name = "decode"
	old.Spec.Template.Roles = append(old.Spec.Template.Roles, decode)
	ms := old.DeepCopy()
	ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate}
	ms.Spec.Template.Roles[1].EntryTemplate.Spec.Containers[0].Image = "decode:v2"
	c := newRevisionTestController(t, ms)
	_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, "legacy", old.Spec.Template.Roles)
	require.NoError(t, err)
	for _, role := range old.Spec.Template.Roles {
		addReadyLegacyGroupToController(t, c, ms, role, 0, "legacy")
	}
	groups, err := c.store.GetServingGroupByModelServing(utils.GetNamespaceName(ms))
	require.NoError(t, err)
	ctx := c.withRevisionHistory(context.Background(), ms)
	selected, outdated, err := c.rolesToDeleteForRoleRollingUpdate(ctx, ms, groups[0], nil)
	require.NoError(t, err)
	assert.True(t, outdated)
	assert.Equal(t, []roleToDelete{{roleName: "decode", roleID: "decode-0"}}, selected)
	assert.Equal(t, templateDifferent, c.compareServingGroupTemplate(ctx, ms, groups[0], utils.ModelServingRevision(ms)))
	require.NoError(t, c.syncModelServing(ctx, namespacedKey(ms.Namespace, ms.Name)))
	targetCR, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, utils.ModelServingRevision(ms))
	require.NoError(t, err)
	require.NotNil(t, targetCR, "independent Role rollout must persist the new template before creating Pods")
	assert.Equal(t, datastore.RoleRunning, c.store.GetRoleStatus(utils.GetNamespaceName(ms), groups[0].Name, "prefill", "prefill-0"))
}

func TestRoleRollingZeroReplicaRoleConvergesStatus(t *testing.T) {
	old := createStandardModelServing("zero-role", 1, 0)
	old.UID = "zero-role-uid"
	old.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "prefill:v1"
	decode := *old.Spec.Template.Roles[0].DeepCopy()
	decode.Name = "decode"
	decode.Replicas = ptr.To[int32](3)
	decode.EntryTemplate.Spec.Containers[0].Image = "decode:v1"
	old.Spec.Template.Roles = append(old.Spec.Template.Roles, decode)

	ms := old.DeepCopy()
	ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate}
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "prefill:v2"
	ms.Spec.Template.Roles[1].EntryTemplate.Spec.Containers[0].Image = "decode:v2"
	ms.Status.CurrentRevision = "legacy"
	ms.Status.UpdateRevision = "legacy"

	c := newRevisionTestController(t, ms)
	_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, "legacy", old.Spec.Template.Roles)
	require.NoError(t, err)
	targetRevision := utils.ModelServingRevision(ms)
	_, err = utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, targetRevision, ms.Spec.Template.Roles)
	require.NoError(t, err)

	key := utils.GetNamespaceName(ms)
	groupName := utils.GenerateServingGroupName(ms.Name, 0)
	c.store.AddServingGroup(key, 0, "legacy")
	require.NoError(t, c.store.UpdateServingGroupStatus(key, groupName, datastore.ServingGroupRunning))
	targetHash := utils.CalRoleTemplateHash(ms.Spec.Template.Roles[1])
	for ordinal := 0; ordinal < 3; ordinal++ {
		roleID := utils.GenerateRoleID("decode", ordinal)
		c.store.AddRole(key, groupName, "decode", roleID, targetRevision, targetHash)
		require.NoError(t, c.store.UpdateRoleStatus(key, groupName, "decode", roleID, datastore.RoleRunning))
	}

	groups, err := c.store.GetServingGroupByModelServing(key)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	ctx := c.withRevisionHistory(context.Background(), ms)
	assert.Equal(t, templateEquivalent, c.compareServingGroupTemplate(ctx, ms, groups[0], targetRevision))

	require.NoError(t, c.UpdateModelServingStatus(ms, targetRevision))
	updated, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(ctx, ms.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, int32(1), updated.Status.UpdatedReplicas)
	assert.Equal(t, targetRevision, updated.Status.CurrentRevision)
	assert.Equal(t, targetRevision, updated.Status.UpdateRevision)
	require.NotEmpty(t, updated.Status.Conditions)
	assert.Equal(t, string(workloadv1alpha1.ModelServingAvailable), updated.Status.Conditions[len(updated.Status.Conditions)-1].Type)
}

func TestRoleRollingZeroReplicaRoleWaitsForObservedInstance(t *testing.T) {
	old := createStandardModelServing("zero-role-observed", 1, 1)
	old.UID = "zero-role-observed-uid"
	old.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "prefill:v1"
	ms := old.DeepCopy()
	ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate}
	ms.Spec.Template.Roles[0].Replicas = ptr.To[int32](0)
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "prefill:v2"

	c := newRevisionTestController(t, ms)
	_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, "legacy", old.Spec.Template.Roles)
	require.NoError(t, err)
	key := utils.GetNamespaceName(ms)
	groupName := utils.GenerateServingGroupName(ms.Name, 0)
	c.store.AddServingGroup(key, 0, "legacy")
	c.store.AddRole(key, groupName, "prefill", "prefill-0", "legacy", utils.CalRoleTemplateHash(old.Spec.Template.Roles[0]))
	require.NoError(t, c.store.UpdateRoleStatus(key, groupName, "prefill", "prefill-0", datastore.RoleRunning))
	groups, err := c.store.GetServingGroupByModelServing(key)
	require.NoError(t, err)

	assert.Equal(t, templateDifferent, c.compareServingGroupTemplate(context.Background(), ms, groups[0], utils.ModelServingRevision(ms)))
	c.store.DeleteRole(key, groupName, "prefill", "prefill-0")
	assert.Equal(t, templateEquivalent, c.compareServingGroupTemplate(context.Background(), ms, groups[0], utils.ModelServingRevision(ms)),
		"an empty per-Role map must not keep a zero-replica Role rollout active")
}

func TestServingGroupRollingStillComparesAbsentZeroReplicaRole(t *testing.T) {
	old := createStandardModelServing("zero-role-group-rollout", 1, 0)
	old.UID = "zero-role-group-rollout-uid"
	old.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "prefill:v1"
	ms := old.DeepCopy()
	ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.ServingGroupRollingUpdate}
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "prefill:v2"

	c := newRevisionTestController(t, ms)
	_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, "legacy", old.Spec.Template.Roles)
	require.NoError(t, err)
	key := utils.GetNamespaceName(ms)
	c.store.AddServingGroup(key, 0, "legacy")
	groups, err := c.store.GetServingGroupByModelServing(key)
	require.NoError(t, err)

	assert.Equal(t, templateDifferent, c.compareServingGroupTemplate(context.Background(), ms, groups[0], utils.ModelServingRevision(ms)))
}

func TestRevisionComparisonIgnoresLifecycleAndReplicaCounts(t *testing.T) {
	for _, status := range []datastore.ServingGroupStatus{datastore.ServingGroupRunning, datastore.ServingGroupCreating, datastore.ServingGroupScaling, datastore.ServingGroupDeleting} {
		t.Run(string(status), func(t *testing.T) {
			old := createStandardModelServing("role-scale", 1, 1)
			old.UID = "role-scale-uid"
			ms := old.DeepCopy()
			ms.Spec.Template.Roles[0].Replicas = ptr.To[int32](2)
			c := newRevisionTestController(t, ms)
			_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, "legacy", old.Spec.Template.Roles)
			require.NoError(t, err)
			pod := addReadyLegacyGroupToController(t, c, ms, old.Spec.Template.Roles[0], 4, "legacy")
			pod.Status.Conditions = nil
			require.NoError(t, c.podsInformer.GetIndexer().Update(pod))
			key := utils.GetNamespaceName(ms)
			require.NoError(t, c.store.UpdateServingGroupStatus(key, "role-scale-4", status))
			groups, err := c.store.GetServingGroupByModelServing(key)
			require.NoError(t, err)
			ctx := c.withRevisionHistory(context.Background(), ms)
			assert.Equal(t, templateEquivalent, c.compareServingGroupTemplate(ctx, ms, groups[0], utils.ModelServingRevision(ms)))
			assert.False(t, c.hasUpdateableOutdatedServingGroup(ctx, ms, groups, utils.ModelServingRevision(ms), 0))
			assert.Equal(t, status, c.store.GetServingGroupStatus(key, "role-scale-4"), "identity comparison cannot resurrect deleting groups")
		})
	}
}

func TestRevisionComparisonUnknownHistoryDoesNotAuthorizeRollout(t *testing.T) {
	for _, kind := range []string{"missing", "foreign", "malformed"} {
		t.Run(kind, func(t *testing.T) {
			ms := createStandardModelServing("unknown", 1, 1)
			ms.UID = "current-uid"
			c := newRevisionTestController(t, ms)
			if kind != "missing" {
				cr, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, "legacy", ms.Spec.Template.Roles)
				require.NoError(t, err)
				if kind == "foreign" {
					cr.OwnerReferences[0].UID = types.UID("foreign-uid")
				} else {
					cr.Data = runtime.RawExtension{Raw: []byte(`{"data":"broken"}`)}
				}
				_, err = c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Update(context.Background(), cr, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			addReadyLegacyGroupToController(t, c, ms, ms.Spec.Template.Roles[0], 0, "legacy")
			groups, err := c.store.GetServingGroupByModelServing(utils.GetNamespaceName(ms))
			require.NoError(t, err)
			ctx := c.withRevisionHistory(context.Background(), ms)
			assert.Equal(t, templateUnknown, c.compareServingGroupTemplate(ctx, ms, groups[0], utils.ModelServingRevision(ms)))
			require.NoError(t, c.manageRollingUpdate(ctx, ms, utils.ModelServingRevision(ms), nil))
			assert.Equal(t, datastore.ServingGroupRunning, c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), groups[0].Name))
			require.NoError(t, c.UpdateModelServingStatus(ms, utils.ModelServingRevision(ms)))
			updated, err := c.modelServingClient.WorkloadV1alpha1().ModelServings(ms.Namespace).Get(ctx, ms.Name, metav1.GetOptions{})
			require.NoError(t, err)
			assert.Zero(t, updated.Status.UpdatedReplicas)
		})
	}
}

func TestRevisionComparisonMixedRoleRevisionsAndRestart(t *testing.T) {
	ms := createStandardModelServing("mixed", 1, 2)
	ms.UID = "mixed-uid"
	for attempt := 0; attempt < 2; attempt++ {
		c := newRevisionTestController(t, ms)
		for _, rev := range []string{"legacy-a", "legacy-b"} {
			_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, rev, ms.Spec.Template.Roles)
			require.NoError(t, err)
		}
		// Either event order must produce the same comparison; no process-local
		// adoption or Pod metadata change is needed after restart.
		first, second := "legacy-a", "legacy-b"
		if attempt == 1 {
			first, second = second, first
		}
		key := utils.GetNamespaceName(ms)
		c.store.AddServingGroup(key, 4, first)
		c.store.AddRole(key, "mixed-4", "prefill", "prefill-0", first, "old-hash-a")
		c.store.AddRole(key, "mixed-4", "prefill", "prefill-3", second, "old-hash-b")
		groups, err := c.store.GetServingGroupByModelServing(key)
		require.NoError(t, err)
		ctx := c.withRevisionHistory(context.Background(), ms)
		assert.Equal(t, templateEquivalent, c.compareServingGroupTemplate(ctx, ms, groups[0], utils.ModelServingRevision(ms)))
		assert.False(t, c.hasUpdateableOutdatedServingGroup(ctx, ms, groups, utils.ModelServingRevision(ms), 0))
		assert.Equal(t, first, groups[0].Revision)
	}
}

func TestRevisionComparisonDoesNotExemptScaleDown(t *testing.T) {
	ms := createStandardModelServing("scale-down", 2, 1)
	ms.UID = "scale-down-uid"
	ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{
		Type:                       workloadv1alpha1.ServingGroupRollingUpdate,
		RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{MaxSurge: ptr.To(intstr.FromInt(1))},
	}
	c := newRevisionTestController(t, ms)
	_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, "legacy", ms.Spec.Template.Roles)
	require.NoError(t, err)
	for _, ordinal := range []int{0, 3, 4} {
		pod := addReadyLegacyGroupToController(t, c, ms, ms.Spec.Template.Roles[0], ordinal, "legacy")
		_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(context.Background(), pod.DeepCopy(), metav1.CreateOptions{})
		require.NoError(t, err)
	}
	require.NoError(t, c.syncServingGroupReplicas(context.Background(), ms, utils.ModelServingRevision(ms)))
	assert.Equal(t, datastore.ServingGroupDeleting, c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), "scale-down-4"))
	assert.Equal(t, datastore.ServingGroupRunning, c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), "scale-down-0"))
}

func TestSyncLegacyRevisionWhileScalingRoleDoesNotRollExistingPod(t *testing.T) {
	old := createStandardModelServing("upgrade-scale", 1, 1)
	old.UID = "upgrade-scale-uid"
	ms := old.DeepCopy()
	ms.Spec.Template.Roles[0].Replicas = ptr.To[int32](2)
	c := newRevisionTestController(t, ms)
	_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, "legacy", old.Spec.Template.Roles)
	require.NoError(t, err)
	pod := addReadyLegacyGroupToController(t, c, ms, old.Spec.Template.Roles[0], 4, "legacy")
	_, err = c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(context.Background(), pod.DeepCopy(), metav1.CreateOptions{})
	require.NoError(t, err)
	client := c.kubeClientSet.(*kubefake.Clientset)
	client.ClearActions()
	require.NoError(t, c.syncModelServing(context.Background(), namespacedKey(ms.Namespace, ms.Name)))
	pods, err := client.CoreV1().Pods(ms.Namespace).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, pods.Items, 2)
	retained, err := client.CoreV1().Pods(ms.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, pod.UID, retained.UID)
	assert.Equal(t, pod.Labels, retained.Labels)
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "pods" {
			assert.NotContains(t, []string{"delete", "delete-collection", "patch", "update"}, action.GetVerb())
		}
	}
}

func TestDesiredRevisionReuseIsDeterministicAndCachesHistory(t *testing.T) {
	ms := createStandardModelServing("reuse", 1, 1)
	ms.UID = "reuse-uid"
	ms.Status.UpdateRevision = "legacy-b"
	c := newRevisionTestController(t, ms)
	for _, revision := range []string{"legacy-a", "legacy-b"} {
		_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, revision, ms.Spec.Template.Roles)
		require.NoError(t, err)
	}
	client := c.kubeClientSet.(*kubefake.Clientset)
	client.ClearActions()
	ctx := c.withRevisionHistory(context.Background(), ms)
	history := c.revisionHistory(ctx, ms)
	desired, err := history.desiredRevision(ctx)
	require.NoError(t, err)
	assert.Equal(t, "legacy-b", desired)
	for i := 0; i < 5; i++ {
		_, err := history.roles(ctx, desired)
		require.NoError(t, err)
	}
	require.Len(t, client.Actions(), 1, "the listed snapshot is reused across comparison calls")
	assert.True(t, client.Actions()[0].Matches("list", "controllerrevisions"))
	// An unknown result only belongs to this reconcile, not the next one.
	_, err = history.roles(ctx, "late")
	require.Error(t, err)
	_, err = utils.CreateControllerRevision(context.Background(), client, ms, "late", ms.Spec.Template.Roles)
	require.NoError(t, err)
	_, err = c.revisionHistory(context.Background(), ms).roles(context.Background(), "late")
	require.NoError(t, err)
}

func TestRevisionRecoveryUsesCompletedRoleTemplate(t *testing.T) {
	ms := createStandardModelServing("completed-role", 1, 4)
	ms.UID = "completed-role-uid"
	ms.Status.CurrentRevision = "stable"
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "latest:v3"
	ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{
		Type:             workloadv1alpha1.RoleRollingUpdate,
		RoleCoordination: &workloadv1alpha1.RoleCoordination{MaxSkew: ptr.To(intstr.FromString("25%"))},
	}
	c := newRevisionTestController(t, ms)
	for revision, replicas := range map[string]int32{"ancient": 1, "stable": 2} {
		old := ms.DeepCopy()
		old.Spec.Template.Roles[0].Replicas = &replicas
		old.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = revision
		_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, revision, old.Spec.Template.Roles)
		require.NoError(t, err)
	}
	key := utils.GetNamespaceName(ms)
	c.store.AddServingGroup(key, 0, "ancient")
	for _, name := range []string{"prefill-0", "prefill-1"} {
		c.store.AddRole(key, "completed-role-0", "prefill", name, "stable", "legacy-hash")
	}
	group := datastore.ServingGroup{Name: "completed-role-0", Revision: "ancient"}
	ctx := c.withRevisionHistory(context.Background(), ms)
	assert.Equal(t, "stable", c.revisionForServingGroup(ctx, ms, group))
	policy, err := c.resolveRoleRolloutPolicy(ctx, ms, utils.ModelServingRevision(ms))
	require.NoError(t, err)
	limits, exists := policy.group(group.Name).role("prefill")
	require.True(t, exists)
	assert.Equal(t, 2, limits.rolloutEnd, "the previous completed replica baseline is 2, not the group's ancient count of 1")
	observed, _ := c.store.GetServingGroupRevision(key, group.Name)
	assert.Equal(t, "ancient", observed)
}

// BinarySI and DecimalSI quantities survive API round trips with distinct hash
// representations, while Kubernetes considers their resource values equal.
func TestSemanticTemplateComparisonPreservesQuantityEquivalentInstances(t *testing.T) {
	for _, strategy := range []workloadv1alpha1.RolloutStrategyType{workloadv1alpha1.ServingGroupRollingUpdate, workloadv1alpha1.RoleRollingUpdate} {
		t.Run(string(strategy), func(t *testing.T) {
			old := createStandardModelServing("semantic-quantity", 1, 1)
			old.UID = "semantic-quantity-uid"
			old.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: strategy}
			old.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Mi")}
			ms := old.DeepCopy()
			ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("4194304")
			require.True(t, utils.EqualRoleTemplatesForRevision(old.Spec.Template.Roles, ms.Spec.Template.Roles))
			require.NotEqual(t, utils.CalRoleTemplateHash(old.Spec.Template.Roles[0]), utils.CalRoleTemplateHash(ms.Spec.Template.Roles[0]))
			c := newRevisionTestController(t, ms)
			_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, "legacy", old.Spec.Template.Roles)
			require.NoError(t, err)
			pod := addReadyLegacyGroupToController(t, c, ms, old.Spec.Template.Roles[0], 0, "legacy")
			_, err = c.kubeClientSet.CoreV1().Pods(ms.Namespace).Create(context.Background(), pod.DeepCopy(), metav1.CreateOptions{})
			require.NoError(t, err)
			groups, err := c.store.GetServingGroupByModelServing(utils.GetNamespaceName(ms))
			require.NoError(t, err)
			ctx := c.withRevisionHistory(context.Background(), ms)
			desired, err := c.revisionHistory(ctx, ms).desiredRevision(ctx)
			require.NoError(t, err)
			assert.Equal(t, "legacy", desired)
			assert.Equal(t, templateEquivalent, c.compareServingGroupTemplate(ctx, ms, groups[0], desired))
			roles, err := c.store.GetRoleList(utils.GetNamespaceName(ms), groups[0].Name, "prefill")
			require.NoError(t, err)
			outdated, unavailable := c.outdatedRoles(ctx, ms, groups[0], ms.Spec.Template.Roles[0], roles)
			assert.Empty(t, outdated)
			assert.Zero(t, unavailable)
			assert.False(t, c.hasUpdateableOutdatedRole(ctx, ms, groups[0].Name, ms.Spec.Template.Roles[0], roles))
			assert.Empty(t, c.findOutdatedRolesInServingGroups(ctx, ms, groups, desired))
			state := c.resolveRoleRolloutState(ctx, ms, groups[0], ms.Spec.Template.Roles[0], 1, roles, 0, false, nil)
			assert.False(t, state.hasOldVersion)
			assert.Equal(t, targetReady, state.targetState)
			assert.False(t, roleTemplateChanged(map[string]workloadv1alpha1.Role{"prefill": old.Spec.Template.Roles[0]}, ms.Spec.Template.Roles[0]))
			require.NoError(t, c.syncModelServing(ctx, namespacedKey(ms.Namespace, ms.Name)))
			retained, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, pod.UID, retained.UID)
			assert.Equal(t, pod.Labels, retained.Labels)
		})
	}
}

func TestSemanticComparisonCacheSeparatesDesiredTemplatesAndRetriesHistory(t *testing.T) {
	old := createStandardModelServing("semantic-cache", 1, 1)
	old.UID = "semantic-cache-uid"
	ms := old.DeepCopy()
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "new:v2"
	c := newRevisionTestController(t, ms)
	_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, "old", old.Spec.Template.Roles)
	require.NoError(t, err)
	group := datastore.ServingGroup{Name: "semantic-cache-0", Revision: "old"}
	observed := datastore.Role{Name: "prefill-0", Revision: "old", RoleTemplateHash: "opaque"}
	ctx := c.withRevisionHistory(context.Background(), ms)
	client := c.kubeClientSet.(*kubefake.Clientset)
	client.ClearActions()
	for i := 0; i < 5; i++ {
		assert.Equal(t, templateDifferent, c.compareRoleTemplate(ctx, ms, group, "prefill", observed))
	}
	require.Len(t, client.Actions(), 1, "repeated comparisons reuse the immutable history")
	history := c.revisionHistory(ctx, ms)
	assert.Len(t, history.comparisons, 1)
	// Recovery compares a stable copy with the same UID/resourceVersion against
	// its older desired spec. It must not reuse the latest target's decision.
	stable := ms.DeepCopy()
	stable.Spec.Template.Roles = old.Spec.Template.Roles
	assert.Equal(t, templateEquivalent, c.compareRoleTemplate(ctx, stable, group, "prefill", observed))
	assert.Len(t, history.comparisons, 2)
	observed.Revision = "late"
	assert.Equal(t, templateUnknown, c.compareRoleTemplate(ctx, ms, group, "prefill", observed))
	_, err = utils.CreateControllerRevision(ctx, client, ms, "late", ms.Spec.Template.Roles)
	require.NoError(t, err)
	assert.Equal(t, templateUnknown, c.compareRoleTemplate(ctx, ms, group, "prefill", observed))
	assert.Equal(t, templateEquivalent, c.compareRoleTemplate(context.Background(), ms, group, "prefill", observed), "the next reconcile must retry a previously missing history")
}

func TestSemanticTerminatingCapacityChecksEveryPod(t *testing.T) {
	old := createStandardModelServing("semantic-terminating", 1, 1)
	old.UID = "semantic-terminating-uid"
	old.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Mi")}
	ms := old.DeepCopy()
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("4194304")
	c := newRevisionTestController(t, ms)
	_, err := utils.CreateControllerRevision(context.Background(), c.kubeClientSet, ms, "equal", old.Spec.Template.Roles)
	require.NoError(t, err)
	recordDifferentRevision(t, c, ms, "different")
	pod := addReadyLegacyGroupToController(t, c, ms, old.Spec.Template.Roles[0], 0, "equal")
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	require.NoError(t, c.podsInformer.GetIndexer().Update(pod))
	group := datastore.ServingGroup{Name: "semantic-terminating-0", Revision: "equal"}
	ctx := c.withRevisionHistory(context.Background(), ms)
	assert.Equal(t, templateEquivalent, c.terminatingRoleReplicas(ctx, ms, group)["prefill"][0])
	worker := pod.DeepCopy()
	worker.Name = "semantic-terminating-0-prefill-0-1"
	worker.Labels[workloadv1alpha1.RevisionLabelKey] = "different"
	require.NoError(t, c.podsInformer.GetIndexer().Add(worker))
	assert.Equal(t, templateDifferent, c.terminatingRoleReplicas(ctx, ms, group)["prefill"][0])
	// Live mixed observations must also remain old, even when the cached entry
	// comparison is equal to the target.
	role := datastore.Role{Name: "prefill-0", Revision: "equal", RoleTemplateHash: "opaque"}
	assert.Equal(t, templateDifferent, c.compareRoleTemplate(ctx, ms, group, "prefill", role))
	worker.Labels[workloadv1alpha1.RevisionLabelKey] = "equal"
	require.NoError(t, c.podsInformer.GetIndexer().Update(worker))
	assert.Equal(t, templateEquivalent, c.compareRoleTemplate(ctx, ms, group, "prefill", role))
}
