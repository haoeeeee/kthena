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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
	extensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestImmutableOwnerConfigurationUsedWithHistoricalRole(t *testing.T) {
	ctx := context.Background()
	old := createStandardModelServing("configuration", 1, 1)
	old.Spec.SchedulerName = "volcano"
	old.Spec.Plugins = []api.PluginSpec{{Name: "demo-pod-tweaks", Type: api.PluginTypeBuiltIn, Config: &extensions.JSON{Raw: []byte(`{"annotations":{"version":"old"}}`)}}}
	desired := old.DeepCopy()
	desired.Spec.Plugins[0].Config.Raw = []byte(`{"annotations":{"version":"owner"}}`)
	desired.Spec.SchedulerName = "custom"
	c := newRevisionTestController(t, desired)
	cr, err := utils.CreateControllerRevision(ctx, c.kubeClientSet, revisiontest.ControllerRevisions(c.kubeClientSet), old, "historical", old.Spec.Template.Roles)
	require.NoError(t, err)
	// Exercise a versioned snapshot as well as the controller's Roles-only
	// format. Its global fields must not override the fixed owner inputs.
	cr.Data.Raw, err = utils.BuildRevisionData(old)
	require.NoError(t, err)
	var snapshot map[string]any
	require.NoError(t, json.Unmarshal(cr.Data.Raw, &snapshot))
	snapshot["data"] = old.Spec.Template.Roles
	cr.Data.Raw, err = json.Marshal(snapshot)
	require.NoError(t, err)
	cr.Annotations = map[string]string{utils.ControllerRevisionDataVersionAnnotation: utils.ControllerRevisionDataVersionV1}
	_, err = c.kubeClientSet.AppsV1().ControllerRevisions(old.Namespace).Update(ctx, cr, metav1.UpdateOptions{})
	require.NoError(t, err)
	role := old.Spec.Template.Roles[0]
	observed := datastore.Role{Revision: "historical", RoleTemplateHash: utils.CalRoleTemplateHash(role)}
	// Deliberately inconsistent history models data left by the old mutable
	// configuration implementation. Legal API updates cannot change globals.
	for _, hash := range []string{observed.RoleTemplateHash, "legacy-hash"} {
		observed.RoleTemplateHash = hash
		require.Equal(t, templateEquivalent, c.compareRoleTemplate(ctx, desired, datastore.ServingGroup{Revision: "historical"}, role.Name, observed))
	}
	target, err := c.revisionHistory(ctx, desired).desiredRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, "historical", target)
	// Recovery still renders the selected historical Role, even with a newer
	// desired Role waiting behind partition.
	desired.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "new-template"
	require.NoError(t, c.CreatePodsByRole(ctx, role, desired, 0, 0, "historical", observed.RoleTemplateHash, ""))
	pods, err := c.kubeClientSet.CoreV1().Pods(old.Namespace).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, pods.Items)
	for _, pod := range pods.Items {
		require.Equal(t, "custom", pod.Spec.SchedulerName)
		require.Equal(t, "owner", pod.Annotations["version"])
		require.Equal(t, role.EntryTemplate.Spec.Containers[0].Image, pod.Spec.Containers[0].Image)
	}
	unchanged, err := utils.GetControllerRevision(ctx, c.kubeClientSet, old, "historical")
	require.NoError(t, err)
	require.Equal(t, cr.Data.Raw, unchanged.Data.Raw)
}

func TestLegacyConfigurationMigrationKeepsRevision(t *testing.T) {
	ctx := context.Background()
	ms := createStandardModelServing("migration", 1, 1)
	ms.Spec.SchedulerName = "volcano"
	ms.Status.CurrentRevision = "legacy"
	ms.Status.UpdateRevision = "legacy"
	c := newRevisionTestController(t, ms)
	source, err := utils.CreateControllerRevision(ctx, c.kubeClientSet, revisiontest.ControllerRevisions(c.kubeClientSet), ms, "legacy", ms.Spec.Template.Roles)
	require.NoError(t, err)
	source.Annotations = nil
	source.Data.Raw, err = json.Marshal(map[string]any{"data": ms.Spec.Template.Roles})
	require.NoError(t, err)
	_, err = c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, source, metav1.UpdateOptions{})
	require.NoError(t, err)
	revision, err := c.revisionHistory(ctx, ms).desiredRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, "legacy", revision)
	original, err := utils.GetControllerRevision(ctx, c.kubeClientSet, ms, "legacy")
	require.NoError(t, err)
	require.Equal(t, source, original)
	_, err = c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).Get(ctx, source.Name+"-baseline", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	list, err := c.kubeClientSet.AppsV1().ControllerRevisions(ms.Namespace).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
}
