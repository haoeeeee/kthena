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

package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
	corev1 "k8s.io/api/core/v1"
	extensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func configurationFixture() *api.ModelServing {
	return &api.ModelServing{ObjectMeta: metav1.ObjectMeta{Name: "config", Namespace: "default", UID: "owner"}, Spec: api.ModelServingSpec{
		SchedulerName: "volcano", Plugins: []api.PluginSpec{{Name: "demo-pod-tweaks", Config: &extensions.JSON{Raw: []byte(`{"annotations":{"version":"old"}}`)}}},
		Template: api.ServingGroup{Roles: []api.Role{{Name: "p", Replicas: ptr.To[int32](3), EntryTemplate: api.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "server", Image: "old"}}}}}}},
	}}
}

func TestRevisionIgnoresImmutableConfigurationAndKeepsRecordedCounts(t *testing.T) {
	ctx := context.Background()
	ms := configurationFixture()
	client := fake.NewSimpleClientset()
	revision := ModelServingRevision(ms)
	cr, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, revision, ms.Spec.Template.Roles)
	require.NoError(t, err)
	require.NotContains(t, string(cr.Data.Raw), "schedulerName")
	require.NotContains(t, string(cr.Data.Raw), "plugins")
	for _, change := range []func(*api.ModelServing){
		func(m *api.ModelServing) { m.Spec.SchedulerName = "custom" },
		func(m *api.ModelServing) { m.Spec.Plugins = nil },
	} {
		changed := ms.DeepCopy()
		change(changed)
		require.Equal(t, revision, ModelServingRevision(changed))
		// Immutable owner configuration neither changes Role identity nor
		// requires a second historical snapshot.
		same, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), changed, revision, changed.Spec.Template.Roles)
		require.NoError(t, err)
		require.Equal(t, cr.Data.Raw, same.Data.Raw)
	}
	scaled := ms.DeepCopy()
	scaled.Spec.Template.Roles[0].Replicas = ptr.To[int32](7)
	require.Equal(t, revision, ModelServingRevision(scaled))
	same, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), scaled, revision, scaled.Spec.Template.Roles)
	require.NoError(t, err)
	require.Equal(t, cr.Data.Raw, same.Data.Raw)
	roles, err := GetRolesFromControllerRevision(same)
	require.NoError(t, err)
	require.EqualValues(t, 3, *roles[0].Replicas)
	equivalent := ms.DeepCopy()
	equivalent.Spec.Plugins[0].Config.Raw = []byte(`{ "annotations": { "version": "old" } }`)
	require.Equal(t, revision, ModelServingRevision(equivalent))
}

func TestRevisionRoleDecodeRejectsUnsupportedOrMalformedV1(t *testing.T) {
	ms := configurationFixture()
	cr, err := CreateControllerRevision(context.Background(), fake.NewSimpleClientset(), revisiontest.ControllerRevisions(fake.NewSimpleClientset()), ms, "history", ms.Spec.Template.Roles)
	require.NoError(t, err)
	cr.Annotations = map[string]string{ControllerRevisionDataVersionAnnotation: "v2"}
	_, err = GetRolesFromControllerRevision(cr)
	require.ErrorContains(t, err, "unsupported revision data version")
	cr.Annotations[ControllerRevisionDataVersionAnnotation] = ControllerRevisionDataVersionV1
	cr.Data.Raw = []byte(`{"data":[{"name":"p","replicas":3}]}`)
	_, err = GetRolesFromControllerRevision(cr)
	require.ErrorContains(t, err, "no roles", "a wrapped Roles array must not hide a corrupt v1 patch")
}
