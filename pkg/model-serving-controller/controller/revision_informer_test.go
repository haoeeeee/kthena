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
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	appslisters "k8s.io/client-go/listers/apps/v1"
	kubetesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

func revisionInformerController(t *testing.T) (*ModelServingController, *api.ModelServing) {
	t.Helper()
	ms := createStandardModelServing("revision-cache", 1, 1)
	ms.UID = "owner"
	c := newRevisionTestController(t, ms)
	c.controllerRevisionLister = appslisters.NewControllerRevisionLister(c.controllerRevisionsInformer.GetIndexer())
	t.Cleanup(c.workqueue.ShutDown)
	return c, ms
}

func forbidRevisionAPILists(client *kubefake.Clientset) {
	client.PrependReactor("list", "controllerrevisions", func(kubetesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("reconciliation must use the revision lister")
	})
	client.ClearActions()
}

func TestControllerRevisionInformerListWatch(t *testing.T) {
	c, ms := revisionInformerController(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := c.kubeClientSet.(*kubefake.Clientset)
	cr, err := utils.CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "a", ms.Spec.Template.Roles)
	require.NoError(t, err)
	require.NotContains(t, cr.Labels, api.GroupNameLabelKey, "revision informer must not inherit the Pod group filter")
	// Objects without the ModelServing label must not populate this informer.
	_, err = client.AppsV1().ControllerRevisions(ms.Namespace).Create(ctx, &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "unlabelled"}, Revision: 100}, metav1.CreateOptions{})
	require.NoError(t, err)
	go c.controllerRevisionsInformer.RunWithContext(ctx)
	require.True(t, cache.WaitForCacheSync(ctx.Done(), c.controllerRevisionsInformer.HasSynced))
	listed, err := c.controllerRevisionLister.ControllerRevisions(ms.Namespace).List(labels.Everything())
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, cr.Name, listed[0].Name)
	updated := cr.DeepCopy()
	updated.Revision = 7
	updated.Annotations = map[string]string{"test/version": "new"}
	_, err = client.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		got, e := c.controllerRevisionLister.ControllerRevisions(ms.Namespace).Get(cr.Name)
		return e == nil && got.Revision == 7 && got.Annotations["test/version"] == "new"
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, client.AppsV1().ControllerRevisions(ms.Namespace).Delete(ctx, cr.Name, metav1.DeleteOptions{}))
	require.Eventually(t, func() bool {
		_, e := c.controllerRevisionLister.ControllerRevisions(ms.Namespace).Get(cr.Name)
		return apierrors.IsNotFound(e)
	}, 2*time.Second, 10*time.Millisecond)
}

func TestDesiredRevisionUsesInformerHistoryForNumber(t *testing.T) {
	c, ms := revisionInformerController(t)
	ctx := context.Background()
	client := c.kubeClientSet.(*kubefake.Clientset)
	cr, err := utils.CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "old", ms.Spec.Template.Roles)
	require.NoError(t, err)
	cr.Revision = 7
	_, err = client.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, cr, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, c.controllerRevisionsInformer.GetIndexer().Add(cr.DeepCopy()))
	foreign := cr.DeepCopy()
	foreign.Name = "foreign"
	foreign.Revision = 99
	foreign.OwnerReferences[0].UID = "other-owner"
	require.NoError(t, c.controllerRevisionsInformer.GetIndexer().Add(foreign))
	forbidRevisionAPILists(client)
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "new:v8"
	h := c.revisionHistory(ctx, ms)
	selected, err := h.desiredRevision(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 8, h.snapshots[selected].revision.Revision)
	require.EqualValues(t, 7, cr.Revision)
	require.EqualValues(t, 99, foreign.Revision, "cached objects must remain immutable")
	for _, action := range client.Actions() {
		require.False(t, action.Matches("list", "controllerrevisions"))
	}
}

func TestDesiredRevisionCacheMissReusesPersistedSnapshot(t *testing.T) {
	c, ms := revisionInformerController(t)
	ctx := context.Background()
	client := c.kubeClientSet.(*kubefake.Clientset)
	a, err := utils.CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "a", ms.Spec.Template.Roles)
	require.NoError(t, err)
	require.NoError(t, c.controllerRevisionsInformer.GetIndexer().Add(a.DeepCopy()))
	ms.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "new:v2"
	b, err := utils.CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, utils.ModelServingRevision(ms), ms.Spec.Template.Roles)
	require.NoError(t, err)
	// B exists in the API, but its watch event has not reached the informer.
	forbidRevisionAPILists(client)
	h := c.revisionHistory(ctx, ms)
	selected, err := h.desiredRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, b.Labels[utils.ControllerRevisionRevisionLabelKey], selected)
	require.EqualValues(t, 2, h.snapshots[selected].revision.Revision)
	for _, action := range client.Actions() {
		require.False(t, action.Matches("create", "controllerrevisions"), "cache delay must not create a duplicate snapshot")
	}
}

func TestDesiredRevisionStaleCacheDoesNotRepromoteRollback(t *testing.T) {
	c, ms := revisionInformerController(t)
	ctx := context.Background()
	client := c.kubeClientSet.(*kubefake.Clientset)
	a, err := utils.CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "a", ms.Spec.Template.Roles)
	require.NoError(t, err)
	a.ResourceVersion = "1"
	require.NoError(t, c.controllerRevisionsInformer.GetIndexer().Add(a.DeepCopy()))
	other := ms.DeepCopy()
	other.Spec.Template.Roles[0].EntryTemplate.Spec.Containers[0].Image = "new:v2"
	b, err := utils.CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), other, "b", other.Spec.Template.Roles)
	require.NoError(t, err)
	require.NoError(t, c.controllerRevisionsInformer.GetIndexer().Add(b.DeepCopy()))
	promoted := a.DeepCopy()
	promoted.Revision = 3
	promoted.ResourceVersion = "3"
	promoted.Annotations = map[string]string{"test/version": "persisted"}
	_, err = client.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, promoted, metav1.UpdateOptions{})
	require.NoError(t, err)
	client.PrependReactor("update", "controllerrevisions", func(action kubetesting.Action) (bool, runtime.Object, error) {
		candidate := action.(kubetesting.UpdateAction).GetObject().(*appsv1.ControllerRevision)
		if candidate.ResourceVersion != "3" {
			return true, nil, apierrors.NewConflict(appsv1.Resource("controllerrevisions"), candidate.Name, fmt.Errorf("stale informer resourceVersion"))
		}
		return false, nil, nil
	})
	forbidRevisionAPILists(client)
	for i := 0; i < 2; i++ {
		h := c.revisionHistory(ctx, ms)
		selected, err := h.desiredRevision(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 3, h.snapshots[selected].revision.Revision)
		require.Equal(t, "persisted", h.snapshots[selected].revision.Annotations["test/version"])
	}
	cached, err := c.controllerRevisionLister.ControllerRevisions(ms.Namespace).Get(a.Name)
	require.NoError(t, err)
	require.EqualValues(t, 1, cached.Revision, "writes must not mutate informer objects")
	require.Empty(t, cached.Annotations)
	require.NoError(t, c.controllerRevisionsInformer.GetIndexer().Update(promoted.DeepCopy()))
	client.ClearActions()
	_, err = c.revisionHistory(ctx, ms).desiredRevision(ctx)
	require.NoError(t, err)
	require.Empty(t, client.Actions(), "after watch delivery the latest snapshot is reused entirely from cache")
}
