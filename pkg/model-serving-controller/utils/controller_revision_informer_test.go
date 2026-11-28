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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	appslisters "k8s.io/client-go/listers/apps/v1"
	kubetesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
)

func TestCleanupControllerRevisionsRejectsStaleInformerVersion(t *testing.T) {
	ctx := context.Background()
	ms := &api.ModelServing{ObjectMeta: metav1.ObjectMeta{Name: "ms", Namespace: "default", UID: "owner"}, Spec: api.ModelServingSpec{RevisionHistoryLimit: ptr.To[int32](0)}}
	client := kubefake.NewSimpleClientset()
	cr, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "a", []api.Role{{Name: "infer"}})
	require.NoError(t, err)
	cr.UID = "snapshot-owner"
	cr.ResourceVersion = "1"
	_, err = client.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, cr, metav1.UpdateOptions{})
	require.NoError(t, err)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, indexer.Add(cr.DeepCopy()))
	lister := appslisters.NewControllerRevisionLister(indexer)
	promoted := cr.DeepCopy()
	promoted.ResourceVersion = "2"
	promoted.Revision = 2
	_, err = client.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, promoted, metav1.UpdateOptions{})
	require.NoError(t, err)
	client.PrependReactor("delete", "controllerrevisions", func(action kubetesting.Action) (bool, runtime.Object, error) {
		options := action.(kubetesting.DeleteAction).GetDeleteOptions()
		require.Equal(t, cr.UID, *options.Preconditions.UID)
		if *options.Preconditions.ResourceVersion != promoted.ResourceVersion {
			return true, nil, apierrors.NewConflict(appsv1.Resource("controllerrevisions"), cr.Name, fmt.Errorf("snapshot changed since informer observation"))
		}
		return false, nil, nil
	})
	require.ErrorContains(t, CleanupOldControllerRevisions(ctx, client, lister, ms), "snapshot changed since informer observation")
	preserved, err := client.AppsV1().ControllerRevisions(ms.Namespace).Get(ctx, cr.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.EqualValues(t, 2, preserved.Revision)
	cached, err := lister.ControllerRevisions(ms.Namespace).Get(cr.Name)
	require.NoError(t, err)
	require.EqualValues(t, 1, cached.Revision)
	require.NoError(t, indexer.Update(promoted.DeepCopy()))
	require.NoError(t, CleanupOldControllerRevisions(ctx, client, lister, ms))
	_, err = client.AppsV1().ControllerRevisions(ms.Namespace).Get(ctx, cr.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
}
