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
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
)

func TestCreateControllerRevisionNumber(t *testing.T) {
	for _, tc := range []struct {
		name    string
		history []int64
		want    int64
	}{
		{name: "first revision", want: 1},
		{name: "legacy history all numbered one", history: []int64{1, 1, 1}, want: 2},
		{name: "maximum rather than history length", history: []int64{7, 2, 4}, want: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ms := &workloadv1alpha1.ModelServing{ObjectMeta: metav1.ObjectMeta{Name: "ms", Namespace: "default", UID: "owner"}}
			client := kubefake.NewSimpleClientset()
			for i, number := range tc.history {
				cr := &appsv1.ControllerRevision{
					ObjectMeta: metav1.ObjectMeta{
						Name: fmt.Sprintf("ms-old-%d", i), Namespace: ms.Namespace,
						Labels:          map[string]string{ControllerRevisionLabelKey: ms.Name},
						OwnerReferences: []metav1.OwnerReference{newModelServingOwnerRef(ms)},
					},
					Revision: number,
				}
				require.NoError(t, client.Tracker().Add(cr))
			}
			// Unrelated history must never affect this ModelServing's sequence.
			for i, kind := range []string{"previous owner", "unowned", "other namespace", "other model serving"} {
				cr := &appsv1.ControllerRevision{
					ObjectMeta: metav1.ObjectMeta{
						Name: fmt.Sprintf("foreign-%d", i), Namespace: ms.Namespace,
						Labels:          map[string]string{ControllerRevisionLabelKey: ms.Name},
						OwnerReferences: []metav1.OwnerReference{newModelServingOwnerRef(ms)},
					},
					Revision: 99,
				}
				switch kind {
				case "previous owner":
					cr.OwnerReferences[0].UID = "previous-owner"
				case "unowned":
					cr.OwnerReferences = nil
				case "other namespace":
					cr.Namespace = "other"
				case "other model serving":
					cr.Labels[ControllerRevisionLabelKey] = "other"
					cr.OwnerReferences[0].UID = "other-owner"
				}
				_, err := client.AppsV1().ControllerRevisions(cr.Namespace).Create(ctx, cr, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			roles := []workloadv1alpha1.Role{{Name: "prefill"}}
			created, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "new", roles)
			require.NoError(t, err)
			require.Equal(t, tc.want, created.Revision)
			reused, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "new", roles)
			require.NoError(t, err)
			require.Equal(t, created, reused)
			next, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "next", roles)
			require.NoError(t, err)
			require.Equal(t, tc.want+1, next.Revision)
			// Reusing older history after a newer creation must preserve its number.
			reused, err = CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "new", roles)
			require.NoError(t, err)
			require.Equal(t, created, reused)
		})
	}
}

func TestCreateControllerRevisionNumberAfterCleanup(t *testing.T) {
	ctx := context.Background()
	ms := &workloadv1alpha1.ModelServing{
		ObjectMeta: metav1.ObjectMeta{Name: "ms", Namespace: "default", UID: "owner"},
		Spec:       workloadv1alpha1.ModelServingSpec{RevisionHistoryLimit: ptr.To[int32](0)},
		Status:     workloadv1alpha1.ModelServingStatus{CurrentRevision: "r3", UpdateRevision: "r3"},
	}
	client := kubefake.NewSimpleClientset()
	roles := []workloadv1alpha1.Role{{Name: "prefill"}}
	for i := 1; i <= 3; i++ {
		_, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, fmt.Sprintf("r%d", i), roles)
		require.NoError(t, err)
	}
	require.NoError(t, CleanupOldControllerRevisions(ctx, client, revisiontest.ControllerRevisions(client), ms))
	history, err := client.AppsV1().ControllerRevisions(ms.Namespace).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, history.Items, 1)
	created, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "r4", roles)
	require.NoError(t, err)
	require.EqualValues(t, 4, created.Revision)
}

func TestCreateControllerRevisionNumberListError(t *testing.T) {
	ctx := context.Background()
	ms := &workloadv1alpha1.ModelServing{ObjectMeta: metav1.ObjectMeta{Name: "ms", Namespace: "default", UID: "owner"}}
	client := kubefake.NewSimpleClientset()
	roles := []workloadv1alpha1.Role{{Name: "prefill"}}
	existing, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "existing", roles)
	require.NoError(t, err)
	listErr := fmt.Errorf("history unavailable")
	unavailable := revisiontest.FailingControllerRevisions(listErr)
	client.ClearActions()
	_, err = CreateControllerRevision(ctx, client, unavailable, ms, "new", roles)
	require.ErrorIs(t, err, listErr)
	for _, action := range client.Actions() {
		require.False(t, action.Matches("create", "controllerrevisions"), "cannot allocate a number without history")
	}
	reused, err := CreateControllerRevision(ctx, client, unavailable, ms, "existing", roles)
	require.NoError(t, err)
	require.Equal(t, existing, reused)
}

func TestCleanupControllerRevisionNumberOrdering(t *testing.T) {
	ctx := context.Background()
	ms := &workloadv1alpha1.ModelServing{
		ObjectMeta: metav1.ObjectMeta{Name: "ms", Namespace: "default", UID: "owner"},
		Spec:       workloadv1alpha1.ModelServingSpec{RevisionHistoryLimit: ptr.To[int32](1)},
		Status:     workloadv1alpha1.ModelServingStatus{CurrentRevision: "r1", UpdateRevision: "r1"},
	}
	client := kubefake.NewSimpleClientset()
	for i := 1; i <= 3; i++ {
		cr, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, fmt.Sprintf("r%d", i), []workloadv1alpha1.Role{{Name: "prefill"}})
		require.NoError(t, err)
		// Higher numbers deliberately have older timestamps.
		cr.CreationTimestamp = metav1.NewTime(time.Unix(int64(400-i*100), 0))
		_, err = client.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, cr, metav1.UpdateOptions{})
		require.NoError(t, err)
	}
	require.NoError(t, CleanupOldControllerRevisions(ctx, client, revisiontest.ControllerRevisions(client), ms))
	history, err := client.AppsV1().ControllerRevisions(ms.Namespace).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	var names []string
	for _, cr := range history.Items {
		names = append(names, cr.Name)
	}
	require.ElementsMatch(t, []string{"ms-r1", "ms-r3"}, names, "preserve active low-numbered history and the highest unused number")
}
