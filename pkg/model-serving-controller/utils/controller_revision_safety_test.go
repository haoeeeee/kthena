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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func TestCreateControllerRevisionConcurrentCreation(t *testing.T) {
	for _, outcome := range []string{"equivalent", "different", "foreign"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			ms := &workloadv1alpha1.ModelServing{ObjectMeta: metav1.ObjectMeta{Name: "ms", Namespace: "default", UID: "owner"}}
			roles := []workloadv1alpha1.Role{{Name: "prefill"}}
			client := kubefake.NewSimpleClientset()
			client.PrependReactor("create", "controllerrevisions", func(action kubetesting.Action) (bool, runtime.Object, error) {
				created := action.(kubetesting.CreateAction).GetObject().(*appsv1.ControllerRevision).DeepCopy()
				if outcome == "different" {
					created.Data.Raw = []byte(`{"data":[{"name":"decode"}]}`)
				} else if outcome == "foreign" {
					created.OwnerReferences[0].UID = "other-owner"
				}
				require.NoError(t, client.Tracker().Add(created))
				return true, nil, apierrors.NewAlreadyExists(appsv1.Resource("controllerrevisions"), created.Name)
			})
			cr, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, "revision", roles)
			if outcome == "equivalent" {
				require.NoError(t, err)
				require.NotNil(t, cr)
			} else {
				require.Error(t, err)
			}
			for _, action := range client.Actions() {
				require.NotContains(t, []string{"patch", "update", "delete"}, action.GetVerb(), "never mutate conflicting history")
			}
		})
	}
}

func TestCleanupControllerRevisionsLivePodsOwnershipAndErrors(t *testing.T) {
	ctx := context.Background()
	ms := &workloadv1alpha1.ModelServing{
		ObjectMeta: metav1.ObjectMeta{Name: "ms", Namespace: "default", UID: "owner"},
		Spec:       workloadv1alpha1.ModelServingSpec{RevisionHistoryLimit: ptr.To[int32](0)},
		Status:     workloadv1alpha1.ModelServingStatus{CurrentRevision: "current", UpdateRevision: "target"},
	}
	client := kubefake.NewSimpleClientset()
	for _, revision := range []string{"current", "target", "role", "pod", "terminating", "unused", "foreign"} {
		cr, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, revision, []workloadv1alpha1.Role{{Name: "prefill"}})
		require.NoError(t, err)
		cr.UID = "uid-" + ms.UID
		cr.ResourceVersion = "rv-" + revision
		if revision == "foreign" {
			cr.OwnerReferences[0].UID = "other-owner"
		}
		_, err = client.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, cr, metav1.UpdateOptions{})
		require.NoError(t, err)
	}
	for _, revision := range []string{"pod", "terminating", "unused"} {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: revision, Namespace: ms.Namespace,
			Labels:          map[string]string{ControllerRevisionLabelKey: ms.Name, ControllerRevisionRevisionLabelKey: revision},
			OwnerReferences: []metav1.OwnerReference{newModelServingOwnerRef(ms)},
		}}
		if revision == "terminating" {
			now := metav1.Now()
			pod.DeletionTimestamp = &now
		} else if revision == "unused" {
			pod.OwnerReferences[0].UID = "previous-owner"
		}
		_, err := client.CoreV1().Pods(ms.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	failList, failDelete := true, true
	client.PrependReactor("list", "pods", func(kubetesting.Action) (bool, runtime.Object, error) {
		if failList {
			return true, nil, fmt.Errorf("injected Pod list failure")
		}
		return false, nil, nil
	})
	client.PrependReactor("delete", "controllerrevisions", func(action kubetesting.Action) (bool, runtime.Object, error) {
		deletion := action.(kubetesting.DeleteAction)
		require.Equal(t, "uid-owner", string(*deletion.GetDeleteOptions().Preconditions.UID))
		require.NotNil(t, deletion.GetDeleteOptions().Preconditions.ResourceVersion)
		require.Equal(t, "rv-"+strings.TrimPrefix(deletion.GetName(), ms.Name+"-"), *deletion.GetDeleteOptions().Preconditions.ResourceVersion)
		if failDelete {
			return true, nil, fmt.Errorf("injected history delete failure")
		}
		return false, nil, nil
	})
	client.ClearActions()
	require.ErrorContains(t, CleanupOldControllerRevisions(ctx, client, revisiontest.ControllerRevisions(client), ms, "role"), "injected Pod list failure")
	for _, action := range client.Actions() {
		require.False(t, action.Matches("delete", "controllerrevisions"), "a failed reference list must prevent all cleanup")
	}
	failList = false
	require.ErrorContains(t, CleanupOldControllerRevisions(ctx, client, revisiontest.ControllerRevisions(client), ms, "role"), "injected history delete failure")
	failDelete = false
	require.NoError(t, CleanupOldControllerRevisions(ctx, client, revisiontest.ControllerRevisions(client), ms, "role"))
	history, err := client.AppsV1().ControllerRevisions(ms.Namespace).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	names := make([]string, 0, len(history.Items))
	for _, cr := range history.Items {
		names = append(names, cr.Name)
	}
	require.ElementsMatch(t, []string{"ms-current", "ms-target", "ms-role", "ms-pod", "ms-terminating", "ms-foreign"}, names)
	// A subsequent cleanup removes history after the last actual reference is gone.
	require.NoError(t, client.CoreV1().Pods(ms.Namespace).Delete(ctx, "terminating", metav1.DeleteOptions{}))
	require.NoError(t, CleanupOldControllerRevisions(ctx, client, revisiontest.ControllerRevisions(client), ms, "role"))
	_, err = client.AppsV1().ControllerRevisions(ms.Namespace).Get(ctx, "ms-terminating", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
}

func TestCleanupControllerRevisionHistoryLimit(t *testing.T) {
	for _, limit := range []*int32{nil, ptr.To[int32](0), ptr.To[int32](2)} {
		name, retain := "default", 10
		if limit != nil {
			name, retain = fmt.Sprint(*limit), int(*limit)
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ms := &workloadv1alpha1.ModelServing{
				ObjectMeta: metav1.ObjectMeta{Name: "ms", Namespace: "default", UID: "owner"},
				Spec:       workloadv1alpha1.ModelServingSpec{RevisionHistoryLimit: limit},
				Status:     workloadv1alpha1.ModelServingStatus{CurrentRevision: "r00", UpdateRevision: "r01"},
			}
			client := kubefake.NewSimpleClientset()
			for i := 0; i < 15; i++ {
				cr, err := CreateControllerRevision(ctx, client, revisiontest.ControllerRevisions(client), ms, fmt.Sprintf("r%02d", i), []workloadv1alpha1.Role{{Name: "prefill"}})
				require.NoError(t, err)
				cr.Revision = int64(i + 1)
				_, err = client.AppsV1().ControllerRevisions(ms.Namespace).Update(ctx, cr, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			require.NoError(t, CleanupOldControllerRevisions(ctx, client, revisiontest.ControllerRevisions(client), ms, "r02"))
			history, err := client.AppsV1().ControllerRevisions(ms.Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			expected := []string{"ms-r00", "ms-r01", "ms-r02"}
			for i := 15 - retain; i < 15; i++ {
				expected = append(expected, fmt.Sprintf("ms-r%02d", i))
			}
			var names []string
			for _, cr := range history.Items {
				names = append(names, cr.Name)
			}
			require.ElementsMatch(t, expected, names, "live references must not count toward the non-live history limit")
		})
	}
}
