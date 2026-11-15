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
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/inplace"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func noneFailedPodFixture(t *testing.T, grace *int64) (*ModelServingController, *api.ModelServing, []*corev1.Pod) {
	t.Helper()
	ms := createStandardModelServing("failed", 1, 1)
	ms.UID = "failed-owner"
	ms.Spec.RecoveryPolicy = api.NoneRestartPolicy
	ms.Spec.Template.RestartGracePeriodSeconds = grace
	role := &ms.Spec.Template.Roles[0]
	role.WorkerReplicas = 1
	role.WorkerTemplate = role.EntryTemplate.DeepCopy()
	ms.Status.Replicas, ms.Status.AvailableReplicas = 1, 1
	c := newRevisionTestController(t, ms)
	c.initialSync.Store(true)
	revision := utils.ModelServingRevision(ms)
	c.store.AddServingGroup(utils.GetNamespaceName(ms), 0, revision)
	pods := addMergeReadyInstance(t, c, ms, ms.Name+"-0", *role, 0, revision, revision, utils.CalRoleTemplateHash(*role))
	require.NoError(t, c.store.UpdateServingGroupStatus(utils.GetNamespaceName(ms), ms.Name+"-0", datastore.ServingGroupRunning))
	return c, ms, pods
}

func setFailedPod(t *testing.T, c *ModelServingController, pod *corev1.Pod) *corev1.Pod {
	t.Helper()
	pod = pod.DeepCopy()
	pod.Status = corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted"}
	updated, err := c.kubeClientSet.CoreV1().Pods(pod.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, c.podsInformer.GetIndexer().Update(updated))
	return updated
}

func TestNoneFailedPodFollowsRestartGrace(t *testing.T) {
	for _, tt := range []struct {
		name  string
		grace *int64
	}{
		{name: "default"},
		{name: "zero", grace: ptr.To[int64](0)},
		{name: "positive", grace: ptr.To[int64](1)},
		{name: "disabled", grace: ptr.To[int64](-1)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, ms, pods := noneFailedPodFixture(t, tt.grace)
			pod := setFailedPod(t, c, pods[1])
			c.updatePod(pods[1], pod)
			client := c.kubeClientSet.(*kubefake.Clientset)
			if tt.grace != nil && *tt.grace == -1 {
				for _, action := range client.Actions() {
					require.False(t, action.Matches("delete", "pods"))
				}
				_, waiting := c.graceMap.Load(getPodGracePeriodKey(pod))
				require.False(t, waiting)
				return
			}
			if tt.grace != nil && *tt.grace > 0 {
				_, err := client.CoreV1().Pods(ms.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
				require.NoError(t, err, "Failed Pods still honor the existing grace period")
				startedAt, waiting := c.graceMap.Load(getPodGracePeriodKey(pod))
				require.True(t, waiting)
				c.updatePod(pod, pod)
				again, waiting := c.graceMap.Load(getPodGracePeriodKey(pod))
				require.True(t, waiting)
				require.Equal(t, startedAt, again, "a repeated event must not restart the grace period")
			}
			// The original goroutine must delete the Pod without another event.
			require.Eventually(t, func() bool {
				_, err := client.CoreV1().Pods(ms.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, 3*time.Second, 10*time.Millisecond)
			deletions := 0
			for _, action := range client.Actions() {
				if deletion, ok := action.(clienttesting.DeleteAction); ok && action.Matches("delete", "pods") {
					deletions++
					require.Equal(t, pod.Name, deletion.GetName())
					require.Equal(t, pod.UID, *deletion.GetDeleteOptions().Preconditions.UID)
				}
			}
			require.Equal(t, 1, deletions)
		})
	}
}

func TestNoneReplacesOnlyFailedPod(t *testing.T) {
	for _, ordinal := range []int{0, 1} {
		t.Run(fmt.Sprintf("member-%d", ordinal), func(t *testing.T) {
			c, ms, pods := noneFailedPodFixture(t, nil)
			client := c.kubeClientSet.(*kubefake.Clientset)
			pod := setFailedPod(t, c, pods[ordinal])
			c.updatePod(pods[ordinal], pod)
			require.Eventually(t, func() bool {
				_, err := client.CoreV1().Pods(ms.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, time.Second, 10*time.Millisecond)
			key := ms.Namespace + "/" + ms.Name
			// Until deletion reaches the informer, the old identity remains occupied.
			require.NoError(t, c.syncModelServing(context.Background(), key))
			_, err := client.CoreV1().Pods(ms.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
			require.True(t, apierrors.IsNotFound(err))
			require.NoError(t, c.podsInformer.GetIndexer().Delete(pod))
			c.deletePod(pod)
			client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				// The fake API does not allocate UIDs.
				created := action.(clienttesting.CreateAction).GetObject().(*corev1.Pod)
				created.UID = types.UID(created.Name + "-replacement")
				return false, nil, nil
			})
			require.NoError(t, c.syncModelServing(context.Background(), key))
			replacement, err := client.CoreV1().Pods(ms.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.NotEqual(t, pod.UID, replacement.UID)
			require.Equal(t, utils.ObjectRevision(pod), utils.ObjectRevision(replacement))
			sibling := pods[1-ordinal]
			actual, err := client.CoreV1().Pods(ms.Namespace).Get(context.Background(), sibling.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, sibling.UID, actual.UID)
			for _, action := range client.Actions() {
				require.False(t, action.Matches("delete-collection", "pods"))
				if deletion, ok := action.(clienttesting.DeleteAction); ok && action.Matches("delete", "pods") {
					require.Equal(t, pod.Name, deletion.GetName())
				}
			}
		})
	}
}

func TestNoneFailedPodDuringInPlaceUpdate(t *testing.T) {
	c, ms, pods := noneFailedPodFixture(t, nil)
	pod := setFailedPod(t, c, pods[1])
	pod.Annotations = map[string]string{api.InPlaceUpdateStateAnnotationKey: marshalState(t, &inplace.State{Revision: "target"})}
	require.True(t, inplace.Updating(pod))
	require.NoError(t, c.podsInformer.GetIndexer().Update(pod))
	c.updatePod(pods[1], pod)
	require.Eventually(t, func() bool {
		_, err := c.kubeClientSet.CoreV1().Pods(ms.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, time.Second, 10*time.Millisecond)
}
