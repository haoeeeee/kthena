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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

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
			ms, pods := recoveryFixture(api.NoneRestartPolicy, 1)
			ms.Spec.Template.RestartGracePeriodSeconds = tt.grace
			c, kube := recoveryController(t, ms, pods...)
			ctx, cancel := context.WithCancel(context.Background())
			c.recoveryCtx = ctx
			t.Cleanup(cancel)
			failed := pods[1].DeepCopy()
			failed.Status = corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted"}
			updateRecoveryPod(t, c, failed)
			c.updatePod(pods[1], failed)
			key := getPodGracePeriodKey(failed)
			if tt.grace != nil && *tt.grace == -1 {
				_, waiting := c.graceMap.Load(key)
				require.False(t, waiting)
				require.Empty(t, recoveryDeletedPods(kube))
				return
			}
			if tt.grace != nil && *tt.grace > 0 {
				require.Empty(t, recoveryDeletedPods(kube))
				started, waiting := c.graceMap.Load(key)
				require.True(t, waiting)
				c.updatePod(failed, failed)
				again, waiting := c.graceMap.Load(key)
				require.True(t, waiting)
				require.Equal(t, started, again, "duplicate events must not restart grace")
			}
			// Recovery must complete without another Pod event.
			require.Eventually(t, func() bool {
				_, err := kube.CoreV1().Pods(ms.Namespace).Get(ctx, failed.Name, metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, 3*time.Second, 10*time.Millisecond)
			require.Equal(t, []string{failed.Name}, recoveryDeletedPods(kube))
			for _, action := range kube.Actions() {
				if deletion, ok := action.(clienttesting.DeleteAction); ok && action.Matches("delete", "pods") {
					require.Equal(t, failed.UID, *deletion.GetDeleteOptions().Preconditions.UID)
					require.Equal(t, failed.ResourceVersion, *deletion.GetDeleteOptions().Preconditions.ResourceVersion)
				}
			}
		})
	}
}

func TestNoneFailedPodReplacesOnlyMissingMember(t *testing.T) {
	for _, ordinal := range []int{0, 1} {
		t.Run(fmt.Sprintf("member-%d", ordinal), func(t *testing.T) {
			ms, pods := recoveryFixture(api.NoneRestartPolicy, 1)
			c, kube := recoveryController(t, ms, pods...)
			failed := pods[ordinal].DeepCopy()
			failed.Status = corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted"}
			updateRecoveryPod(t, c, failed)
			c.updatePod(pods[ordinal], failed)
			require.Eventually(t, func() bool {
				_, err := kube.CoreV1().Pods(ms.Namespace).Get(context.Background(), failed.Name, metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, time.Second, 10*time.Millisecond)
			require.NoError(t, c.podsInformer.GetIndexer().Delete(failed))
			c.deletePod(failed)
			kube.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				// The fake API does not allocate Pod UIDs.
				pod := action.(clienttesting.CreateAction).GetObject().(*corev1.Pod)
				pod.UID = types.UID(pod.Name + "-replacement")
				return false, nil, nil
			})
			require.NoError(t, c.CreatePodsByRole(context.Background(), ms.Spec.Template.Roles[0], ms, 0, 0, utils.ObjectRevision(pods[0]), utils.ObjectRoleTemplateHash(pods[0]), ""))
			replacement, err := kube.CoreV1().Pods(ms.Namespace).Get(context.Background(), failed.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.NotEqual(t, failed.UID, replacement.UID)
			require.Equal(t, utils.ObjectRevision(failed), utils.ObjectRevision(replacement))
			sibling, err := kube.CoreV1().Pods(ms.Namespace).Get(context.Background(), pods[1-ordinal].Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, pods[1-ordinal].UID, sibling.UID)
			require.Equal(t, []string{failed.Name}, recoveryDeletedPods(kube))
		})
	}
}

func TestNoneFailedPodRecoveryUsesCurrentPhase(t *testing.T) {
	for _, currentFailed := range []bool{false, true} {
		t.Run(fmt.Sprintf("current-failed=%t", currentFailed), func(t *testing.T) {
			ms, pods := recoveryFixture(api.NoneRestartPolicy, 0)
			c, kube := recoveryController(t, ms, pods...)
			observed := pods[0].DeepCopy()
			observed.Status.Conditions[0].Status = corev1.ConditionFalse
			observed.Status.ContainerStatuses = []corev1.ContainerStatus{{RestartCount: 1}}
			current := observed.DeepCopy()
			if currentFailed {
				current.Status.Phase = corev1.PodFailed
			} else {
				observed.Status.Phase = corev1.PodFailed
			}
			// Timer permissions use the current cache; deletion must also consult
			// the live API if it has advanced beyond that cache.
			updateRecoveryPod(t, c, current)
			if !currentFailed {
				require.NoError(t, c.podsInformer.GetIndexer().Update(observed))
			}
			started := time.Now()
			c.graceMap.Store(getPodGracePeriodKey(observed), started)
			c.handlePodAfterGraceTime(ms, observed, started)
			if currentFailed {
				require.Equal(t, []string{current.Name}, recoveryDeletedPods(kube))
			} else {
				require.Empty(t, recoveryDeletedPods(kube))
			}
		})
	}
}

func TestNoneFailedPodRecoveryOnColdStart(t *testing.T) {
	ms, pods := recoveryFixture(api.NoneRestartPolicy, 1)
	pods[1].Status = corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted"}
	_, kube := recoveryController(t, ms, pods...)
	require.Eventually(t, func() bool {
		_, err := kube.CoreV1().Pods(ms.Namespace).Get(context.Background(), pods[1].Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, []string{pods[1].Name}, recoveryDeletedPods(kube))
}
