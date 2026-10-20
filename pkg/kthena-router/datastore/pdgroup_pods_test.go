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

package datastore

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	aiv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/networking/v1alpha1"
)

func TestPDGroup(t *testing.T) {
	store := New()

	// Create a ModelServer with PDGroup configuration
	modelServer := &aiv1alpha1.ModelServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-model",
			Namespace: "default",
		},
		Spec: aiv1alpha1.ModelServerSpec{
			WorkloadSelector: &aiv1alpha1.WorkloadSelector{
				PDGroup: &aiv1alpha1.PDGroup{
					GroupKey: "pd-group",
					DecodeLabels: map[string]string{
						"role": "decode",
					},
					PrefillLabels: map[string]string{
						"role": "prefill",
					},
				},
			},
		},
	}

	modelServerName := types.NamespacedName{
		Namespace: "default",
		Name:      "test-model",
	}

	// Add the ModelServer to store
	err := store.AddOrUpdateModelServer(modelServer, nil)
	if err != nil {
		t.Fatalf("Failed to add model server: %v", err)
	}

	// Create test pods
	decodePod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "decode-pod-1",
			Namespace: "default",
			Labels: map[string]string{
				"pd-group": "group-a",
				"role":     "decode",
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}

	decodePod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "decode-pod-2",
			Namespace: "default",
			Labels: map[string]string{
				"pd-group": "group-b",
				"role":     "decode",
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.2",
		},
	}

	prefillPod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "prefill-pod-1",
			Namespace: "default",
			Labels: map[string]string{
				"pd-group": "group-a",
				"role":     "prefill",
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.3",
		},
	}

	prefillPod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "prefill-pod-2",
			Namespace: "default",
			Labels: map[string]string{
				"pd-group": "group-b",
				"role":     "prefill",
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.4",
		},
	}

	// Add pods to store
	err = store.AddOrUpdatePod(decodePod1, []*aiv1alpha1.ModelServer{modelServer})
	if err != nil {
		t.Fatalf("Failed to add decode pod 1: %v", err)
	}

	err = store.AddOrUpdatePod(decodePod2, []*aiv1alpha1.ModelServer{modelServer})
	if err != nil {
		t.Fatalf("Failed to add decode pod 2: %v", err)
	}

	err = store.AddOrUpdatePod(prefillPod1, []*aiv1alpha1.ModelServer{modelServer})
	if err != nil {
		t.Fatalf("Failed to add prefill pod 1: %v", err)
	}

	err = store.AddOrUpdatePod(prefillPod2, []*aiv1alpha1.ModelServer{modelServer})
	if err != nil {
		t.Fatalf("Failed to add prefill pod 2: %v", err)
	}

	// Test GetDecodePods
	decodePods, err := store.GetDecodePods(modelServerName)
	if err != nil {
		t.Fatalf("Failed to get decode pods: %v", err)
	}

	if len(decodePods) != 2 {
		t.Errorf("Expected 2 decode pods, got %d", len(decodePods))
	}

	// Test GetPrefillPods
	prefillPods, err := store.GetPrefillPods(modelServerName)
	if err != nil {
		t.Fatalf("Failed to get prefill pods: %v", err)
	}

	if len(prefillPods) != 2 {
		t.Errorf("Expected 2 prefill pods, got %d", len(prefillPods))
	}

	// Test GetPrefillPodsForDecodeGroup
	decodePod1Name := types.NamespacedName{
		Namespace: "default",
		Name:      "decode-pod-1",
	}

	matchingPrefillPods, err := store.GetPrefillPodsForDecodeGroup(modelServerName, decodePod1Name)
	if err != nil {
		t.Fatalf("Failed to get prefill pods for decode group: %v", err)
	}

	if len(matchingPrefillPods) != 1 {
		t.Errorf("Expected 1 prefill pod for decode group, got %d", len(matchingPrefillPods))
	}

	if len(matchingPrefillPods) > 0 && matchingPrefillPods[0].Pod.Name != prefillPod1.Name {
		t.Errorf("Expected prefill-pod-1, got %s", matchingPrefillPods[0].Pod.Name)
	}

	// Test with decode-pod-2 (group-b)
	decodePod2Name := types.NamespacedName{
		Namespace: "default",
		Name:      "decode-pod-2",
	}

	matchingPrefillPods2, err := store.GetPrefillPodsForDecodeGroup(modelServerName, decodePod2Name)
	if err != nil {
		t.Fatalf("Failed to get prefill pods for decode group: %v", err)
	}

	if len(matchingPrefillPods2) != 1 {
		t.Errorf("Expected 1 prefill pod for decode group, got %d", len(matchingPrefillPods2))
	}

	if len(matchingPrefillPods2) > 0 && matchingPrefillPods2[0].Pod.Name != "prefill-pod-2" {
		t.Errorf("Expected prefill-pod-2, got %s", matchingPrefillPods2[0].Pod.Name)
	}
}

func TestPDGroupPodRemoval(t *testing.T) {
	store := New()

	// Create a ModelServer with PDGroup configuration
	modelServer := &aiv1alpha1.ModelServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-model",
			Namespace: "default",
		},
		Spec: aiv1alpha1.ModelServerSpec{
			WorkloadSelector: &aiv1alpha1.WorkloadSelector{
				PDGroup: &aiv1alpha1.PDGroup{
					GroupKey: "pd-group",
					DecodeLabels: map[string]string{
						"role": "decode",
					},
					PrefillLabels: map[string]string{
						"role": "prefill",
					},
				},
			},
		},
	}

	modelServerName := types.NamespacedName{
		Namespace: "default",
		Name:      "test-model",
	}

	// Add the ModelServer to store
	err := store.AddOrUpdateModelServer(modelServer, nil)
	if err != nil {
		t.Fatalf("Failed to add model server: %v", err)
	}

	// Create and add a decode pod
	decodePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "decode-pod",
			Namespace: "default",
			Labels: map[string]string{
				"pd-group": "group-a",
				"role":     "decode",
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}

	err = store.AddOrUpdatePod(decodePod, []*aiv1alpha1.ModelServer{modelServer})
	if err != nil {
		t.Fatalf("Failed to add decode pod: %v", err)
	}

	// Verify pod is categorized
	decodePods, err := store.GetDecodePods(modelServerName)
	if err != nil {
		t.Fatalf("Failed to get decode pods: %v", err)
	}

	if len(decodePods) != 1 {
		t.Errorf("Expected 1 decode pod, got %d", len(decodePods))
	}

	// Remove the pod
	podName := types.NamespacedName{
		Namespace: "default",
		Name:      "decode-pod",
	}

	err = store.DeletePod(podName)
	if err != nil {
		t.Fatalf("Failed to delete pod: %v", err)
	}

	// Verify pod is removed from categorization
	decodePods, err = store.GetDecodePods(modelServerName)
	if err != nil {
		t.Fatalf("Failed to get decode pods after deletion: %v", err)
	}

	if len(decodePods) != 0 {
		t.Errorf("Expected 0 decode pods after deletion, got %d", len(decodePods))
	}
}

// Label updates must replace a Pod's PD classification while it still matches the ModelServer.
func TestPDGroupPodLabelUpdate(t *testing.T) {
	// The changing Pod starts in group-a with oldRole; group and role are its new labels.
	// Empty values remove the corresponding label. decode and prefill count only the
	// changing Pod, excluding the unchanged prefill peer added in every case.
	tests := []struct {
		name    string
		oldRole string
		group   string
		role    string
		decode  int
		prefill int
	}{
		{"move decode group", "decode", "group-b", "decode", 1, 0},
		{"move prefill group", "prefill", "group-b", "prefill", 0, 1},
		{"prefill to decode", "prefill", "group-a", "decode", 1, 0},
		{"decode to prefill", "decode", "group-a", "prefill", 0, 1},
		{"remove group", "decode", "", "decode", 0, 0},
		{"remove role", "prefill", "group-a", "", 0, 0},
		{"change group and remove role", "decode", "group-b", "", 0, 0},
		{"unchanged labels", "decode", "group-a", "decode", 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New().(*store)
			ms := newTestModelServerWithPDGroup("test-model", "default")
			msName := types.NamespacedName{Namespace: ms.Namespace, Name: ms.Name}
			require.NoError(t, s.AddOrUpdateModelServer(ms, nil))
			pod := newTestPod("changing", "default", map[string]string{
				"app": ms.Name, "pd-group": "group-a", "role": tt.oldRole,
			})
			podName := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
			// Keep a peer in the original group to check that reclassification preserves other Pods.
			peer := newTestPod("peer", "default", map[string]string{
				"app": ms.Name, "pd-group": "group-a", "role": "prefill",
			})
			require.NoError(t, s.AddOrUpdatePod(pod, []*aiv1alpha1.ModelServer{ms}))
			require.NoError(t, s.AddOrUpdatePod(peer, []*aiv1alpha1.ModelServer{ms}))
			oldInfo := s.GetPodInfo(podName)

			// Update a copy so PodInfo keeps its previous labels until the store handles the event.
			updated := pod.DeepCopy()
			updated.Labels["pd-group"] = tt.group
			updated.Labels["role"] = tt.role
			if tt.group == "" {
				delete(updated.Labels, "pd-group")
			}
			if tt.role == "" {
				delete(updated.Labels, "role")
			}
			require.NoError(t, s.AddOrUpdatePod(updated, []*aiv1alpha1.ModelServer{ms}))
			assert.Same(t, oldInfo, s.GetPodInfo(podName), "updates must preserve PodInfo")
			decode, err := s.GetDecodePods(msName)
			require.NoError(t, err)
			assert.Len(t, decode, tt.decode)
			prefill, err := s.GetPrefillPods(msName)
			require.NoError(t, err)
			assert.Len(t, prefill, tt.prefill+1, "the unrelated prefill peer must remain")
			matching, err := s.GetPrefillPodsForDecodeGroup(msName, podName)
			require.NoError(t, err)
			if tt.decode == 1 && tt.group == "group-a" {
				require.Len(t, matching, 1)
				assert.Equal(t, peer.Name, matching[0].GetPodNamespacedName().Name)
			} else {
				assert.Empty(t, matching, "only a classified decode Pod can select its group's prefill peer")
			}
			value, ok := s.modelServer.Load(msName)
			require.True(t, ok)
			msInfo := value.(*modelServer)
			assert.Len(t, msInfo.getPods(), 2, "the ModelServer selector still matches both pods")

			// Removing both Pods must also clean up every group they occupied.
			require.NoError(t, s.DeletePod(podName))
			require.NoError(t, s.DeletePod(types.NamespacedName{Namespace: peer.Namespace, Name: peer.Name}))
			assert.Empty(t, msInfo.pdGroups, "deleting the pods must leave no stale group")
		})
	}
}

func TestPDGroupLookupDuringPodLabelUpdate(t *testing.T) {
	s := New().(*store)
	ms := newTestModelServerWithPDGroup("test-model", "default")
	second := ms.DeepCopy()
	second.Name = "second-model"
	servers := []*aiv1alpha1.ModelServer{ms, second}
	for _, server := range servers {
		require.NoError(t, s.AddOrUpdateModelServer(server, nil))
	}
	msName := types.NamespacedName{Namespace: ms.Namespace, Name: ms.Name}
	pod := newTestPod("decode", "default", map[string]string{
		"app": ms.Name, "pd-group": "group-a", "role": "decode",
	})
	podName := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
	require.NoError(t, s.AddOrUpdatePod(pod, servers))
	for _, group := range []string{"group-a", "group-b"} {
		prefill := newTestPod("prefill-"+group, "default", map[string]string{
			"app": ms.Name, "pd-group": group, "role": "prefill",
		})
		require.NoError(t, s.AddOrUpdatePod(prefill, servers))
	}
	value, ok := s.modelServer.Load(msName)
	require.True(t, ok)
	msInfo := value.(*modelServer)
	value, ok = s.modelServer.Load(types.NamespacedName{Namespace: second.Namespace, Name: second.Name})
	require.True(t, ok)
	secondInfo := value.(*modelServer)

	// Both ModelServers match this Pod. Hold a reader lock on the second one so
	// AddOrUpdatePod pauses after updating the first index, before updating PodInfo.
	secondInfo.mutex.RLock()
	release := sync.OnceFunc(secondInfo.mutex.RUnlock)
	done := make(chan error, 1)
	defer func() {
		release()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Error("Pod update did not complete after releasing the second ModelServer")
		}
	}()
	updated := pod.DeepCopy()
	updated.Labels["pd-group"] = "group-b"
	go func() { done <- s.AddOrUpdatePod(updated, servers) }()
	require.Eventually(t, func() bool {
		msInfo.mutex.RLock()
		defer msInfo.mutex.RUnlock()
		return len(msInfo.pdGroups["group-b"].GetDecodePods()) == 1
	}, time.Second, time.Millisecond)

	decode, err := s.GetDecodePods(msName)
	require.NoError(t, err)
	require.NotEmpty(t, decode)
	require.Equal(t, "group-a", decode[0].GetPodLabels()["pd-group"], "PodInfo update is still blocked")

	// The index already places decode in B; looking up its peer must use that
	// same classification, even though its PodInfo still has the old A label.
	prefill, err := s.GetPrefillPodsForDecodeGroup(msName, podName)
	require.NoError(t, err)
	require.Len(t, prefill, 1)
	assert.Equal(t, "prefill-group-b", prefill[0].GetPodNamespacedName().Name)
}

func TestPDGroupAppendThenLabelUpdateCleansAllGroups(t *testing.T) {
	for _, role := range []string{"decode", "prefill"} {
		t.Run(role, func(t *testing.T) {
			s := New().(*store)
			ms := newTestModelServerWithPDGroup("test-model", "default")
			second := ms.DeepCopy()
			second.Name = "second-model"
			servers := []*aiv1alpha1.ModelServer{ms, second}
			msName := types.NamespacedName{Namespace: ms.Namespace, Name: ms.Name}
			secondName := types.NamespacedName{Namespace: second.Namespace, Name: second.Name}
			for _, server := range servers {
				require.NoError(t, s.AddOrUpdateModelServer(server, nil))
			}
			pod := newTestPod("moving", "default", map[string]string{
				"app": ms.Name, "role": role, "pd-group": "group-a",
			})
			podName := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
			require.NoError(t, s.AddOrUpdatePod(pod, []*aiv1alpha1.ModelServer{ms}))
			latest := pod.DeepCopy()
			latest.Labels["pd-group"] = "group-b"
			// Append classifies the new binding using B without replacing PodInfo's A labels.
			require.NoError(t, s.AppendModelServerToPod(latest, []*aiv1alpha1.ModelServer{second}))
			require.Equal(t, "group-a", s.GetPodInfo(podName).GetPodLabels()["pd-group"])
			updated := latest.DeepCopy()
			updated.Labels["pd-group"] = "group-c"
			require.NoError(t, s.AddOrUpdatePod(updated, servers))
			for _, name := range []types.NamespacedName{msName, secondName} {
				value, ok := s.modelServer.Load(name)
				require.True(t, ok)
				msInfo := value.(*modelServer)
				assert.Len(t, msInfo.pdGroups, 1, "only the current group may retain the Pod")
				assert.Contains(t, msInfo.pdGroups, "group-c")
			}
			require.NoError(t, s.DeletePod(podName))
			for _, name := range []types.NamespacedName{msName, secondName} {
				value, ok := s.modelServer.Load(name)
				require.True(t, ok)
				assert.Empty(t, value.(*modelServer).pdGroups)
				assert.Empty(t, value.(*modelServer).decodePodGroups)
			}
		})
	}
}

// A status-only update must not hide a healthy Pod from concurrent PD scheduling.
func TestPDGroupConcurrentPodUpdate(t *testing.T) {
	for _, role := range []string{"decode", "prefill"} {
		t.Run(role, func(t *testing.T) {
			s := New()
			ms := newTestModelServerWithPDGroup("test-model", "default")
			msName := types.NamespacedName{Namespace: ms.Namespace, Name: ms.Name}
			require.NoError(t, s.AddOrUpdateModelServer(ms, nil))
			var updated *corev1.Pod
			for _, podRole := range []string{"decode", "prefill"} {
				pod := newTestPod(podRole, "default", map[string]string{
					"app": ms.Name, "pd-group": "group-a", "role": podRole,
				})
				require.NoError(t, s.AddOrUpdatePod(pod, []*aiv1alpha1.ModelServer{ms}))
				if podRole == role {
					updated = pod
				}
			}
			start := make(chan struct{})
			errs := make(chan error, 1)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 10000; i++ {
					pod := updated.DeepCopy()
					pod.ResourceVersion = strconv.Itoa(i + 1)
					if err := s.AddOrUpdatePod(pod, []*aiv1alpha1.ModelServer{ms}); err != nil {
						errs <- err
						return
					}
				}
			}()
			defer wg.Wait()
			close(start)
			for i := 0; i < 10000; i++ {
				decode, err := s.GetDecodePods(msName)
				require.NoError(t, err)
				require.Len(t, decode, 1, "a status-only update must preserve the decode candidate")
				prefill, err := s.GetPrefillPodsForDecodeGroup(msName, decode[0].GetPodNamespacedName())
				require.NoError(t, err)
				require.Len(t, prefill, 1, "a status-only update must preserve the prefill candidate")
			}
			wg.Wait()
			select {
			case err := <-errs:
				require.NoError(t, err)
			default:
			}
		})
	}
}

func TestPDGroupModelServerConfigUpdate(t *testing.T) {
	tests := []struct {
		name              string
		initiallyDisabled bool
		update            func(*aiv1alpha1.PDGroup) *aiv1alpha1.PDGroup
		decode            []string
		prefill           []string
		peer              []string
	}{
		{
			name: "change group key",
			update: func(pd *aiv1alpha1.PDGroup) *aiv1alpha1.PDGroup {
				pd.GroupKey = "next-group"
				return pd
			},
			decode: []string{"decode"}, prefill: []string{"prefill-a", "prefill-b"}, peer: []string{"prefill-b"},
		},
		{
			name: "change decode selector",
			update: func(pd *aiv1alpha1.PDGroup) *aiv1alpha1.PDGroup {
				pd.DecodeLabels = map[string]string{"role": "other-decode"}
				return pd
			},
			prefill: []string{"prefill-a", "prefill-b"},
		},
		{
			name: "change prefill selector",
			update: func(pd *aiv1alpha1.PDGroup) *aiv1alpha1.PDGroup {
				pd.PrefillLabels = map[string]string{"role": "other-prefill"}
				return pd
			},
			decode: []string{"decode"},
		},
		{
			name:   "disable PD",
			update: func(_ *aiv1alpha1.PDGroup) *aiv1alpha1.PDGroup { return nil },
		},
		{
			name: "enable PD", initiallyDisabled: true,
			update: func(pd *aiv1alpha1.PDGroup) *aiv1alpha1.PDGroup { return pd },
			decode: []string{"decode"}, prefill: []string{"prefill-a", "prefill-b"}, peer: []string{"prefill-a"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New().(*store)
			ms := newTestModelServerWithPDGroup("test-model", "default")
			updated := ms.DeepCopy()
			updated.Spec.WorkloadSelector.PDGroup = tt.update(updated.Spec.WorkloadSelector.PDGroup)
			if tt.initiallyDisabled {
				ms.Spec.WorkloadSelector.PDGroup = nil
			}
			msName := types.NamespacedName{Namespace: ms.Namespace, Name: ms.Name}
			require.NoError(t, s.AddOrUpdateModelServer(ms, nil))
			var pods []*corev1.Pod
			var infos []*PodInfo
			// Switching the group key must move decode's peer from prefill-a to
			// prefill-b, without any Pod label updates or new PodInfo objects.
			for _, p := range []struct{ name, role, group, nextGroup string }{
				{"decode", "decode", "group-a", "group-b"},
				{"prefill-a", "prefill", "group-a", "group-c"},
				{"prefill-b", "prefill", "group-b", "group-b"},
			} {
				pod := newTestPod(p.name, "default", map[string]string{
					"app": ms.Name, "role": p.role, "pd-group": p.group, "next-group": p.nextGroup,
				})
				require.NoError(t, s.AddOrUpdatePod(pod, []*aiv1alpha1.ModelServer{ms}))
				pods = append(pods, pod)
				infos = append(infos, s.GetPodInfo(types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}))
			}
			require.NoError(t, s.AddOrUpdateModelServer(updated, nil))
			names := func(pods []*PodInfo) []string {
				var result []string
				for _, pod := range pods {
					result = append(result, pod.GetPodNamespacedName().Name)
				}
				return result
			}
			decode, err := s.GetDecodePods(msName)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.decode, names(decode))
			prefill, err := s.GetPrefillPods(msName)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.prefill, names(prefill))
			peer, err := s.GetPrefillPodsForDecodeGroup(msName, infos[0].GetPodNamespacedName())
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.peer, names(peer))
			for i, pod := range pods {
				assert.Same(t, infos[i], s.GetPodInfo(infos[i].GetPodNamespacedName()))
				// A later Pod event and deletion must not leave buckets indexed
				// with the previous configuration's group key.
				require.NoError(t, s.AddOrUpdatePod(pod.DeepCopy(), []*aiv1alpha1.ModelServer{updated}))
				require.NoError(t, s.DeletePod(infos[i].GetPodNamespacedName()))
			}
			value, ok := s.modelServer.Load(msName)
			require.True(t, ok)
			assert.Empty(t, value.(*modelServer).pdGroups)
			assert.Empty(t, value.(*modelServer).decodePodGroups)
		})
	}
}

// Both configurations have a valid pair. Rebuilding must not expose an empty index.
func TestPDGroupConcurrentModelServerConfigUpdate(t *testing.T) {
	s := New()
	ms := newTestModelServerWithPDGroup("test-model", "default")
	msName := types.NamespacedName{Namespace: ms.Namespace, Name: ms.Name}
	require.NoError(t, s.AddOrUpdateModelServer(ms, nil))
	for _, role := range []string{"decode", "prefill"} {
		pod := newTestPod(role, "default", map[string]string{
			"app": ms.Name, "role": role, "pd-group": "group-a", "next-group": "group-b",
		})
		require.NoError(t, s.AddOrUpdatePod(pod, []*aiv1alpha1.ModelServer{ms}))
	}
	updated := ms.DeepCopy()
	updated.Spec.WorkloadSelector.PDGroup.GroupKey = "next-group"
	configs := []*aiv1alpha1.ModelServer{ms, updated}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 1000; i++ {
			if err := s.AddOrUpdateModelServer(configs[i%2], nil); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	defer func() { require.NoError(t, <-done) }()
	for i := 0; i < 1000; i++ {
		decode, err := s.GetDecodePods(msName)
		require.NoError(t, err)
		require.Len(t, decode, 1)
		prefill, err := s.GetPrefillPodsForDecodeGroup(msName, decode[0].GetPodNamespacedName())
		require.NoError(t, err)
		require.Len(t, prefill, 1, "both complete configurations have a matching prefill")
	}
}

// Pod events can be handled before the event of the ModelServer that selects
// them. The pods must end up in their PD group either way.
func TestPDGroupCategorizedRegardlessOfEventOrder(t *testing.T) {
	tests := []struct {
		name     string
		podFirst bool
	}{
		{name: "model server first", podFirst: false},
		{name: "pods first", podFirst: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newStore(&fakePodRuntimeInspector{})
			ms := newTestModelServerWithPDGroup("test-model", "default")
			msName := types.NamespacedName{Namespace: ms.Namespace, Name: ms.Name}
			decode := newTestPod("decode-0", "default", map[string]string{
				"app": ms.Name, "role": "decode", "pd-group": "group-a",
			})
			prefill := newTestPod("prefill-0", "default", map[string]string{
				"app": ms.Name, "role": "prefill", "pd-group": "group-a",
			})
			pods := []*corev1.Pod{decode, prefill}
			podNames := sets.New(
				types.NamespacedName{Namespace: "default", Name: "decode-0"},
				types.NamespacedName{Namespace: "default", Name: "prefill-0"},
			)
			servers := []*aiv1alpha1.ModelServer{ms}

			if tt.podFirst {
				// syncPodHandler runs while the ModelServer is in the lister
				// but not yet in the store.
				for _, p := range pods {
					require.NoError(t, s.AddOrUpdatePod(p, servers))
				}
				// syncModelServerHandler: pods already in the store get appended.
				require.NoError(t, s.AddOrUpdateModelServer(ms, podNames))
				for _, p := range pods {
					require.NoError(t, s.AppendModelServerToPod(p, servers))
				}
			} else {
				require.NoError(t, s.AddOrUpdateModelServer(ms, podNames))
				for _, p := range pods {
					require.NoError(t, s.AddOrUpdatePod(p, servers))
				}
			}
			decodePods, err := s.GetDecodePods(msName)
			require.NoError(t, err)
			require.Len(t, decodePods, 1)
			assert.Equal(t, "decode-0", decodePods[0].GetPodNamespacedName().Name)

			prefillPods, err := s.GetPrefillPodsForDecodeGroup(msName,
				types.NamespacedName{Namespace: "default", Name: "decode-0"})
			require.NoError(t, err)
			require.Len(t, prefillPods, 1)
			assert.Equal(t, "prefill-0", prefillPods[0].GetPodNamespacedName().Name)
		})
	}
}
