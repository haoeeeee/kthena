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

	"github.com/stretchr/testify/require"
	clientfake "github.com/volcano-sh/kthena/client-go/clientset/versioned/fake"
	apiextfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	volcanofake "volcano.sh/apis/pkg/client/clientset/versioned/fake"
)

func TestModelServingCanceledCacheSyncDoesNotInitialize(t *testing.T) {
	for _, beforeStart := range []bool{true, false} {
		t.Run(map[bool]string{true: "before start", false: "during cache sync"}[beforeStart], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			kubeClient := kubefake.NewSimpleClientset()
			c, err := newControllerWithRevisionHistory(t, kubeClient, clientfake.NewSimpleClientset(), volcanofake.NewSimpleClientset(), apiextfake.NewSimpleClientset())
			require.NoError(t, err)
			if beforeStart {
				cancel()
			} else {
				kubeClient.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
					cancel()
					return true, nil, context.Canceled
				})
			}
			c.Run(ctx, 1)
			require.False(t, c.initialSync.Load())
			require.True(t, c.workqueue.ShuttingDown())
		})
	}
}
