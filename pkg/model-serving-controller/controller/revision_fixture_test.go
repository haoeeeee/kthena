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
	"testing"

	"github.com/stretchr/testify/require"
	clientset "github.com/volcano-sh/kthena/client-go/clientset/versioned"
	revisiontest "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils/test"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/kubernetes"
	volcano "volcano.sh/apis/pkg/client/clientset/versioned"
)

// Existing synchronous unit fixtures do not run their informer loops. Read
// their fake tracker through a lister; real informer tests keep the real lister.
func newControllerWithRevisionHistory(t *testing.T, kube kubernetes.Interface, ms clientset.Interface, vc volcano.Interface, ext apiextclient.Interface) (*ModelServingController, error) {
	t.Helper()
	c, err := NewModelServingController(kube, ms, vc, ext)
	if err != nil {
		return nil, err
	}
	require.NotNil(t, c.controllerRevisionsInformer)
	c.controllerRevisionLister = revisiontest.ControllerRevisions(kube)
	return c, nil
}
