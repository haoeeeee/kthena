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

package filesource

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	aiv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/networking/v1alpha1"
)

// The API server applies the CRD structural defaults (`+kubebuilder:default`
// markers on the types in pkg/apis/networking/v1alpha1) when manifests are
// written to the cluster. Without an API server the same defaults are applied
// here after decoding, before validation, so file manifests behave exactly
// like cluster manifests. Keep these functions in sync with the markers.

func defaultModelRoute(obj *aiv1alpha1.ModelRoute) {
	for _, rule := range obj.Spec.Rules {
		if rule == nil {
			continue
		}
		for _, target := range rule.TargetModels {
			if target != nil && target.Weight == nil {
				target.Weight = ptr.To(uint32(100))
			}
		}
	}
	if obj.Spec.RateLimit != nil && obj.Spec.RateLimit.Unit == "" {
		obj.Spec.RateLimit.Unit = aiv1alpha1.Second
	}
}

func defaultModelServer(obj *aiv1alpha1.ModelServer) {
	if obj.Spec.WorkloadPort.Protocol == "" {
		obj.Spec.WorkloadPort.Protocol = "http"
	}
	if obj.Spec.KVConnector != nil && obj.Spec.KVConnector.Type == "" {
		obj.Spec.KVConnector.Type = aiv1alpha1.ConnectorTypeHTTP
	}
	if obj.Spec.TrafficPolicy != nil && obj.Spec.TrafficPolicy.Retry != nil &&
		obj.Spec.TrafficPolicy.Retry.RetryInterval == nil {
		obj.Spec.TrafficPolicy.Retry.RetryInterval = &metav1.Duration{Duration: 100 * time.Millisecond}
	}
}

func defaultExternalModelProvider(obj *aiv1alpha1.ExternalModelProvider) {
	if obj.Spec.ProviderType == "" {
		obj.Spec.ProviderType = aiv1alpha1.OpenAI
	}
}
