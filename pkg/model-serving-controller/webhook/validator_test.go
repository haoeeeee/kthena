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

package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/plugins/ranktable"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

func TestValidPodNameLength(t *testing.T) {
	replicas := int32(3)
	type args struct {
		ms *workloadv1alpha1.ModelServing
	}
	tests := []struct {
		name string
		args args
		want field.ErrorList
	}{
		{
			name: "normal name length",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					ObjectMeta: v1.ObjectMeta{
						Name: "valid-name",
					},
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "role1",
									Replicas:       &replicas,
									WorkerReplicas: 2,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "name length exceeds limit",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					ObjectMeta: v1.ObjectMeta{
						Name: "this-is-a-very-long-name-that-exceeds-the-allowed-length-for-generated-name",
					},
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "role1",
									Replicas:       &replicas,
									WorkerReplicas: 2,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("metadata").Child("name"),
					"this-is-a-very-long-name-that-exceeds-the-allowed-length-for-generated-name",
					"invalid name: must be no more than 63 characters"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validGeneratedNameLength(tt.args.ms)
			if got != nil {
				assert.EqualValues(t, tt.want[0], got[0])
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestValidateModelServingMissingReplicasDoesNotPanic(t *testing.T) {
	validator := NewModelServingValidator(nil)
	ms := &workloadv1alpha1.ModelServing{
		ObjectMeta: v1.ObjectMeta{
			Name: "valid-name",
		},
		Spec: workloadv1alpha1.ModelServingSpec{
			Template: workloadv1alpha1.ServingGroup{
				Roles: []workloadv1alpha1.Role{
					{
						Name: "role1",
					},
				},
			},
		},
	}

	var allowed bool
	var reason string
	assert.NotPanics(t, func() {
		allowed, reason = validator.validateModelServing(context.Background(), ms)
	})
	assert.False(t, allowed)
	assert.Contains(t, reason, "spec.replicas")
	assert.Contains(t, reason, "spec.template.roles[0].replicas")
}

func TestModelServingValidatorNetworkTopologyIsImmutableOnUpdate(t *testing.T) {
	newModelServing := func() *workloadv1alpha1.ModelServing {
		return &workloadv1alpha1.ModelServing{
			TypeMeta: v1.TypeMeta{
				APIVersion: workloadv1alpha1.SchemeGroupVersion.String(),
				Kind:       "ModelServing",
			},
			ObjectMeta: v1.ObjectMeta{Name: "immutable-topology"},
			Spec: workloadv1alpha1.ModelServingSpec{
				Replicas:      ptr.To[int32](1),
				SchedulerName: "volcano",
				Template: workloadv1alpha1.ServingGroup{
					NetworkTopology: &workloadv1alpha1.NetworkTopology{
						GroupPolicy: &schedulingv1beta1.NetworkTopologySpec{
							Mode: schedulingv1beta1.HardNetworkTopologyMode,
						},
						ServingGroupAntiAffinity: &workloadv1alpha1.ServingGroupAntiAffinity{
							Required: []workloadv1alpha1.ServingGroupAffinityTerm{{TopologyTierName: "rack"}},
						},
					},
					Roles: []workloadv1alpha1.Role{{
						Name:     "predictor",
						Replicas: ptr.To[int32](1),
						EntryTemplate: workloadv1alpha1.PodTemplateSpec{Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "predictor", Image: "nginx:latest"}},
						}},
					}},
				},
			},
		}
	}

	tests := []struct {
		name        string
		mutate      func(oldModelServing, modelServing *workloadv1alpha1.ModelServing)
		wantAllowed bool
	}{
		{
			name: "allows ModelServing and Role scaling with unchanged topology",
			mutate: func(_, modelServing *workloadv1alpha1.ModelServing) {
				modelServing.Spec.Replicas = ptr.To[int32](2)
				modelServing.Spec.Template.Roles[0].Replicas = ptr.To[int32](2)
			},
			wantAllowed: true,
		},
		{
			name: "rejects adding network topology",
			mutate: func(oldModelServing, _ *workloadv1alpha1.ModelServing) {
				oldModelServing.Spec.Template.NetworkTopology = nil
			},
		},
		{
			name: "rejects removing network topology",
			mutate: func(_, modelServing *workloadv1alpha1.ModelServing) {
				modelServing.Spec.Template.NetworkTopology = nil
			},
		},
		{
			name: "rejects changing aggregation policy",
			mutate: func(_, modelServing *workloadv1alpha1.ModelServing) {
				modelServing.Spec.Template.NetworkTopology.GroupPolicy.Mode = schedulingv1beta1.SoftNetworkTopologyMode
			},
		},
		{
			name: "rejects changing affinity policy",
			mutate: func(_, modelServing *workloadv1alpha1.ModelServing) {
				modelServing.Spec.Template.NetworkTopology.ServingGroupAntiAffinity.Required[0].TopologyTierName = "zone"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldModelServing := newModelServing()
			modelServing := oldModelServing.DeepCopy()
			tt.mutate(oldModelServing, modelServing)

			oldRaw, err := json.Marshal(oldModelServing)
			assert.NoError(t, err)
			newRaw, err := json.Marshal(modelServing)
			assert.NoError(t, err)

			admissionReview := admissionv1.AdmissionReview{
				TypeMeta: v1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
				Request: &admissionv1.AdmissionRequest{
					UID:       types.UID("immutable-topology-test"),
					Operation: admissionv1.Update,
					Object:    runtime.RawExtension{Raw: newRaw},
					OldObject: runtime.RawExtension{Raw: oldRaw},
				},
			}
			body, err := json.Marshal(admissionReview)
			assert.NoError(t, err)

			request := httptest.NewRequest(http.MethodPost, "/validate-workload-ai-v1alpha1-modelserving", bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			NewModelServingValidator(nil).Handle(response, request)

			assert.Equal(t, http.StatusOK, response.Code)
			var responseReview admissionv1.AdmissionReview
			assert.NoError(t, json.Unmarshal(response.Body.Bytes(), &responseReview))
			if assert.NotNil(t, responseReview.Response) {
				assert.Equal(t, tt.wantAllowed, responseReview.Response.Allowed)
				if !tt.wantAllowed && assert.NotNil(t, responseReview.Response.Result) {
					assert.Contains(t, responseReview.Response.Result.Message, "spec.template.networkTopology")
					assert.Contains(t, responseReview.Response.Result.Message, "field is immutable")
				}
			}
		})
	}
}

func TestModelServingValidatorRoleNamesAreImmutableOnUpdate(t *testing.T) {
	newModelServing := func(names ...string) *workloadv1alpha1.ModelServing {
		roles := make([]workloadv1alpha1.Role, 0, len(names))
		for _, name := range names {
			roles = append(roles, workloadv1alpha1.Role{
				Name:     name,
				Replicas: ptr.To[int32](1),
				EntryTemplate: workloadv1alpha1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: name, Image: "nginx:latest"}},
				}},
			})
		}
		return &workloadv1alpha1.ModelServing{
			ObjectMeta: v1.ObjectMeta{Name: "immutable-role-names"},
			Spec: workloadv1alpha1.ModelServingSpec{
				Replicas: ptr.To[int32](1),
				Template: workloadv1alpha1.ServingGroup{Roles: roles},
			},
		}
	}

	tests := []struct {
		name        string
		oldRoles    []string
		newRoles    []string
		wantAllowed bool
	}{
		{name: "unchanged role names", oldRoles: []string{"prefill", "decode"}, newRoles: []string{"prefill", "decode"}, wantAllowed: true},
		{name: "reordered role names", oldRoles: []string{"prefill", "decode"}, newRoles: []string{"decode", "prefill"}, wantAllowed: true},
		{name: "renamed role", oldRoles: []string{"prefill", "decode"}, newRoles: []string{"embedding", "decode"}},
		{name: "added role", oldRoles: []string{"prefill", "decode"}, newRoles: []string{"prefill", "decode", "embedding"}},
		{name: "removed role", oldRoles: []string{"prefill", "decode"}, newRoles: []string{"prefill"}},
	}

	validator := NewModelServingValidator(nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowed, reason := validator.validateModelServingUpdate(
				context.Background(),
				newModelServing(tt.oldRoles...),
				newModelServing(tt.newRoles...),
			)
			assert.Equal(t, tt.wantAllowed, allowed)
			if tt.wantAllowed {
				assert.Empty(t, reason)
			} else {
				assert.Contains(t, reason, "role names are immutable")
			}
		})
	}
}

func TestValidGeneratedNameLengthUsesReplicaDefaultsForMissingValues(t *testing.T) {
	replicas := int32(1)
	longName := "this-is-a-very-long-name-that-exceeds-the-allowed-length-for-generated-name"
	tests := []struct {
		name    string
		ms      *workloadv1alpha1.ModelServing
		wantErr bool
	}{
		{
			name: "missing top-level replicas",
			ms: &workloadv1alpha1.ModelServing{
				ObjectMeta: v1.ObjectMeta{Name: "valid-name"},
				Spec: workloadv1alpha1.ModelServingSpec{
					Template: workloadv1alpha1.ServingGroup{
						Roles: []workloadv1alpha1.Role{
							{Name: "role1", Replicas: &replicas},
						},
					},
				},
			},
		},
		{
			name: "missing role replicas",
			ms: &workloadv1alpha1.ModelServing{
				ObjectMeta: v1.ObjectMeta{Name: "valid-name"},
				Spec: workloadv1alpha1.ModelServingSpec{
					Replicas: &replicas,
					Template: workloadv1alpha1.ServingGroup{
						Roles: []workloadv1alpha1.Role{
							{Name: "role1"},
						},
					},
				},
			},
		},
		{
			name: "missing top-level replicas still validates generated name length",
			ms: &workloadv1alpha1.ModelServing{
				ObjectMeta: v1.ObjectMeta{Name: longName},
				Spec: workloadv1alpha1.ModelServingSpec{
					Template: workloadv1alpha1.ServingGroup{
						Roles: []workloadv1alpha1.Role{
							{Name: "role1", Replicas: &replicas},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "missing role replicas still validates generated name length",
			ms: &workloadv1alpha1.ModelServing{
				ObjectMeta: v1.ObjectMeta{Name: longName},
				Spec: workloadv1alpha1.ModelServingSpec{
					Replicas: &replicas,
					Template: workloadv1alpha1.ServingGroup{
						Roles: []workloadv1alpha1.Role{
							{Name: "role1"},
						},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got field.ErrorList
			assert.NotPanics(t, func() {
				got = validGeneratedNameLength(tt.ms)
			})
			if tt.wantErr {
				assert.NotEmpty(t, got)
				return
			}
			assert.Empty(t, got)
		})
	}
}

func TestValidateTopologyAffinity(t *testing.T) {
	newModelServing := func() *workloadv1alpha1.ModelServing {
		return &workloadv1alpha1.ModelServing{
			Spec: workloadv1alpha1.ModelServingSpec{
				SchedulerName: "volcano",
				Template: workloadv1alpha1.ServingGroup{
					Roles: []workloadv1alpha1.Role{
						{Name: "prefill", Replicas: ptr.To[int32](2)},
						{Name: "decode", Replicas: ptr.To[int32](2)},
					},
					NetworkTopology: &workloadv1alpha1.NetworkTopology{
						ServingGroupAntiAffinity: &workloadv1alpha1.ServingGroupAntiAffinity{
							Required: []workloadv1alpha1.ServingGroupAffinityTerm{{TopologyTierName: "communication-domain"}},
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name         string
		mutate       func(*workloadv1alpha1.ModelServing)
		wantContains []string
	}{
		{
			name: "valid required and preferred rules",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				weight := int32(60)
				nodeTier := int32(0)
				ms.Spec.Template.NetworkTopology.ServingGroupAntiAffinity.Preferred = []workloadv1alpha1.ServingGroupAffinityTerm{
					{Weight: &weight, TopologyTier: &nodeTier},
				}
				ms.Spec.Template.NetworkTopology.RoleAffinity = &workloadv1alpha1.RoleAffinity{
					Required: []workloadv1alpha1.RoleAffinityTerm{
						{Roles: []string{"prefill", "decode"}, TopologyTierName: "rack"},
					},
				}
				ms.Spec.Template.NetworkTopology.RoleAntiAffinity = &workloadv1alpha1.RoleAntiAffinity{
					Required: []workloadv1alpha1.RoleAffinityTerm{
						{Roles: []string{"prefill"}, TopologyTierName: "node"},
						{Roles: []string{"decode"}, TopologyTierName: "node"},
					},
				}
			},
		},
		{
			name: "network topology without affinity rules preserves existing validation behavior",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.SchedulerName = "default-scheduler"
				ms.Spec.Template.NetworkTopology = &workloadv1alpha1.NetworkTopology{}
			},
		},
		{
			name: "requires volcano scheduler",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.SchedulerName = "default-scheduler"
			},
			wantContains: []string{"affinity rules require schedulerName to be volcano"},
		},
		{
			name: "rejects empty topology affinity",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.NetworkTopology = &workloadv1alpha1.NetworkTopology{
					ServingGroupAntiAffinity: &workloadv1alpha1.ServingGroupAntiAffinity{},
				}
			},
			wantContains: []string{"at least one topology affinity term"},
		},
		{
			name: "rejects missing and duplicate tiers",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				tier := int32(1)
				ms.Spec.Template.NetworkTopology.ServingGroupAntiAffinity.Required = []workloadv1alpha1.ServingGroupAffinityTerm{
					{},
					{TopologyTierName: "rack", TopologyTier: &tier},
				}
			},
			wantContains: []string{
				"spec.template.networkTopology.servingGroupAntiAffinity.required[0]",
				"spec.template.networkTopology.servingGroupAntiAffinity.required[1]",
				"exactly one of topologyTierName and topologyTier",
			},
		},
		{
			name: "validates required and preferred weights",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				requiredWeight := int32(1)
				invalidPreferredWeight := int32(101)
				ms.Spec.Template.NetworkTopology.ServingGroupAntiAffinity.Required[0].Weight = &requiredWeight
				ms.Spec.Template.NetworkTopology.ServingGroupAntiAffinity.Preferred = []workloadv1alpha1.ServingGroupAffinityTerm{
					{TopologyTierName: "rack"},
					{Weight: &invalidPreferredWeight, TopologyTierName: "rack"},
				}
			},
			wantContains: []string{
				"weight must not be set in a required term",
				"weight is required in a preferred term",
				"must be between 1 and 100",
			},
		},
		{
			name: "validates Role references cardinality and uniqueness",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.NetworkTopology.RoleAffinity = &workloadv1alpha1.RoleAffinity{
					Required: []workloadv1alpha1.RoleAffinityTerm{
						{Roles: []string{"prefill"}, TopologyTierName: "rack"},
						{Roles: []string{"prefill", "missing", "prefill"}, TopologyTierName: "rack"},
					},
				}
			},
			wantContains: []string{
				"must contain at least 2 distinct Role name(s)",
				"Not found",
				"Duplicate value",
			},
		},
		{
			name: "defers named-tier relationship validation to Volcano",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Template.NetworkTopology.RoleAffinity = &workloadv1alpha1.RoleAffinity{
					Required: []workloadv1alpha1.RoleAffinityTerm{
						{Roles: []string{"prefill", "decode"}, TopologyTierName: "rack"},
					},
				}
				ms.Spec.Template.NetworkTopology.RoleAntiAffinity = &workloadv1alpha1.RoleAntiAffinity{
					Required: []workloadv1alpha1.RoleAffinityTerm{
						{Roles: []string{"prefill", "decode"}, TopologyTierName: "rack"},
					},
				}
			},
		},
		{
			name: "allows equal numeric Role tiers",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				rackTier := int32(1)
				ms.Spec.Template.NetworkTopology.RoleAffinity = &workloadv1alpha1.RoleAffinity{
					Required: []workloadv1alpha1.RoleAffinityTerm{
						{Roles: []string{"prefill", "decode"}, TopologyTier: &rackTier},
					},
				}
				ms.Spec.Template.NetworkTopology.RoleAntiAffinity = &workloadv1alpha1.RoleAntiAffinity{
					Required: []workloadv1alpha1.RoleAffinityTerm{
						{Roles: []string{"prefill", "decode"}, TopologyTier: &rackTier},
					},
				}
			},
		},
		{
			name: "rejects broader numeric anti-affinity tier",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				nodeTier := int32(0)
				rackTier := int32(1)
				ms.Spec.Template.NetworkTopology.RoleAffinity = &workloadv1alpha1.RoleAffinity{
					Required: []workloadv1alpha1.RoleAffinityTerm{
						{Roles: []string{"prefill", "decode"}, TopologyTier: &nodeTier},
					},
				}
				ms.Spec.Template.NetworkTopology.RoleAntiAffinity = &workloadv1alpha1.RoleAntiAffinity{
					Required: []workloadv1alpha1.RoleAffinityTerm{
						{Roles: []string{"prefill", "decode"}, TopologyTier: &rackTier},
					},
				}
			},
			wantContains: []string{"broader anti-affinity tier"},
		},
		{
			name: "allows affinity at rack and anti-affinity at node",
			mutate: func(ms *workloadv1alpha1.ModelServing) {
				nodeTier := int32(0)
				rackTier := int32(1)
				ms.Spec.Template.NetworkTopology.RoleAffinity = &workloadv1alpha1.RoleAffinity{
					Required: []workloadv1alpha1.RoleAffinityTerm{
						{Roles: []string{"prefill", "decode"}, TopologyTier: &rackTier},
					},
				}
				ms.Spec.Template.NetworkTopology.RoleAntiAffinity = &workloadv1alpha1.RoleAntiAffinity{
					Required: []workloadv1alpha1.RoleAffinityTerm{
						{Roles: []string{"prefill"}, TopologyTier: &nodeTier},
						{Roles: []string{"decode"}, TopologyTier: &nodeTier},
					},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := newModelServing()
			tt.mutate(ms)
			errs := validateTopologyAffinity(ms)
			if len(tt.wantContains) == 0 {
				assert.Empty(t, errs)
				return
			}
			combined := errs.ToAggregate().Error()
			for _, expected := range tt.wantContains {
				assert.Contains(t, combined, expected)
			}
		})
	}
}

func TestValidateRollingUpdateConfiguration(t *testing.T) {
	replicas := int32(3)
	type args struct {
		ms *workloadv1alpha1.ModelServing
	}
	tests := []struct {
		name string
		args args
		want field.ErrorList
	}{
		{
			name: "normal rolling update configuration",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							Type: workloadv1alpha1.ServingGroupRollingUpdate,
							RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
								MaxUnavailable: &intstr.IntOrString{
									Type:   intstr.Int,
									IntVal: 1,
								},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "rejects configuration for role rolling update",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							Type: workloadv1alpha1.RoleRollingUpdate,
							RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
								MaxUnavailable: &intstr.IntOrString{
									Type:   intstr.Int,
									IntVal: 1,
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec").Child("rolloutStrategy").Child("rollingUpdateConfiguration"),
					"rollingUpdateConfiguration is only valid when rolloutStrategy.type is ServingGroupRollingUpdate",
				),
			},
		},
		{
			name: "invalid maxUnavailable format",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
								MaxUnavailable: &intstr.IntOrString{
									Type:   intstr.String,
									StrVal: "invalid",
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("rolloutStrategy").Child("rollingUpdateConfiguration").Child("maxUnavailable"),
					&intstr.IntOrString{
						Type:   intstr.String,
						StrVal: "invalid",
					},
					"a valid percent string must be a numeric string followed by an ending '%' (e.g. '1%',  or '93%', regex used for validation is '[0-9]+%')",
				),
				field.Invalid(
					field.NewPath("spec").Child("rolloutStrategy").Child("rollingUpdateConfiguration").Child("maxUnavailable"),
					&intstr.IntOrString{
						Type:   intstr.String,
						StrVal: "invalid",
					},
					"invalid maxUnavailable: invalid value for IntOrString: invalid type: string is not a percentage",
				),
			},
		},
		{
			name: "both maxUnavailable and maxSurge are zero",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
								MaxUnavailable: &intstr.IntOrString{
									Type:   intstr.Int,
									IntVal: 0,
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("rolloutStrategy").Child("rollingUpdateConfiguration"),
					"",
					"maxUnavailable and maxSurge cannot both resolve to 0",
				),
			},
		},
		{
			name: "allows zero maxUnavailable with positive maxSurge",
			args: args{ms: &workloadv1alpha1.ModelServing{Spec: workloadv1alpha1.ModelServingSpec{
				Replicas: &replicas,
				RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
					Type: workloadv1alpha1.ServingGroupRollingUpdate,
					RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
						MaxUnavailable: ptr.To(intstr.FromInt(0)),
						MaxSurge:       ptr.To(intstr.FromString("25%")),
					},
				},
			}}},
			want: nil,
		},
		{
			name: "allows non-zero percentage maxUnavailable that rounds down",
			args: args{ms: &workloadv1alpha1.ModelServing{Spec: workloadv1alpha1.ModelServingSpec{
				Replicas: &replicas,
				RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
					Type: workloadv1alpha1.ServingGroupRollingUpdate,
					RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
						MaxUnavailable: ptr.To(intstr.FromString("20%")),
					},
				},
			}}},
			want: nil,
		},
		{
			name: "maxUnavailable greater than replicas is allowed for scale down",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
								MaxUnavailable: &intstr.IntOrString{
									Type:   intstr.Int,
									IntVal: 4,
								},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "valid partition - within range",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
								MaxUnavailable: &intstr.IntOrString{
									Type:   intstr.Int,
									IntVal: 1,
								},
								Partition: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "invalid partition - negative value",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
								MaxUnavailable: &intstr.IntOrString{
									Type:   intstr.Int,
									IntVal: 1,
								},
								Partition: &intstr.IntOrString{Type: intstr.Int, IntVal: -1},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("rolloutStrategy").Child("rollingUpdateConfiguration").Child("partition"),
					int64(-1),
					"must be a non-negative integer",
				),
			},
		},
		{
			name: "valid partition - equal to replicas",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
								MaxUnavailable: &intstr.IntOrString{
									Type:   intstr.Int,
									IntVal: 1,
								},
								Partition: &intstr.IntOrString{Type: intstr.Int, IntVal: 3},
							},
						},
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:     "predictor",
									Replicas: ptr.To[int32](1),
									EntryTemplate: workloadv1alpha1.PodTemplateSpec{
										Metadata: &workloadv1alpha1.Metadata{},
									},
									WorkerReplicas: 0,
								},
							},
						},
					},
				},
			},
			want: nil,
		},
		{
			name: "valid partition - greater than replicas",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
								MaxUnavailable: &intstr.IntOrString{
									Type:   intstr.Int,
									IntVal: 1,
								},
								Partition: &intstr.IntOrString{Type: intstr.Int, IntVal: 5},
							},
						},
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:     "predictor",
									Replicas: ptr.To[int32](1),
									EntryTemplate: workloadv1alpha1.PodTemplateSpec{
										Metadata: &workloadv1alpha1.Metadata{},
									},
									WorkerReplicas: 0,
								},
							},
						},
					},
				},
			},
			want: nil,
		},
		{
			name: "valid partition - zero value",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
								MaxUnavailable: &intstr.IntOrString{
									Type:   intstr.Int,
									IntVal: 1,
								},
								Partition: &intstr.IntOrString{Type: intstr.Int, IntVal: 0},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "valid partition - percentage value",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
								MaxUnavailable: &intstr.IntOrString{
									Type:   intstr.Int,
									IntVal: 1,
								},
								Partition: &intstr.IntOrString{Type: intstr.String, StrVal: "50%"},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "invalid partition - percentage over 100",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
								MaxUnavailable: &intstr.IntOrString{
									Type:   intstr.Int,
									IntVal: 1,
								},
								Partition: &intstr.IntOrString{Type: intstr.String, StrVal: "110%"},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("rolloutStrategy").Child("rollingUpdateConfiguration").Child("partition"),
					&intstr.IntOrString{Type: intstr.String, StrVal: "110%"},
					"must be a valid percent value (0-100)",
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateRollingUpdateConfiguration(tt.args.ms)
			if got != nil {
				assert.EqualValues(t, tt.want, got)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestValidateMaxUnavailableForRoles(t *testing.T) {
	tests := []struct {
		name    string
		ms      *workloadv1alpha1.ModelServing
		wantErr bool
	}{
		{
			name: "valid with role rolling update",
			ms: &workloadv1alpha1.ModelServing{Spec: workloadv1alpha1.ModelServingSpec{
				RolloutStrategy: &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate},
				Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{{
					Name:     "decode",
					Replicas: ptr.To[int32](4),
					RollingUpdateConfiguration: workloadv1alpha1.RollingUpdateConfiguration{
						MaxUnavailable: ptr.To(intstr.FromInt(2)),
					},
				}}},
			}},
		},
		{
			name: "rejects zero maxUnavailable without maxSurge",
			ms: &workloadv1alpha1.ModelServing{Spec: workloadv1alpha1.ModelServingSpec{
				RolloutStrategy: &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate},
				Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{{
					Name:     "decode",
					Replicas: ptr.To[int32](4),
					RollingUpdateConfiguration: workloadv1alpha1.RollingUpdateConfiguration{
						MaxUnavailable: ptr.To(intstr.FromString("0%")),
					},
				}}},
			}},
			wantErr: true,
		},
		{
			name: "allows CRD default maxUnavailable with serving group rolling update",
			ms: &workloadv1alpha1.ModelServing{Spec: workloadv1alpha1.ModelServingSpec{
				RolloutStrategy: &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.ServingGroupRollingUpdate},
				Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{{
					Name:     "decode",
					Replicas: ptr.To[int32](4),
					RollingUpdateConfiguration: workloadv1alpha1.RollingUpdateConfiguration{
						MaxUnavailable: ptr.To(intstr.FromInt(1)),
					},
				}}},
			}},
		},
		{
			name: "requires role rolling update for partition",
			ms: &workloadv1alpha1.ModelServing{Spec: workloadv1alpha1.ModelServingSpec{
				RolloutStrategy: &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.ServingGroupRollingUpdate},
				Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{{
					Name:     "decode",
					Replicas: ptr.To[int32](4),
					RollingUpdateConfiguration: workloadv1alpha1.RollingUpdateConfiguration{
						Partition: ptr.To(intstr.FromInt(1)),
					},
				}}},
			}},
			wantErr: true,
		},
		{
			name: "allows role maxSurge",
			ms: &workloadv1alpha1.ModelServing{Spec: workloadv1alpha1.ModelServingSpec{
				RolloutStrategy: &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate},
				Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{{
					Name:     "decode",
					Replicas: ptr.To[int32](4),
					RollingUpdateConfiguration: workloadv1alpha1.RollingUpdateConfiguration{
						MaxUnavailable: ptr.To(intstr.FromInt(1)),
						MaxSurge:       ptr.To(intstr.FromInt(1)),
					},
				}}},
			}},
		},
		{
			name: "allows zero maxUnavailable with positive maxSurge",
			ms: &workloadv1alpha1.ModelServing{Spec: workloadv1alpha1.ModelServingSpec{
				RolloutStrategy: &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate},
				Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{{
					Name:     "decode",
					Replicas: ptr.To[int32](4),
					RollingUpdateConfiguration: workloadv1alpha1.RollingUpdateConfiguration{
						MaxUnavailable: ptr.To(intstr.FromInt(0)),
						MaxSurge:       ptr.To(intstr.FromString("25%")),
					},
				}}},
			}},
		},
		{
			name: "rejects role maxSurge for serving group rolling update",
			ms: &workloadv1alpha1.ModelServing{Spec: workloadv1alpha1.ModelServingSpec{
				RolloutStrategy: &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.ServingGroupRollingUpdate},
				Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{{
					Name:     "decode",
					Replicas: ptr.To[int32](4),
					RollingUpdateConfiguration: workloadv1alpha1.RollingUpdateConfiguration{
						MaxSurge: ptr.To(intstr.FromInt(1)),
					},
				}}},
			}},
			wantErr: true,
		},
		{
			name: "rejects maxUnavailable greater than role replicas",
			ms: &workloadv1alpha1.ModelServing{Spec: workloadv1alpha1.ModelServingSpec{
				RolloutStrategy: &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate},
				Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{{
					Name:     "decode",
					Replicas: ptr.To[int32](3),
					RollingUpdateConfiguration: workloadv1alpha1.RollingUpdateConfiguration{
						MaxUnavailable: ptr.To(intstr.FromInt(4)),
					},
				}}},
			}},
			wantErr: true,
		},
		{
			name: "allows maxUnavailable equal to role replicas",
			ms: &workloadv1alpha1.ModelServing{Spec: workloadv1alpha1.ModelServingSpec{
				RolloutStrategy: &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate},
				Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{{
					Name:     "decode",
					Replicas: ptr.To[int32](3),
					RollingUpdateConfiguration: workloadv1alpha1.RollingUpdateConfiguration{
						MaxUnavailable: ptr.To(intstr.FromInt(3)),
					},
				}}},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateMaxUnavailableForRoles(tt.ms)
			if tt.wantErr {
				assert.NotEmpty(t, got)
			} else {
				assert.Empty(t, got)
			}
		})
	}
}

func TestValidatorReplicas(t *testing.T) {
	type args struct {
		ms *workloadv1alpha1.ModelServing
	}
	tests := []struct {
		name string
		args args
		want field.ErrorList
	}{
		{
			name: "normal replicas",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: int32Ptr(3),
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "role1",
									Replicas:       int32Ptr(2),
									WorkerReplicas: 1,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
								{
									Name:           "role2",
									Replicas:       int32Ptr(1),
									WorkerReplicas: 1,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "replicas is nil",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: int32PtrNil(),
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "role1",
									Replicas:       int32Ptr(2),
									WorkerReplicas: 1,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
								{
									Name:           "role2",
									Replicas:       int32Ptr(1),
									WorkerReplicas: 1,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("replicas"),
					int32PtrNil(),
					"replicas must be a non-negative integer",
				),
			},
		},
		{
			name: "replicas is less than 0",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: int32Ptr(-1),
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "role1",
									Replicas:       int32Ptr(2),
									WorkerReplicas: 1,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
								{
									Name:           "role2",
									Replicas:       int32Ptr(1),
									WorkerReplicas: 1,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("replicas"),
					int32Ptr(-1),
					"replicas must be a non-negative integer",
				),
			},
		},
		{
			name: "role replicas is less than 0",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: int32Ptr(3),
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "role1",
									Replicas:       int32Ptr(-1),
									WorkerReplicas: 1,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
								{
									Name:           "role2",
									Replicas:       int32Ptr(1),
									WorkerReplicas: 1,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("template").Child("roles").Index(0).Child("replicas"),
					int32Ptr(-1),
					"role replicas must be a non-negative integer",
				),
			},
		},
		{
			name: "role replicas is nil",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: int32Ptr(3),
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "role1",
									Replicas:       int32PtrNil(),
									WorkerReplicas: 1,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
								{
									Name:           "role2",
									Replicas:       int32Ptr(1),
									WorkerReplicas: 1,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("template").Child("roles").Index(0).Child("replicas"),
					int32PtrNil(),
					"role replicas must be a non-negative integer",
				),
			},
		},
		{
			name: "no role",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: int32Ptr(3),
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("template").Child("roles"),
					[]workloadv1alpha1.Role{},
					"roles must be specified",
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validatorReplicas(tt.args.ms)
			if got != nil {
				assert.EqualValues(t, tt.want, got)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestValidateGangPolicy(t *testing.T) {
	replicas := int32(3)
	roleReplicas := int32(2)
	type args struct {
		ms *workloadv1alpha1.ModelServing
	}
	tests := []struct {
		name string
		args args
		want field.ErrorList
	}{
		{
			name: "valid minRoleReplicas",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "worker",
									Replicas:       &roleReplicas,
									WorkerReplicas: 3,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
							GangPolicy: &workloadv1alpha1.GangPolicy{
								MinRoleReplicas: map[string]int32{
									"worker": 2,
								},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "invalid minRoleReplicas - role not exist",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "worker",
									Replicas:       &roleReplicas,
									WorkerReplicas: 3,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
							GangPolicy: &workloadv1alpha1.GangPolicy{
								MinRoleReplicas: map[string]int32{
									"nonexistent": 1,
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("template").Child("gangPolicy").Child("minRoleReplicas").Key("nonexistent"),
					"nonexistent",
					"role nonexistent does not exist in template.roles",
				),
			},
		},
		{
			name: "invalid minRoleReplicas - exceeds role replicas",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "worker",
									Replicas:       &roleReplicas,
									WorkerReplicas: 3,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
							GangPolicy: &workloadv1alpha1.GangPolicy{
								MinRoleReplicas: map[string]int32{
									"worker": 10, // exceeds replicas 2
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("template").Child("gangPolicy").Child("minRoleReplicas").Key("worker"),
					int32(10),
					"minRoleReplicas (10) for role worker cannot exceed replicas (2)",
				),
			},
		},
		{
			name: "invalid minRoleReplicas - negative value",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "worker",
									Replicas:       &roleReplicas,
									WorkerReplicas: 3,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
							GangPolicy: &workloadv1alpha1.GangPolicy{
								MinRoleReplicas: map[string]int32{
									"worker": -1,
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("template").Child("gangPolicy").Child("minRoleReplicas").Key("worker"),
					int32(-1),
					"minRoleReplicas for role worker must be non-negative",
				),
			},
		},
		{
			name: "nil gang Policy",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "worker",
									Replicas:       &roleReplicas,
									WorkerReplicas: 3,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
							GangPolicy: nil,
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "nil minRoleReplicas",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "worker",
									Replicas:       &roleReplicas,
									WorkerReplicas: 3,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
							GangPolicy: &workloadv1alpha1.GangPolicy{
								MinRoleReplicas: nil,
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateGangPolicy(tt.args.ms)
			if got != nil {
				assert.EqualValues(t, tt.want, got)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestValidateWorkerReplicas(t *testing.T) {
	replicas := int32(3)
	roleReplicas := int32(2)
	type args struct {
		ms *workloadv1alpha1.ModelServing
	}
	tests := []struct {
		name string
		args args
		want field.ErrorList
	}{
		{
			name: "WorkerReplicas > 0 but WorkerTemplate is nil",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas, // It Uses the variable defined at top of test
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "worker",
									Replicas:       &roleReplicas,
									WorkerReplicas: 1,   // > 0 to trigger the check
									WorkerTemplate: nil, // Missing template!
								},
							},
						},
					},
				},
			},

			want: field.ErrorList{
				field.Required(
					field.NewPath("spec").Child("template").Child("roles").Index(0).Child("workerTemplate"),
					"workerTemplate is required when workerReplicas is greater than 0",
				),
			},
		},

		{
			name: "valid zero worker replicas",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "worker",
									Replicas:       &roleReplicas,
									WorkerReplicas: 0,
								},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "invalid negative worker replicas",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "worker",
									Replicas:       &roleReplicas,
									WorkerReplicas: -1,
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("template").Child("roles").Index(0).Child("workerReplicas"),
					int32(-1),
					"workerReplicas must be a non-negative integer",
				),
			},
		},
		{
			name: "multiple roles with one invalid worker replicas",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "worker1",
									Replicas:       &roleReplicas,
									WorkerReplicas: 3,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
								{
									Name:           "worker2",
									Replicas:       &roleReplicas,
									WorkerReplicas: -1,
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("template").Child("roles").Index(1).Child("workerReplicas"),
					int32(-1),
					"workerReplicas must be a non-negative integer",
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateWorkerReplicas(tt.args.ms)
			if got != nil {
				assert.EqualValues(t, tt.want, got)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestValidateRoleNames(t *testing.T) {
	replicas := int32(3)
	type args struct {
		ms *workloadv1alpha1.ModelServing
	}
	tests := []struct {
		name string
		args args
		want field.ErrorList
	}{
		{
			name: "valid lowercase role name",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "prefill",
									Replicas:       &replicas,
									WorkerReplicas: 2,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
								{
									Name:           "decode",
									Replicas:       &replicas,
									WorkerReplicas: 2,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "invalid uppercase role name",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "Prefill",
									Replicas:       &replicas,
									WorkerReplicas: 2,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("template").Child("roles").Index(0).Child("name"),
					"Prefill",
					"role name must be a valid DNS-1035 label (lowercase alphanumeric characters or '-', must start with a letter): a DNS-1035 label must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character",
				),
			},
		},
		{
			name: "invalid role name starting with number",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "1role",
									Replicas:       &replicas,
									WorkerReplicas: 2,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("template").Child("roles").Index(0).Child("name"),
					"1role",
					"role name must be a valid DNS-1035 label (lowercase alphanumeric characters or '-', must start with a letter): a DNS-1035 label must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character",
				),
			},
		},
		{
			name: "invalid role name ending with hyphen",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "role-",
									Replicas:       &replicas,
									WorkerReplicas: 2,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("template").Child("roles").Index(0).Child("name"),
					"role-",
					"role name must be a valid DNS-1035 label (lowercase alphanumeric characters or '-', must start with a letter): a DNS-1035 label must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character",
				),
			},
		},
		{
			name: "multiple roles with one invalid",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{
									Name:           "prefill",
									Replicas:       &replicas,
									WorkerReplicas: 2,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
								{
									Name:           "Decode",
									Replicas:       &replicas,
									WorkerReplicas: 2,
									WorkerTemplate: &workloadv1alpha1.PodTemplateSpec{}, // <--- FIXED TYPE
								},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("template").Child("roles").Index(1).Child("name"),
					"Decode",
					"role name must be a valid DNS-1035 label (lowercase alphanumeric characters or '-', must start with a letter): a DNS-1035 label must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character",
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateRoleNames(tt.args.ms)
			if len(got) > 0 {
				// Check that we got an error for the expected field
				assert.Equal(t, len(tt.want), len(got), "error count mismatch")
				if len(tt.want) > 0 && len(got) > 0 {
					assert.Equal(t, tt.want[0].Field, got[0].Field, "field path mismatch")
					assert.Equal(t, tt.want[0].BadValue, got[0].BadValue, "bad value mismatch")
					// Check that error message contains the expected text
					assert.Contains(t, got[0].Detail, "role name must be a valid DNS-1035 label", "error message should mention DNS-1035")
				}
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestValidateRecoveryPolicyAndRolloutStrategy(t *testing.T) {
	replicas := int32(3)

	type args struct {
		ms *workloadv1alpha1.ModelServing
	}
	tests := []struct {
		name string
		args args
		want field.ErrorList
	}{
		{
			name: "no recovery policy and no rollout strategy - valid",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					ObjectMeta: v1.ObjectMeta{
						Name: "test-model-serving",
					},
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas: &replicas,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{Name: "role1", Replicas: &replicas, WorkerReplicas: 2},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "serving group recovery policy with role rollout strategy - invalid",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					ObjectMeta: v1.ObjectMeta{
						Name: "test-model-serving",
					},
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas:       &replicas,
						RecoveryPolicy: workloadv1alpha1.ServingGroupRecreate,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							Type: workloadv1alpha1.RoleRollingUpdate,
						},
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{Name: "role1", Replicas: &replicas, WorkerReplicas: 2},
							},
						},
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("rolloutStrategy").Child("type"),
					workloadv1alpha1.RoleRollingUpdate,
					"incompatible recoveryPolicy and rolloutStrategy.type after applying defaults: recoveryPolicy=ServingGroupRecreate, rolloutStrategy.type=RoleRollingUpdate; valid pairs: (ServingGroupRecreate,ServingGroupRollingUpdate) or (RoleRecreate,RoleRollingUpdate)",
				),
			},
		},
		{
			name: "recovery policy ServingGroupRecreate with compatible rollout strategy ServingGroup - valid",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					ObjectMeta: v1.ObjectMeta{
						Name: "test-model-serving",
					},
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas:       &replicas,
						RecoveryPolicy: workloadv1alpha1.ServingGroupRecreate,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							Type: workloadv1alpha1.ServingGroupRollingUpdate,
						},
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{Name: "role1", Replicas: &replicas, WorkerReplicas: 2},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "recovery policy RoleRecreate with compatible rollout strategy Role - valid",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					ObjectMeta: v1.ObjectMeta{
						Name: "test-model-serving",
					},
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas:       &replicas,
						RecoveryPolicy: workloadv1alpha1.RoleRecreate,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							Type: workloadv1alpha1.RoleRollingUpdate,
						},
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{Name: "role1", Replicas: &replicas, WorkerReplicas: 2},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "recovery policy RoleRecreate with rollout strategy ServingGroup - valid",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					ObjectMeta: v1.ObjectMeta{
						Name: "test-model-serving",
					},
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas:       &replicas,
						RecoveryPolicy: workloadv1alpha1.RoleRecreate,
						RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
							Type: workloadv1alpha1.ServingGroupRollingUpdate,
						},
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{Name: "role1", Replicas: &replicas, WorkerReplicas: 2},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
		{
			name: "serving group recovery policy without rollout strategy - valid (default rollout is ServingGroupRollingUpdate)",
			args: args{
				ms: &workloadv1alpha1.ModelServing{
					ObjectMeta: v1.ObjectMeta{
						Name: "test-model-serving",
					},
					Spec: workloadv1alpha1.ModelServingSpec{
						Replicas:       &replicas,
						RecoveryPolicy: workloadv1alpha1.ServingGroupRecreate,
						Template: workloadv1alpha1.ServingGroup{
							Roles: []workloadv1alpha1.Role{
								{Name: "role1", Replicas: &replicas, WorkerReplicas: 2},
							},
						},
					},
				},
			},
			want: field.ErrorList(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateRecoveryPolicyAndRolloutStrategy(tt.args.ms)

			// Compare the error lists
			if len(got) != len(tt.want) {
				t.Errorf("validateRecoveryPolicyAndRolloutStrategy() = %v, want %v", got, tt.want)
				return
			}

			for i := range got {
				assert.Equalf(t, tt.want[i].Error(), got[i].Error(), "Error mismatch at index %d", i)
			}
		})
	}
}

func TestValidateEvictionStrategyRoleMinAvailable(t *testing.T) {
	replicas := int32(3)
	roleReplicas := int32(2)
	tests := []struct {
		name              string
		protection        workloadv1alpha1.ProtectionLevelType
		minAvailable      *intstr.IntOrString
		roleMinAvail      map[string]intstr.IntOrString
		wantAllowed       bool
		wantMessage       string
		useEmptyRoleMap   bool
		omitProtectionVal bool
	}{
		{
			name:        "reject servingGroup without minAvailable",
			protection:  workloadv1alpha1.ProtectionLevelServingGroup,
			wantAllowed: false,
			wantMessage: "minAvailable is required when evictionStrategy.protectionLevel is ServingGroup",
		},
		{
			name:         "valid servingGroup integer minAvailable",
			protection:   workloadv1alpha1.ProtectionLevelServingGroup,
			minAvailable: intstrPtr(intstr.FromInt(2)),
			wantAllowed:  true,
		},
		{
			name:         "valid servingGroup percent minAvailable",
			protection:   workloadv1alpha1.ProtectionLevelServingGroup,
			minAvailable: intstrPtr(intstr.FromString("67%")),
			wantAllowed:  true,
		},
		{
			name:         "reject servingGroup minAvailable above replicas",
			protection:   workloadv1alpha1.ProtectionLevelServingGroup,
			minAvailable: intstrPtr(intstr.FromInt(4)),
			wantAllowed:  false,
			wantMessage:  "minAvailable (4) cannot exceed replicas (3)",
		},
		{
			name:         "reject servingGroup negative minAvailable",
			protection:   workloadv1alpha1.ProtectionLevelServingGroup,
			minAvailable: intstrPtr(intstr.FromInt(-1)),
			wantAllowed:  false,
			wantMessage:  "must be a non-negative integer",
		},
		{
			name:       "valid role minAvailable without global minAvailable",
			protection: workloadv1alpha1.ProtectionLevelRole,
			roleMinAvail: map[string]intstr.IntOrString{
				"decode": intstr.FromInt(1),
			},
			wantAllowed: true,
		},
		{
			name:        "reject role without roleMinAvailable",
			protection:  workloadv1alpha1.ProtectionLevelRole,
			wantAllowed: false,
			wantMessage: "roleMinAvailable is required when evictionStrategy.protectionLevel is Role",
		},
		{
			name:            "reject role with empty roleMinAvailable",
			protection:      workloadv1alpha1.ProtectionLevelRole,
			roleMinAvail:    map[string]intstr.IntOrString{},
			useEmptyRoleMap: true,
			wantAllowed:     false,
			wantMessage:     "roleMinAvailable is required when evictionStrategy.protectionLevel is Role",
		},
		{
			name:       "reject unknown role key",
			protection: workloadv1alpha1.ProtectionLevelRole,
			roleMinAvail: map[string]intstr.IntOrString{
				"unknown": intstr.FromInt(1),
			},
			wantAllowed: false,
			wantMessage: "role unknown does not exist in template.roles",
		},
		{
			name:       "reject invalid role percent",
			protection: workloadv1alpha1.ProtectionLevelRole,
			roleMinAvail: map[string]intstr.IntOrString{
				"decode": intstr.FromString("101%"),
			},
			wantAllowed: false,
			wantMessage: "must be a valid percent value",
		},
		{
			name:       "reject role minAvailable above role replicas",
			protection: workloadv1alpha1.ProtectionLevelRole,
			roleMinAvail: map[string]intstr.IntOrString{
				"decode": intstr.FromInt(3),
			},
			wantAllowed: false,
			wantMessage: "roleMinAvailable (3) for role decode cannot exceed replicas (2)",
		},
		{
			name:       "valid role percent minAvailable",
			protection: workloadv1alpha1.ProtectionLevelRole,
			roleMinAvail: map[string]intstr.IntOrString{
				"decode": intstr.FromString("50%"),
			},
			wantAllowed: true,
		},
		{
			name:         "role ignores global minAvailable",
			protection:   workloadv1alpha1.ProtectionLevelRole,
			minAvailable: intstrPtr(intstr.FromInt(4)),
			roleMinAvail: map[string]intstr.IntOrString{
				"decode": intstr.FromInt(1),
			},
			wantAllowed: true,
		},
		{
			name:              "empty protectionLevel defaults to servingGroup and requires minAvailable",
			omitProtectionVal: true,
			wantAllowed:       false,
			wantMessage:       "minAvailable is required when evictionStrategy.protectionLevel is ServingGroup",
		},
		{
			name:         "reject invalid servingGroup percent",
			protection:   workloadv1alpha1.ProtectionLevelServingGroup,
			minAvailable: intstrPtr(intstr.FromString("101%")),
			wantAllowed:  false,
			wantMessage:  "must be a valid percent value",
		},
		{
			name:         "reject unsupported protectionLevel",
			protection:   workloadv1alpha1.ProtectionLevelType("Invalid"),
			minAvailable: intstrPtr(intstr.FromInt(1)),
			wantAllowed:  false,
			wantMessage:  "Unsupported value: \"Invalid\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := &workloadv1alpha1.EvictionStrategySpec{
				MinAvailable:     tt.minAvailable,
				RoleMinAvailable: tt.roleMinAvail,
			}
			if !tt.omitProtectionVal {
				strategy.ProtectionLevel = tt.protection
			}
			if tt.useEmptyRoleMap {
				strategy.RoleMinAvailable = tt.roleMinAvail
			}

			ms := &workloadv1alpha1.ModelServing{
				Spec: workloadv1alpha1.ModelServingSpec{
					Replicas: &replicas,
					RolloutStrategy: &workloadv1alpha1.RolloutStrategy{
						EvictionStrategy: strategy,
					},
					Template: workloadv1alpha1.ServingGroup{
						Roles: []workloadv1alpha1.Role{
							{Name: "decode", Replicas: &roleReplicas},
						},
					},
				},
			}

			errs := validateEvictionStrategy(ms)
			if tt.wantAllowed {
				assert.Empty(t, errs)
				return
			}
			assert.NotEmpty(t, errs)
			assert.Contains(t, errs.ToAggregate().Error(), tt.wantMessage)
		})
	}
}

func int32Ptr(i int32) *int32 {
	return &i
}

func int32PtrNil() *int32 {
	return nil
}

func TestValidateRanktablePlugin(t *testing.T) {
	// Setup fake client
	kubeClient := fake.NewSimpleClientset()
	v := NewModelServingValidator(kubeClient)

	// Create a valid template ConfigMap in default namespace
	templateName := "valid-template"
	_, err := kubeClient.CoreV1().ConfigMaps("default").Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: v1.ObjectMeta{
			Name:      templateName,
			Namespace: "default",
		},
	}, v1.CreateOptions{})
	assert.NoError(t, err)

	// Helper to create JSON config
	createConfig := func(template string) *apiextensionsv1.JSON {
		cfg := ranktable.RanktableConfig{
			Template: template,
		}
		bytes, _ := json.Marshal(cfg)
		return &apiextensionsv1.JSON{Raw: bytes}
	}

	tests := []struct {
		name          string
		ms            *workloadv1alpha1.ModelServing
		setupEnv      func()
		teardownEnv   func()
		expectedError bool
		errorMsg      string
	}{
		{
			name: "valid ranktable plugin config",
			ms: &workloadv1alpha1.ModelServing{
				Spec: workloadv1alpha1.ModelServingSpec{
					Plugins: []workloadv1alpha1.PluginSpec{
						{
							Name:   ranktable.PluginName,
							Config: createConfig(templateName),
						},
					},
				},
			},
			expectedError: false,
		},
		{
			name: "missing template config",
			ms: &workloadv1alpha1.ModelServing{
				Spec: workloadv1alpha1.ModelServingSpec{
					Plugins: []workloadv1alpha1.PluginSpec{
						{
							Name:   ranktable.PluginName,
							Config: createConfig(""),
						},
					},
				},
			},
			expectedError: true,
			errorMsg:      "ranktable template is required",
		},
		{
			name: "non-existent template configmap",
			ms: &workloadv1alpha1.ModelServing{
				Spec: workloadv1alpha1.ModelServingSpec{
					Plugins: []workloadv1alpha1.PluginSpec{
						{
							Name:   ranktable.PluginName,
							Config: createConfig("non-existent-template"),
						},
					},
				},
			},
			expectedError: true,
			errorMsg:      "ranktable template ConfigMap 'non-existent-template' not found",
		},
		{
			name: "valid template in custom namespace",
			ms: &workloadv1alpha1.ModelServing{
				Spec: workloadv1alpha1.ModelServingSpec{
					Plugins: []workloadv1alpha1.PluginSpec{
						{
							Name:   ranktable.PluginName,
							Config: createConfig("custom-template"),
						},
					},
				},
			},
			setupEnv: func() {
				os.Setenv("POD_NAMESPACE", "custom-ns")
				_, _ = kubeClient.CoreV1().ConfigMaps("custom-ns").Create(context.Background(), &corev1.ConfigMap{
					ObjectMeta: v1.ObjectMeta{
						Name:      "custom-template",
						Namespace: "custom-ns",
					},
				}, v1.CreateOptions{})
			},
			teardownEnv: func() {
				os.Unsetenv("POD_NAMESPACE")
				_ = kubeClient.CoreV1().ConfigMaps("custom-ns").Delete(context.Background(), "custom-template", v1.DeleteOptions{})
			},
			expectedError: false,
		},
		{
			name: "missing template in custom namespace",
			ms: &workloadv1alpha1.ModelServing{
				Spec: workloadv1alpha1.ModelServingSpec{
					Plugins: []workloadv1alpha1.PluginSpec{
						{
							Name:   ranktable.PluginName,
							Config: createConfig("missing-custom-template"),
						},
					},
				},
			},
			setupEnv: func() {
				os.Setenv("POD_NAMESPACE", "custom-ns")
			},
			teardownEnv: func() {
				os.Unsetenv("POD_NAMESPACE")
			},
			expectedError: true,
			errorMsg:      "ranktable template ConfigMap 'missing-custom-template' not found in namespace 'custom-ns'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupEnv != nil {
				tt.setupEnv()
			}
			if tt.teardownEnv != nil {
				defer tt.teardownEnv()
			}

			errs := v.validateRanktablePlugin(context.Background(), tt.ms)
			if tt.expectedError {
				assert.NotEmpty(t, errs)
				found := false
				for _, err := range errs {
					if err.Detail != "" && contains(err.Detail, tt.errorMsg) {
						found = true
						break
					}
				}
				// If detail check failed, check string representation or fallback
				if !found {
					// re-check roughly
					for _, err := range errs {
						if contains(err.Error(), tt.errorMsg) {
							found = true
							break
						}
					}
				}
				assert.True(t, found, "Expected error message '%s' not found in %v", tt.errorMsg, errs)
			} else {
				assert.Empty(t, errs)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[0:len(substr)] == substr || (len(s) > len(substr) && contains(s[1:], substr))
}
