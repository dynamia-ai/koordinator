/*
Copyright 2022 The Koordinator Authors.

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

package hamiplacement

import (
	"encoding/json"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	sev1alpha1 "github.com/koordinator-sh/koordinator/apis/scheduling/v1alpha1"
)

func TestWebhookGatesOnlyTheArmedReplicaSet(t *testing.T) {
	p := Placement{JobUID: "job", SourceName: "source", SourceUID: "source", OwnerUID: "owner", NodeName: "target",
		GPUUUIDs: []string{"GPU-a"}, Reservation: "pmj-job", ExpiresAt: metav1.NewTime(time.Now().Add(time.Hour))}
	cm, err := p.ConfigMap("inference", metav1.OwnerReference{
		APIVersion: "scheduling.koordinator.sh/v1alpha1", Kind: "PodMigrationJob", Name: "job-name", UID: p.JobUID,
	})
	require.NoError(t, err)
	cm.UID = "placement-uid"
	job := &sev1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Name: cm.OwnerReferences[0].Name, UID: p.JobUID},
		Status: sev1alpha1.PodMigrationJobStatus{Conditions: []sev1alpha1.PodMigrationJobCondition{
			{Type: LockCondition, Status: sev1alpha1.PodMigrationJobConditionStatusTrue, Message: string(cm.UID)},
			{Type: ReadyCondition, Status: sev1alpha1.PodMigrationJobConditionStatusTrue},
		}},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, sev1alpha1.AddToScheme(scheme))
	source := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: cm.Namespace, Name: p.SourceName, UID: p.SourceUID}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm, job, source).Build()
	w := Webhook{Reader: c, Decoder: admission.NewDecoder(scheme)}
	existingTerm := corev1.NodeSelectorRequirement{Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"inference"}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: cm.Namespace, GenerateName: "model-", OwnerReferences: []metav1.OwnerReference{
			{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "model", UID: "another-owner", Controller: ptr.To(true)},
		}},
		Spec: corev1.PodSpec{SchedulerName: SchedulerName, Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{}, {MatchExpressions: []corev1.NodeSelectorRequirement{existingTerm}},
			}},
		}}},
	}
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create, Namespace: cm.Namespace, UserInfo: authenticationv1.UserInfo{Username: "system:kube-controller-manager"},
	}}
	handle := func() admission.Response {
		t.Helper()
		req.Object.Raw, err = json.Marshal(pod)
		require.NoError(t, err)
		return w.Handle(t.Context(), req)
	}
	response := handle()
	require.True(t, response.Allowed)
	require.Empty(t, response.Patches)
	pod.OwnerReferences[0].UID = p.OwnerUID
	require.Empty(t, handle().Patches, "a live source does not authorize touching other replicas")
	req.DryRun = ptr.To(true)
	require.NotEmpty(t, handle().Patches, "the webhook probe runs before source departure")
	req.DryRun = nil
	require.NoError(t, c.Delete(t.Context(), source))
	req.UserInfo.Username = "user"
	require.False(t, handle().Allowed)
	req.UserInfo.Username = "system:kube-controller-manager"
	pod.Annotations = map[string]string{
		AnnotationUseUUID:                          "GPU-a,GPU-b",
		"resource.hami.io/revision":                "pool-revision",
		"resource.hami.io/pool-selection-revision": "selection-revision",
	}
	response = handle()
	require.True(t, response.Allowed)
	encoded, err := json.Marshal(response.Patches)
	require.NoError(t, err)
	patch, err := jsonpatch.DecodePatch(encoded)
	require.NoError(t, err)
	mutated, err := patch.Apply(req.Object.Raw)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(mutated, pod))
	require.True(t, HasGate(pod))
	require.True(t, PinnedToNode(pod, p.NodeName))
	require.Equal(t, "GPU-a", pod.Annotations[AnnotationUseUUID])
	require.Equal(t, "pmj-job", pod.Annotations[AnnotationReservation])
	require.Equal(t, "job", pod.Annotations[AnnotationJobUID])
	require.Equal(t, "pool-revision", pod.Annotations["resource.hami.io/revision"])
	require.Equal(t, "selection-revision", pod.Annotations["resource.hami.io/pool-selection-revision"])
	require.Empty(t, handle().Patches)

	affinity := nodeaffinity.GetRequiredNodeAffinity(pod)
	for _, tc := range []struct {
		node, pool string
		allowed    bool
	}{
		{"target", "inference", true}, {"different", "inference", false}, {"target", "other", false},
	} {
		match, err := affinity.Match(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: tc.node, Labels: map[string]string{"pool": tc.pool}}})
		require.NoError(t, err)
		require.Equal(t, tc.allowed, match)
	}

	pod.Spec.SchedulingGates = nil
	pod.Annotations = nil
	require.NoError(t, c.Delete(t.Context(), cm))
	cm.UID, cm.ResourceVersion = "forged-placement-uid", ""
	require.NoError(t, c.Create(t.Context(), cm))
	response = handle()
	require.True(t, response.Allowed)
	require.Empty(t, response.Patches)
}

func TestPlacementHonorsExistingGPUConstraints(t *testing.T) {
	placement := Placement{JobUID: "job", NodeName: "target", GPUUUIDs: []string{"GPU-a", "GPU-c"}}
	for _, tc := range []struct {
		name, allow, deny, wantError string
	}{
		{name: "placement before RPM"},
		{name: "narrow RPM whitelist", allow: " GPU-c, GPU-b, GPU-a, GPU-a "},
		{name: "target outside whitelist", allow: "GPU-a,GPU-b", wantError: "GPU-c is outside"},
		{name: "excluded target", allow: "GPU-a,GPU-c", deny: " GPU-a ", wantError: "GPU-a is excluded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
					AnnotationUseUUID: tc.allow, "nvidia.com/nouse-gpuuuid": tc.deny,
				}},
				Spec: corev1.PodSpec{SchedulerName: SchedulerName},
			}
			if tc.wantError != "" {
				require.ErrorContains(t, placement.Apply(pod), tc.wantError)
				require.Equal(t, tc.allow, pod.Annotations[AnnotationUseUUID])
				require.False(t, HasGate(pod))
				return
			}
			require.NoError(t, placement.Apply(pod))
			require.Equal(t, "GPU-a,GPU-c", pod.Annotations[AnnotationUseUUID])
			require.NoError(t, placement.Apply(pod))
			require.Len(t, pod.Spec.SchedulingGates, 1)
		})
	}
}
