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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	sev1alpha1 "github.com/koordinator-sh/koordinator/apis/scheduling/v1alpha1"
)

const (
	RecoveryEvictableLabel  = "hami.io/recovery-evictable"
	AnnotationNode          = "hami.io/target-node"
	AnnotationUUIDs         = "hami.io/target-gpu-uuids"
	AnnotationPlacementOnly = "hami.io/placement-only"
	AnnotationJobUID        = "hami.io/placement-job-uid"
	AnnotationReservation   = "hami.io/gpu-reservation"
	AnnotationUseUUID       = "nvidia.com/use-gpuuuid"
	AnnotationAllocated     = "hami.io/vgpu-devices-allocated"
	ManagedNamespaceLabel   = "hami.io/managed-placement"
	SchedulingGate          = "hami.io/placement"
	SchedulerName           = "hami-scheduler"
	WebhookPath             = "/mutate-hami-placement"
)

const (
	ReadyCondition       sev1alpha1.PodMigrationJobConditionType = "HamiPlacementReady"
	LockCondition        sev1alpha1.PodMigrationJobConditionType = "HamiPlacementLock"
	ArbitrationCondition sev1alpha1.PodMigrationJobConditionType = "HamiArbitration"
)

func PassedArbitration(job *sev1alpha1.PodMigrationJob) bool {
	if job.Spec.PodRef == nil || job.Spec.PodRef.UID == "" {
		return false
	}
	return slices.ContainsFunc(job.Status.Conditions, func(c sev1alpha1.PodMigrationJobCondition) bool {
		return c.Type == ArbitrationCondition && c.Status == sev1alpha1.PodMigrationJobConditionStatusTrue &&
			c.Message == string(job.Spec.PodRef.UID)
	})
}

type Placement struct {
	SourceName  string      `json:"sourceName"`
	JobUID      types.UID   `json:"jobUID"`
	SourceUID   types.UID   `json:"sourceUID"`
	OwnerUID    types.UID   `json:"ownerUID"`
	SiblingUIDs []types.UID `json:"siblingUIDs,omitempty"`
	NodeName    string      `json:"nodeName"`
	GPUUUIDs    []string    `json:"gpuUUIDs"`
	Reservation string      `json:"reservation,omitempty"`
	ExpiresAt   metav1.Time `json:"expiresAt"`
}

func Requested(annotations map[string]string) bool {
	for _, key := range []string{AnnotationNode, AnnotationUUIDs, AnnotationPlacementOnly} {
		if _, found := annotations[key]; found {
			return true
		}
	}
	return false
}

func LockName(ownerUID types.UID) string {
	return "hami-placement-" + string(ownerUID)
}

func (p Placement) ConfigMap(namespace string, owner metav1.OwnerReference) (*corev1.ConfigMap, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	immutable := true
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: LockName(p.OwnerUID), Namespace: namespace,
			Labels:          map[string]string{AnnotationJobUID: string(p.JobUID)},
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Immutable: &immutable,
		Data:      map[string]string{"placement": string(data)},
	}, nil
}

func FromConfigMap(cm *corev1.ConfigMap) (Placement, error) {
	var p Placement
	err := json.Unmarshal([]byte(cm.Data["placement"]), &p)
	if err == nil && (p.JobUID == "" || p.OwnerUID == "" || p.SourceUID == "" || p.SourceName == "" || p.NodeName == "" ||
		len(p.GPUUUIDs) == 0 || p.ExpiresAt.IsZero() || cm.Name != LockName(p.OwnerUID) ||
		cm.Immutable == nil || !*cm.Immutable ||
		cm.Labels[AnnotationJobUID] != string(p.JobUID) || len(cm.OwnerReferences) != 1 ||
		cm.OwnerReferences[0].UID != p.JobUID || cm.OwnerReferences[0].Kind != "PodMigrationJob" ||
		cm.OwnerReferences[0].APIVersion != "scheduling.koordinator.sh/v1alpha1") {
		err = fmt.Errorf("invalid HAMi placement ConfigMap %s/%s", cm.Namespace, cm.Name)
	}
	return p, err
}

func ReplicaSetOwner(pod *corev1.Pod) *metav1.OwnerReference {
	owner := metav1.GetControllerOfNoCopy(pod)
	if owner == nil || owner.APIVersion != "apps/v1" || owner.Kind != "ReplicaSet" || owner.UID == "" {
		return nil
	}
	return owner
}

func SourceActive(pod *corev1.Pod, uid types.UID) bool {
	return pod.UID == uid && pod.DeletionTimestamp.IsZero() &&
		pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded
}

func HasGate(pod *corev1.Pod) bool {
	return slices.Contains(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: SchedulingGate})
}

func PinnedToNode(pod *corev1.Pod, nodeName string) bool {
	affinity := pod.Spec.Affinity
	if affinity == nil || affinity.NodeAffinity == nil {
		return false
	}
	selector := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if selector == nil {
		return false
	}
	pinned := false
	for _, term := range selector.NodeSelectorTerms {
		if len(term.MatchFields)+len(term.MatchExpressions) == 0 {
			continue
		}
		if !slices.ContainsFunc(term.MatchFields, func(req corev1.NodeSelectorRequirement) bool {
			return req.Key == "metadata.name" && req.Operator == corev1.NodeSelectorOpIn &&
				len(req.Values) == 1 && req.Values[0] == nodeName
		}) {
			return false
		}
		pinned = true
	}
	return pinned
}

func (p Placement) Apply(pod *corev1.Pod) error {
	if pod.Spec.SchedulerName != SchedulerName || pod.Spec.NodeName != "" {
		return fmt.Errorf("replacement must use %s and have no preassigned node", SchedulerName)
	}
	// RPM may already have populated the pool's full GPU whitelist. Narrow it
	// without adding devices to an existing whitelist or touching its revisions.
	if allowed := strings.TrimSpace(pod.Annotations[AnnotationUseUUID]); allowed != "" {
		existing := strings.Split(allowed, ",")
		for _, uuid := range p.GPUUUIDs {
			if !slices.ContainsFunc(existing, func(id string) bool { return strings.TrimSpace(id) == uuid }) {
				return fmt.Errorf("target GPU %s is outside the Pod's %s whitelist", uuid, AnnotationUseUUID)
			}
		}
	}
	for _, uuid := range strings.Split(pod.Annotations["nvidia.com/nouse-gpuuuid"], ",") {
		uuid = strings.TrimSpace(uuid)
		if slices.Contains(p.GPUUUIDs, uuid) {
			return fmt.Errorf("target GPU %s is excluded by the Pod", uuid)
		}
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	for key, value := range map[string]string{
		AnnotationJobUID: string(p.JobUID), AnnotationReservation: p.Reservation,
	} {
		if current := pod.Annotations[key]; current != "" && current != value {
			return fmt.Errorf("replacement has conflicting annotation %s", key)
		}
		if value != "" {
			pod.Annotations[key] = value
		}
	}
	pod.Annotations[AnnotationUseUUID] = strings.Join(p.GPUUUIDs, ",")
	if HasGate(pod) {
		if !PinnedToNode(pod, p.NodeName) {
			return fmt.Errorf("gated replacement is missing its target node affinity")
		}
		return nil
	}
	pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: SchedulingGate})

	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	affinity := pod.Spec.Affinity.NodeAffinity
	requirement := corev1.NodeSelectorRequirement{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{p.NodeName}}
	if affinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		affinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{requirement}}},
		}
		return nil
	}
	terms := affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	for i := range terms {
		// An empty Kubernetes selector term matches no nodes.
		if len(terms[i].MatchFields)+len(terms[i].MatchExpressions) > 0 {
			terms[i].MatchFields = append(terms[i].MatchFields, requirement)
		}
	}
	return nil
}

type Webhook struct {
	Reader  client.Reader
	Decoder admission.Decoder
}

func (w Webhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	if req.Operation != admissionv1.Create {
		return admission.Allowed("Pod CREATE only")
	}
	pod := &corev1.Pod{}
	if err := w.Decoder.Decode(req, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	owner := ReplicaSetOwner(pod)
	if owner == nil {
		return admission.Allowed("no ReplicaSet owner")
	}
	cm := &corev1.ConfigMap{}
	err := w.Reader.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: LockName(owner.UID)}, cm)
	if apierrors.IsNotFound(err) {
		return admission.Allowed("no HAMi placement")
	}
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	// A namespaced ConfigMap alone cannot authorize placement. The cluster
	// migration controller records its API-assigned UID in the PMJ status.
	if len(cm.OwnerReferences) != 1 || cm.OwnerReferences[0].Kind != "PodMigrationJob" {
		return admission.Allowed("no controller-owned HAMi placement")
	}
	job := &sev1alpha1.PodMigrationJob{}
	if err := w.Reader.Get(ctx, client.ObjectKey{Name: cm.OwnerReferences[0].Name}, job); err != nil {
		if apierrors.IsNotFound(err) {
			return admission.Allowed("HAMi placement job was retired")
		}
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if job.UID != cm.OwnerReferences[0].UID || job.Status.Phase == sev1alpha1.PodMigrationJobSucceeded ||
		!slices.ContainsFunc(job.Status.Conditions, func(c sev1alpha1.PodMigrationJobCondition) bool {
			return c.Type == LockCondition && c.Status == sev1alpha1.PodMigrationJobConditionStatusTrue && c.Message == string(cm.UID)
		}) {
		return admission.Allowed("no active controller-owned HAMi placement")
	}
	p, err := FromConfigMap(cm)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if req.DryRun == nil || !*req.DryRun {
		// Until the source departs, a ReplicaSet create repairs some other
		// replica or scales the workload. It is not this migration's Pod.
		if !slices.ContainsFunc(job.Status.Conditions, func(c sev1alpha1.PodMigrationJobCondition) bool {
			return c.Type == ReadyCondition && c.Status == sev1alpha1.PodMigrationJobConditionStatusTrue
		}) {
			return admission.Allowed("HAMi placement has not been armed")
		}
		source := &corev1.Pod{}
		err := w.Reader.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: p.SourceName}, source)
		if client.IgnoreNotFound(err) != nil {
			return admission.Errored(http.StatusInternalServerError, err)
		}
		if SourceActive(source, p.SourceUID) {
			return admission.Allowed("HAMi placement source is still active")
		}
		// A client can supply owner references. Only the actual controller can
		// create a replacement. Dry-run probes cannot persist a Pod.
		if req.UserInfo.Username != "system:serviceaccount:kube-system:replicaset-controller" &&
			req.UserInfo.Username != "system:kube-controller-manager" {
			return admission.Denied("HAMi replacement must be created by the ReplicaSet controller")
		}
	}
	if err := p.Apply(pod); err != nil {
		return admission.Denied(err.Error())
	}
	raw, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, raw)
}
