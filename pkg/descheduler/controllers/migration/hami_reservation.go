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

package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sev1alpha1 "github.com/koordinator-sh/koordinator/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/hamiplacement"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/util"
)

const (
	gpuReservationProtectionFinalizer = "resource.hami.io/gpu-reservation-protection"
	hamiSourceEvictionAnnotation      = "hami.io/eviction-intent"
	hamiEvictionRecoveryFinalizer     = "hami.io/source-eviction-recovery"
	hamiEvictionOutcomeCondition      = "HamiEvictionOutcomeRecorded"
	hamiEvictionStartCondition        = "HamiEvictionAttemptStarted"
	hamiEvictionCheckpointPrefix      = "pmj-checkpoint/"
)

type hamiSourceEviction struct {
	Name            string    `json:"name"`
	UID             types.UID `json:"uid"`
	ResourceVersion string    `json:"resourceVersion"`
	Attempt         string    `json:"attempt"`
}

var gpuReservationGVK = schema.GroupVersionKind{Group: "resource.hami.io", Version: "v1alpha1", Kind: "GPUReservation"}

func gpuReservationName(job *sev1alpha1.PodMigrationJob) string {
	return "pmj-" + string(job.UID)
}

func gpuReservationObject(job *sev1alpha1.PodMigrationJob) *unstructured.Unstructured {
	reservation := &unstructured.Unstructured{}
	reservation.SetGroupVersionKind(gpuReservationGVK)
	reservation.SetName(gpuReservationName(job))
	reservation.SetNamespace(job.Spec.PodRef.Namespace)
	return reservation
}

func (r *Reconciler) hamiReservation(ctx context.Context, job *sev1alpha1.PodMigrationJob, placement hamiplacement.Placement, allowCreate bool) (*unstructured.Unstructured, error) {
	reservation := gpuReservationObject(job)
	err := r.hamiReader.Get(ctx, client.ObjectKeyFromObject(reservation), reservation)
	if apierrors.IsNotFound(err) && allowCreate {
		reservation.SetOwnerReferences([]metav1.OwnerReference{hamiJobOwner(job)})
		reservation.Object["spec"] = map[string]any{
			"nodeName":  placement.NodeName,
			"expiresAt": placement.ExpiresAt.Time.UTC().Format(time.RFC3339),
		}
		if err := unstructured.SetNestedStringSlice(reservation.Object, placement.GPUUUIDs, "spec", "gpuUUIDs"); err != nil {
			return nil, err
		}
		if len(placement.AllowedOccupantUIDs) > 0 {
			if err := unstructured.SetNestedStringSlice(reservation.Object, placement.AllowedOccupantUIDs, "spec", "allowedOccupantUIDs"); err != nil {
				return nil, err
			}
		}
		return reservation, r.Create(ctx, reservation)
	}
	if err != nil {
		return nil, err
	}
	node, _, _ := unstructured.NestedString(reservation.Object, "spec", "nodeName")
	uuids, _, _ := unstructured.NestedStringSlice(reservation.Object, "spec", "gpuUUIDs")
	expiresAt, _, _ := unstructured.NestedString(reservation.Object, "spec", "expiresAt")
	deadline, _ := time.Parse(time.RFC3339, expiresAt)
	occupants, _, err := unstructured.NestedStringSlice(reservation.Object, "spec", "allowedOccupantUIDs")
	slices.Sort(uuids)
	if err != nil || !ownedByHamiJob(reservation, job) || node != placement.NodeName ||
		!slices.Equal(uuids, placement.GPUUUIDs) || !deadline.Equal(placement.ExpiresAt.Time) ||
		!slices.Equal(occupants, placement.AllowedOccupantUIDs) ||
		(len(placement.AllowedOccupantUIDs) > 0 && job.Annotations[hamiplacement.AnnotationTargetUID] != "" &&
			job.Annotations[hamiplacement.AnnotationTargetUID] != string(reservation.GetUID())) {
		return nil, fmt.Errorf("GPUReservation %s/%s does not match migration %s", reservation.GetNamespace(), reservation.GetName(), job.Name)
	}
	return reservation, nil
}

func reservationPhase(reservation *unstructured.Unstructured) string {
	phase, _, _ := unstructured.NestedString(reservation.Object, "status", "phase")
	return phase
}

func reservationProtected(reservation *unstructured.Unstructured) bool {
	return slices.Contains(reservation.GetFinalizers(), gpuReservationProtectionFinalizer)
}

func usableHamiReservation(reservation *unstructured.Unstructured, now time.Time) bool {
	annotations := reservation.GetAnnotations()
	_, hasEvictionIntent := annotations[hamiSourceEvictionAnnotation]
	_, hasDependentEviction := annotations[hamiDependentEvictionAnnotation]
	return !hasEvictionIntent && !hasDependentEviction && currentHamiProtection(reservation, now) && reservationPhase(reservation) == "Reserved"
}

func currentHamiProtection(reservation *unstructured.Unstructured, now time.Time) bool {
	expiresAt, _, _ := unstructured.NestedString(reservation.Object, "spec", "expiresAt")
	deadline, err := time.Parse(time.RFC3339, expiresAt)
	return err == nil && now.Before(deadline) && reservation.GetDeletionTimestamp() == nil && reservationProtected(reservation)
}

func (r *Reconciler) reportHamiTargetProtection(ctx context.Context, job *sev1alpha1.PodMigrationJob, reservation *unstructured.Unstructured, armed bool) (bool, error) {
	uid := string(reservation.GetUID())
	if uid == "" {
		return true, fmt.Errorf("GPUReservation %s/%s has no UID", reservation.GetNamespace(), reservation.GetName())
	}
	if job.Annotations[hamiplacement.AnnotationTargetUID] == "" {
		job.Annotations[hamiplacement.AnnotationTargetUID] = uid
		return true, r.Update(ctx, job)
	}
	_, previous := util.GetCondition(&job.Status, hamiplacement.TargetProtectedCondition)
	phase := reservationPhase(reservation)
	if armed && (phase == "Protecting" || phase == "Reserved") && currentHamiProtection(reservation, r.clock.Now()) {
		if util.UpdateCondition(&job.Status, &sev1alpha1.PodMigrationJobCondition{
			Type: hamiplacement.TargetProtectedCondition, Status: sev1alpha1.PodMigrationJobConditionStatusTrue,
			Reason: "HamiSchedulerProtected", Message: uid,
		}) {
			return true, r.Status().Update(ctx, job)
		}
	} else if previous != nil && previous.Status == sev1alpha1.PodMigrationJobConditionStatusTrue {
		util.UpdateCondition(&job.Status, &sev1alpha1.PodMigrationJobCondition{
			Type: hamiplacement.TargetProtectedCondition, Status: sev1alpha1.PodMigrationJobConditionStatusFalse,
			Reason: "HamiSchedulerProtectionEnded", Message: uid,
		})
		return true, r.Status().Update(ctx, job)
	}
	return false, nil
}

// commitHamiEviction takes the scheduler's hold on the same reservation
// version that was checked before reading the PMJ. Cancellation and scheduler
// retirement must win this same update race to release an uncommitted grant.
func hamiEvictionMarker(source *corev1.Pod, attempt string) (string, error) {
	payload, err := json.Marshal(hamiSourceEviction{
		Name: source.Name, UID: source.UID, ResourceVersion: source.ResourceVersion, Attempt: attempt,
	})
	return string(payload), err
}

func (r *Reconciler) commitHamiEviction(ctx context.Context, reservation *unstructured.Unstructured, annotation, marker string) error {
	annotations := reservation.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[annotation] = marker
	reservation.SetAnnotations(annotations)
	// Update contains both the live UID (immutable) and resourceVersion.
	return r.Update(ctx, reservation)
}

// clearHamiEviction must only follow a definitive Eviction result or a
// locally abandoned submission. A different attempt or reservation UID
// must survive retries.
func (r *Reconciler) clearHamiEviction(ctx context.Context, reservation *unstructured.Unstructured, annotation, marker string) error {
	if reservation == nil || marker == "" {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &unstructured.Unstructured{}
		current.SetGroupVersionKind(gpuReservationGVK)
		if err := r.hamiReader.Get(ctx, client.ObjectKeyFromObject(reservation), current); err != nil {
			return client.IgnoreNotFound(err)
		}
		annotations := current.GetAnnotations()
		if current.GetUID() != reservation.GetUID() || annotations[annotation] != marker {
			return nil
		}
		delete(annotations, annotation)
		current.SetAnnotations(annotations)
		return r.Update(ctx, current)
	})
}

func (r *Reconciler) startHamiEviction(ctx context.Context, job *sev1alpha1.PodMigrationJob, attempt string) error {
	util.UpdateCondition(&job.Status, &sev1alpha1.PodMigrationJobCondition{
		Type: hamiEvictionStartCondition, Status: sev1alpha1.PodMigrationJobConditionStatusTrue,
		Reason: "EvictionSubmissionAuthorized", Message: attempt,
	})
	// This write uses the live PMJ version approved immediately before
	// submission. Recovery uses that same version to fence an unsent attempt.
	return r.Status().Update(ctx, job)
}

// A known API response is recorded before releasing the intent. Persist the
// exact reservation UID and marker so a new reconciler can finish the release.
func (r *Reconciler) recordHamiEvictionOutcome(ctx context.Context, job *sev1alpha1.PodMigrationJob, eviction *sev1alpha1.PodMigrationJobCondition, attempt string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &sev1alpha1.PodMigrationJob{}
		if err := r.hamiReader.Get(ctx, client.ObjectKeyFromObject(job), current); err != nil {
			return err
		}
		if current.UID != job.UID {
			return fmt.Errorf("PodMigrationJob %s changed identity before recording eviction outcome", job.Name)
		}
		if err := r.persistHamiEvictionOutcome(ctx, current, eviction, attempt); err != nil {
			return err
		}
		*job = *current
		return nil
	})
}

func (r *Reconciler) persistHamiEvictionOutcome(ctx context.Context, job *sev1alpha1.PodMigrationJob, eviction *sev1alpha1.PodMigrationJobCondition, attempt string) error {
	util.UpdateCondition(&job.Status, eviction)
	util.UpdateCondition(&job.Status, &sev1alpha1.PodMigrationJobCondition{
		Type: hamiEvictionOutcomeCondition, Status: sev1alpha1.PodMigrationJobConditionStatusTrue,
		Reason: eviction.Reason, Message: attempt,
	})
	switch job.Status.Phase {
	case "", sev1alpha1.PodMigrationJobPending, sev1alpha1.PodMigrationJobRunning:
		job.Status.Status = string(eviction.Type)
		job.Status.Reason = eviction.Reason
		job.Status.Message = eviction.Message
	}
	return r.Status().Update(ctx, job)
}

func (r *Reconciler) recoverHamiEviction(ctx context.Context, job *sev1alpha1.PodMigrationJob) error {
	if _, dependency := util.GetCondition(&job.Status, hamiEvictionDependencyCondition); dependency != nil {
		return r.recoverHamiDependentEviction(ctx, job, dependency.Message)
	}
	if job.Spec.PodRef == nil || job.Spec.PodRef.Namespace == "" {
		return fmt.Errorf("PodMigrationJob %s lost its source namespace during eviction recovery", job.Name)
	}
	reservation := gpuReservationObject(job)
	err := r.hamiReader.Get(ctx, client.ObjectKeyFromObject(reservation), reservation)
	if client.IgnoreNotFound(err) != nil {
		return err
	}
	if err == nil && ownedByHamiJob(reservation, job) {
		if marker, held := reservation.GetAnnotations()[hamiSourceEvictionAnnotation]; held {
			return r.recoverHamiEvictionAttempt(ctx, job, reservation, marker)
		}
	}
	return r.finishHamiEvictionRecovery(ctx, job)
}

func (r *Reconciler) finishHamiEvictionRecovery(ctx context.Context, job *sev1alpha1.PodMigrationJob) error {
	recovered := job.DeepCopy()
	recovered.Finalizers = slices.DeleteFunc(recovered.Finalizers, func(value string) bool { return value == hamiEvictionRecoveryFinalizer })
	if err := r.Update(ctx, recovered); err != nil {
		return err
	}
	*job = *recovered
	return nil
}

func (r *Reconciler) recoverHamiEvictionAttempt(ctx context.Context, job *sev1alpha1.PodMigrationJob, reservation *unstructured.Unstructured, marker string) error {
	attempt := string(reservation.GetUID()) + "/" + marker
	_, recorded := util.GetCondition(&job.Status, hamiEvictionOutcomeCondition)
	if recorded != nil && recorded.Status == sev1alpha1.PodMigrationJobConditionStatusTrue && recorded.Message == attempt {
		return r.clearHamiEviction(ctx, reservation, hamiSourceEvictionAnnotation, marker)
	}
	var source hamiSourceEviction
	if err := json.Unmarshal([]byte(marker), &source); err != nil || source.Name == "" || source.UID == "" || source.ResourceVersion == "" {
		return fmt.Errorf("GPUReservation %s/%s has an invalid eviction intent", reservation.GetNamespace(), reservation.GetName())
	}
	_, started := util.GetCondition(&job.Status, hamiEvictionStartCondition)
	if strings.HasPrefix(source.Attempt, hamiEvictionCheckpointPrefix) &&
		(started == nil || started.Status != sev1alpha1.PodMigrationJobConditionStatusTrue || started.Message != attempt) {
		// Commit the decision against the version where the start checkpoint
		// was absent. An in-flight submitter must win that same PMJ CAS.
		recovered := job.DeepCopy()
		eviction := &sev1alpha1.PodMigrationJobCondition{
			Type: sev1alpha1.PodMigrationJobConditionEviction, Status: sev1alpha1.PodMigrationJobConditionStatusFalse,
			Reason: "HamiEvictionDeferred", Message: "Recovered a reservation intent before eviction submission",
			LastProbeTime: metav1.NewTime(r.clock.Now()),
		}
		if err := r.persistHamiEvictionOutcome(ctx, recovered, eviction, attempt); err != nil {
			return err
		}
		*job = *recovered
		return r.clearHamiEviction(ctx, reservation, hamiSourceEvictionAnnotation, marker)
	}
	pod := &corev1.Pod{}
	err := r.hamiReader.Get(ctx, client.ObjectKey{Namespace: reservation.GetNamespace(), Name: source.Name}, pod)
	if client.IgnoreNotFound(err) != nil {
		return err
	}
	if err == nil && pod.UID == source.UID && pod.DeletionTimestamp.IsZero() {
		return nil
	}
	return r.clearHamiEviction(ctx, reservation, hamiSourceEvictionAnnotation, marker)
}

func hamiReservationConsumerMatches(reservation *unstructured.Unstructured, pod *corev1.Pod) bool {
	name, _, _ := unstructured.NestedString(reservation.Object, "spec", "consumerRef", "name")
	uid, _, _ := unstructured.NestedString(reservation.Object, "spec", "consumerRef", "uid")
	return name == pod.Name && uid == string(pod.UID)
}

func (r *Reconciler) assignHamiReservation(ctx context.Context, reservation *unstructured.Unstructured, pod *corev1.Pod) error {
	name, _, _ := unstructured.NestedString(reservation.Object, "spec", "consumerRef", "name")
	uid, _, _ := unstructured.NestedString(reservation.Object, "spec", "consumerRef", "uid")
	if name == pod.Name && uid == string(pod.UID) {
		return nil
	}
	if name != "" || uid != "" {
		return fmt.Errorf("GPUReservation %s/%s cannot be assigned to Pod %s/%s UID %s", reservation.GetNamespace(), reservation.GetName(), pod.Namespace, pod.Name, pod.UID)
	}
	consumer := map[string]any{"name": pod.Name, "uid": string(pod.UID)}
	if err := unstructured.SetNestedMap(reservation.Object, consumer, "spec", "consumerRef"); err != nil {
		return err
	}
	// Update carries the resourceVersion read with the empty consumerRef.
	return r.Update(ctx, reservation)
}

func (r *Reconciler) releaseHamiReservation(ctx context.Context, job *sev1alpha1.PodMigrationJob, requireUnassigned bool) error {
	reservation := gpuReservationObject(job)
	if err := r.hamiReader.Get(ctx, client.ObjectKeyFromObject(reservation), reservation); err != nil {
		if meta.IsNoMatchError(err) {
			return nil
		}
		return client.IgnoreNotFound(err)
	}
	if !ownedByHamiJob(reservation, job) || reservation.GetDeletionTimestamp() != nil {
		return nil
	}
	consumer, _, err := unstructured.NestedMap(reservation.Object, "spec", "consumerRef")
	if err != nil {
		return err
	}
	if consumer != nil {
		if requireUnassigned {
			return fmt.Errorf("GPUReservation %s/%s may already have a consumer", reservation.GetNamespace(), reservation.GetName())
		}
		if reservationPhase(reservation) == "Reserved" && !reservationProtected(reservation) {
			return fmt.Errorf("GPUReservation %s/%s lacks scheduler protection for cancellation", reservation.GetNamespace(), reservation.GetName())
		}
	}
	// A concurrent consumerRef update changes the resourceVersion. A protected
	// grant is released only after the scheduler resolves Bind versus rollback.
	uid, version := reservation.GetUID(), reservation.GetResourceVersion()
	return client.IgnoreNotFound(r.Delete(ctx, reservation, client.Preconditions{UID: &uid, ResourceVersion: &version}))
}
