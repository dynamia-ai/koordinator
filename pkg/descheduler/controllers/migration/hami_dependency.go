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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sev1alpha1 "github.com/koordinator-sh/koordinator/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/hamiplacement"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/util"
)

const (
	hamiDependentEvictionAnnotation = "hami.io/dependent-eviction-intent"
	hamiEvictionFenceAnnotation     = "hami.io/eviction-attempt-fence"
	hamiEvictionDependencyCondition = "HamiEvictionDependencyCheckpoint"
)

type hamiDependencyCheckpoint struct {
	Destination corev1.ObjectReference `json:"destination"`
	Target      corev1.ObjectReference `json:"target"`
	Marker      string                 `json:"marker"`
}

func (r *Reconciler) readHamiReservation(ctx context.Context, namespace, name string) (*unstructured.Unstructured, error) {
	reservation := &unstructured.Unstructured{}
	reservation.SetGroupVersionKind(gpuReservationGVK)
	if err := r.hamiReader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, reservation); err != nil {
		return nil, err
	}
	return reservation, nil
}

func (r *Reconciler) hamiEvictionDependency(ctx context.Context, source *corev1.Pod, ref *hamiplacement.ReservationReference, expectedMarker string) (*unstructured.Unstructured, error) {
	target, err := r.readHamiReservation(ctx, source.Namespace, ref.Name)
	if err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	occupants, _, err := unstructured.NestedStringSlice(target.Object, "spec", "allowedOccupantUIDs")
	if err != nil {
		return nil, err
	}
	node, _, err := unstructured.NestedString(target.Object, "spec", "nodeName")
	if err != nil {
		return nil, err
	}
	consumer, _, err := unstructured.NestedMap(target.Object, "spec", "consumerRef")
	if err != nil {
		return nil, err
	}
	phase := reservationPhase(target)
	if target.GetUID() != ref.UID || target.GetResourceVersion() == "" || node != source.Spec.NodeName ||
		!slices.Contains(occupants, string(source.UID)) || consumer != nil || !currentHamiProtection(target, r.clock.Now()) ||
		(phase != "Protecting" && phase != "Reserved") || target.GetAnnotations()[hamiDependentEvictionAnnotation] != expectedMarker {
		return nil, nil
	}
	return target, nil
}

func hamiReservationReference(reservation *unstructured.Unstructured) corev1.ObjectReference {
	return corev1.ObjectReference{
		Namespace: reservation.GetNamespace(), Name: reservation.GetName(),
		UID: reservation.GetUID(), ResourceVersion: reservation.GetResourceVersion(),
	}
}

func (r *Reconciler) checkpointHamiDependency(ctx context.Context, job *sev1alpha1.PodMigrationJob, reservation, target *unstructured.Unstructured, marker string) error {
	payload, err := json.Marshal(hamiDependencyCheckpoint{
		Destination: hamiReservationReference(reservation), Target: hamiReservationReference(target), Marker: marker,
	})
	if err != nil {
		return err
	}
	util.UpdateCondition(&job.Status, &sev1alpha1.PodMigrationJobCondition{
		Type: hamiEvictionDependencyCondition, Status: sev1alpha1.PodMigrationJobConditionStatusTrue,
		Reason: "EvictionReservationsChecked", Message: string(payload),
	})
	return r.Status().Update(ctx, job)
}

func (r *Reconciler) recoverHamiDependentEviction(ctx context.Context, job *sev1alpha1.PodMigrationJob, message string) error {
	var checkpoint hamiDependencyCheckpoint
	var source hamiSourceEviction
	if err := json.Unmarshal([]byte(message), &checkpoint); err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(checkpoint.Marker), &source); err != nil {
		return err
	}
	if source.Name == "" || source.UID == "" || source.ResourceVersion == "" {
		return fmt.Errorf("PodMigrationJob %s has an invalid dependency checkpoint", job.Name)
	}
	attempt := string(checkpoint.Destination.UID) + "/" + checkpoint.Marker
	_, started := util.GetCondition(&job.Status, hamiEvictionStartCondition)
	_, outcome := util.GetCondition(&job.Status, hamiEvictionOutcomeCondition)
	recorded := outcome != nil && outcome.Status == sev1alpha1.PodMigrationJobConditionStatusTrue && outcome.Message == attempt
	submissionAuthorized := started != nil && started.Status == sev1alpha1.PodMigrationJobConditionStatusTrue && started.Message == attempt
	if !recorded && !submissionAuthorized {
		// Race the submitter's start checkpoint using the same PMJ version.
		// Fencing the reservation versions below then closes late pin writes.
		eviction := &sev1alpha1.PodMigrationJobCondition{
			Type: sev1alpha1.PodMigrationJobConditionEviction, Status: sev1alpha1.PodMigrationJobConditionStatusFalse,
			Reason: "HamiEvictionDeferred", Message: "Recovered a dependency before eviction submission",
			LastProbeTime: metav1.NewTime(r.clock.Now()),
		}
		if err := r.persistHamiEvictionOutcome(ctx, job, eviction, attempt); err != nil {
			return err
		}
		outcome, recorded = eviction, true
	}
	rejected := recorded && (outcome.Reason == "HamiEvictionDeferred" || outcome.Reason == "HamiEvictionBlocked")
	if !rejected {
		pod := &corev1.Pod{}
		err := r.hamiReader.Get(ctx, client.ObjectKey{Namespace: checkpoint.Target.Namespace, Name: source.Name}, pod)
		if client.IgnoreNotFound(err) != nil {
			return err
		}
		if err == nil && pod.UID == source.UID {
			return nil
		}
	}
	if err := r.settleHamiDependencyPin(ctx, checkpoint.Destination, hamiSourceEvictionAnnotation, checkpoint.Marker); err != nil {
		return err
	}
	if err := r.settleHamiDependencyPin(ctx, checkpoint.Target, hamiDependentEvictionAnnotation, checkpoint.Marker); err != nil {
		return err
	}
	return r.finishHamiEvictionRecovery(ctx, job)
}

func (r *Reconciler) settleHamiDependencyPin(ctx context.Context, ref corev1.ObjectReference, annotation, marker string) error {
	reservation, err := r.readHamiReservation(ctx, ref.Namespace, ref.Name)
	if err != nil {
		return client.IgnoreNotFound(err)
	}
	if reservation.GetUID() != ref.UID {
		return nil
	}
	annotations := reservation.GetAnnotations()
	if annotations[annotation] == marker {
		return r.clearHamiEviction(ctx, reservation, annotation, marker)
	}
	if reservation.GetResourceVersion() != ref.ResourceVersion {
		return nil
	}
	if annotations == nil {
		annotations = make(map[string]string)
	}
	// A submitter that has read but not yet updated this version must lose
	// the same CAS even if neither reservation shows a pin yet.
	annotations[hamiEvictionFenceAnnotation] = ref.ResourceVersion
	reservation.SetAnnotations(annotations)
	return r.Update(ctx, reservation)
}
