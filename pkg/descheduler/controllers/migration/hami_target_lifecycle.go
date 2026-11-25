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
	"slices"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sev1alpha1 "github.com/koordinator-sh/koordinator/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/hamiplacement"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/util"
)

const (
	hamiTargetOwnerFinalizer     = "hami.io/target-reservation-cleanup"
	hamiTargetNamespace          = "hami.io/target-reservation-namespace"
	hamiTargetOwnerAnnotation    = "hami.io/target-reservation-owner"
	hamiTargetRetiringAnnotation = "hami.io/target-reservation-retiring"
)

func hamiOwnsTarget(job *sev1alpha1.PodMigrationJob, reservation *unstructured.Unstructured) bool {
	return ownedByHamiJob(reservation, job) || (reservation.GetName() == gpuReservationName(job) &&
		reservation.GetNamespace() == job.Annotations[hamiTargetNamespace] &&
		reservation.GetAnnotations()[hamiTargetOwnerAnnotation] == job.Name)
}

func (r *Reconciler) ensureHamiTargetCleanup(ctx context.Context, job *sev1alpha1.PodMigrationJob) error {
	if slices.Contains(job.Finalizers, hamiTargetOwnerFinalizer) {
		return nil
	}
	job.Finalizers = append(job.Finalizers, hamiTargetOwnerFinalizer)
	job.Annotations[hamiTargetNamespace] = job.Spec.PodRef.Namespace
	return r.Update(ctx, job)
}

func (r *Reconciler) manageHamiTarget(ctx context.Context, job *sev1alpha1.PodMigrationJob, target *unstructured.Unstructured) (bool, error) {
	if target.GetAnnotations()[hamiTargetOwnerAnnotation] == job.Name {
		return true, nil
	}
	live := &sev1alpha1.PodMigrationJob{}
	if err := r.hamiReader.Get(ctx, client.ObjectKeyFromObject(job), live); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if live.GetUID() != job.GetUID() || live.ResourceVersion != job.ResourceVersion || !live.DeletionTimestamp.IsZero() ||
		!hamiJobActive(live) || live.Annotations[hamiTargetNamespace] != target.GetNamespace() ||
		!slices.Contains(live.Finalizers, hamiTargetOwnerFinalizer) || target.GetDeletionTimestamp() != nil {
		return false, nil
	}

	// Keep the ownerRef at creation so a late CREATE remains GC-managed.
	// Detach before admitting pins: foreground GC can delete dependents even
	// while the owner's cleanup finalizer is present.
	annotations := target.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[hamiTargetOwnerAnnotation] = job.Name
	target.SetAnnotations(annotations)
	owner := hamiJobOwner(job)
	target.SetOwnerReferences(slices.DeleteFunc(target.GetOwnerReferences(), func(ref metav1.OwnerReference) bool {
		return ref.UID == owner.UID && ref.Name == owner.Name && ref.Kind == owner.Kind && ref.APIVersion == owner.APIVersion
	}))
	if err := r.Update(ctx, target); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Reconciler) activeHamiTargetOwner(ctx context.Context, target *unstructured.Unstructured) (bool, error) {
	name := target.GetAnnotations()[hamiTargetOwnerAnnotation]
	if name == "" || len(target.GetOwnerReferences()) != 0 {
		return false, nil
	}
	owner := &sev1alpha1.PodMigrationJob{}
	if err := r.hamiReader.Get(ctx, client.ObjectKey{Name: name}, owner); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	_, protected := util.GetCondition(&owner.Status, hamiplacement.TargetProtectedCondition)
	return hamiOwnsTarget(owner, target) && owner.Annotations[hamiplacement.AnnotationTargetUID] == string(target.GetUID()) &&
		protected != nil && protected.Status == sev1alpha1.PodMigrationJobConditionStatusTrue && protected.Message == string(target.GetUID()) &&
		owner.DeletionTimestamp.IsZero() && !owner.Spec.Paused && hamiJobActive(owner) &&
		slices.Contains(owner.Finalizers, hamiTargetOwnerFinalizer), nil
}

func (r *Reconciler) retireHamiTarget(ctx context.Context, job *sev1alpha1.PodMigrationJob) (bool, error) {
	namespace := job.Annotations[hamiTargetNamespace]
	if namespace == "" && job.Spec.PodRef != nil {
		namespace = job.Spec.PodRef.Namespace
	}
	if namespace == "" {
		return false, nil
	}
	target, err := r.readHamiReservation(ctx, namespace, gpuReservationName(job))
	if client.IgnoreNotFound(err) != nil && !meta.IsNoMatchError(err) {
		return false, err
	}
	reportedUID := job.Annotations[hamiplacement.AnnotationTargetUID]
	owned := target != nil && hamiOwnsTarget(job, target) && (reportedUID == "" || reportedUID == string(target.GetUID()))
	if owned {
		annotations := target.GetAnnotations()
		if annotations[hamiTargetRetiringAnnotation] != "true" {
			if annotations == nil {
				annotations = make(map[string]string)
			}
			annotations[hamiTargetRetiringAnnotation] = "true"
			target.SetAnnotations(annotations)
			// The same resourceVersion arbitrates a late pin and retirement.
			if err := r.Update(ctx, target); err != nil {
				return true, client.IgnoreNotFound(err)
			}
		}
		if _, pinned := target.GetAnnotations()[hamiDependentEvictionAnnotation]; pinned {
			return true, nil
		}
	}

	_, protected := util.GetCondition(&job.Status, hamiplacement.TargetProtectedCondition)
	if protected != nil && protected.Status == sev1alpha1.PodMigrationJobConditionStatusTrue {
		util.UpdateCondition(&job.Status, &sev1alpha1.PodMigrationJobCondition{
			Type: hamiplacement.TargetProtectedCondition, Status: sev1alpha1.PodMigrationJobConditionStatusFalse,
			Reason: "HamiPlacementRetired", Message: reportedUID,
		})
		if err := r.Status().Update(ctx, job); err != nil {
			return false, err
		}
	}
	if owned {
		return true, r.deleteHamiReservation(ctx, target, false)
	}

	if slices.Contains(job.Finalizers, hamiTargetOwnerFinalizer) {
		job.Finalizers = slices.DeleteFunc(job.Finalizers, func(value string) bool { return value == hamiTargetOwnerFinalizer })
		return false, r.Update(ctx, job)
	}
	return false, nil
}
