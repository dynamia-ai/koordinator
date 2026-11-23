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
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sev1alpha1 "github.com/koordinator-sh/koordinator/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/evictor"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/hamiplacement"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/util"
)

const hamiEvictionSubmitting = "HamiEvictionSubmitting"

func isHamiJob(job *sev1alpha1.PodMigrationJob) bool {
	_, condition := util.GetCondition(&job.Status, hamiplacement.ReadyCondition)
	_, approval := util.GetCondition(&job.Status, hamiplacement.ArbitrationCondition)
	return hamiplacement.Requested(job.Annotations) || condition != nil || approval != nil ||
		slices.Contains(job.Finalizers, hamiEvictionRecoveryFinalizer) || slices.Contains(job.Finalizers, hamiTargetOwnerFinalizer)
}

func hamiJobActive(job *sev1alpha1.PodMigrationJob) bool {
	switch job.Status.Phase {
	case "", sev1alpha1.PodMigrationJobPending, sev1alpha1.PodMigrationJobRunning:
		return true
	default:
		return false
	}
}

func hamiJobManaged(job *sev1alpha1.PodMigrationJob) bool {
	_, lock := util.GetCondition(&job.Status, hamiplacement.LockCondition)
	return (lock != nil && lock.Status == sev1alpha1.PodMigrationJobConditionStatusTrue && lock.Message != "") ||
		slices.Contains(job.Finalizers, hamiEvictionRecoveryFinalizer) || slices.Contains(job.Finalizers, hamiTargetOwnerFinalizer)
}

var hamiRequeue = reconcile.Result{RequeueAfter: defaultRequeueAfter}

func hamiJobOwner(job *sev1alpha1.PodMigrationJob) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: sev1alpha1.GroupVersion.String(), Kind: "PodMigrationJob", Name: job.Name, UID: job.UID,
	}
}

func ownedByHamiJob(obj client.Object, job *sev1alpha1.PodMigrationJob) bool {
	return slices.ContainsFunc(obj.GetOwnerReferences(), func(ref metav1.OwnerReference) bool {
		return ref.UID == job.UID && ref.Name == job.Name && ref.Kind == "PodMigrationJob" &&
			ref.APIVersion == sev1alpha1.GroupVersion.String()
	})
}

func hamiTarget(job *sev1alpha1.PodMigrationJob) (hamiplacement.Placement, error) {
	ref := job.Spec.PodRef
	if ref == nil || ref.UID == "" || ref.Namespace == "" || ref.Name == "" {
		return hamiplacement.Placement{}, fmt.Errorf("spec.podRef.namespace, name and uid are required")
	}
	if job.Spec.TTL == nil || job.Spec.TTL.Duration <= 0 {
		return hamiplacement.Placement{}, fmt.Errorf("spec.ttl must be positive")
	}
	if job.Spec.Mode != sev1alpha1.PodMigrationJobModeEvictionDirectly || job.Spec.ReservationOptions != nil {
		return hamiplacement.Placement{}, fmt.Errorf("HAMi placement requires spec.mode=EvictDirectly without Koordinator reservation options")
	}
	if job.Spec.DeleteOptions != nil && len(job.Spec.DeleteOptions.DryRun) > 0 {
		return hamiplacement.Placement{}, fmt.Errorf("HAMi placement does not support spec.deleteOptions.dryRun")
	}
	node := job.Annotations[hamiplacement.AnnotationNode]
	if len(validation.IsDNS1123Subdomain(node)) != 0 {
		return hamiplacement.Placement{}, fmt.Errorf("invalid %s", hamiplacement.AnnotationNode)
	}
	uuids := strings.Split(job.Annotations[hamiplacement.AnnotationUUIDs], ",")
	for i, uuid := range uuids {
		uuid = strings.TrimSpace(uuid)
		if !strings.HasPrefix(uuid, "GPU-") || len(uuid) == len("GPU-") || len(validation.IsDNS1123Subdomain(strings.ToLower(uuid))) != 0 {
			return hamiplacement.Placement{}, fmt.Errorf("invalid NVIDIA GPU UUID %q", uuid)
		}
		uuids[i] = uuid
	}
	slices.Sort(uuids)
	if len(slices.Compact(slices.Clone(uuids))) != len(uuids) {
		return hamiplacement.Placement{}, fmt.Errorf("target GPU UUIDs must be unique")
	}
	reservation := gpuReservationName(job)
	value, placementOnly := job.Annotations[hamiplacement.AnnotationPlacementOnly]
	if placementOnly {
		if value != "true" {
			return hamiplacement.Placement{}, fmt.Errorf("%s must be true when supplied", hamiplacement.AnnotationPlacementOnly)
		}
		reservation = ""
	}
	var occupants []string
	if raw, exists := job.Annotations[hamiplacement.AnnotationTargetOccupants]; exists {
		if !placementOnly || json.Unmarshal([]byte(raw), &occupants) != nil || len(occupants) == 0 || len(occupants) > 128 {
			return hamiplacement.Placement{}, fmt.Errorf("%s requires placement-only and a JSON array of 1 to 128 Pod UIDs", hamiplacement.AnnotationTargetOccupants)
		}
		unique := sets.New[string]()
		for _, uid := range occupants {
			if uid == "" || len(uid) > 253 || uid != strings.TrimSpace(uid) || uid == string(ref.UID) || unique.Has(uid) {
				return hamiplacement.Placement{}, fmt.Errorf("invalid or duplicate allowed occupant UID %q", uid)
			}
			unique.Insert(uid)
		}
		reservation = gpuReservationName(job)
	}
	var required *hamiplacement.ReservationReference
	if raw, exists := job.Annotations[hamiplacement.AnnotationRequiredTarget]; exists {
		required = &hamiplacement.ReservationReference{}
		if placementOnly || json.Unmarshal([]byte(raw), required) != nil || required.UID == "" || string(required.UID) != strings.TrimSpace(string(required.UID)) ||
			required.Name == reservation || len(validation.IsDNS1123Subdomain(required.Name)) != 0 {
			return hamiplacement.Placement{}, fmt.Errorf("%s requires an eviction job and a distinct GPUReservation name and UID", hamiplacement.AnnotationRequiredTarget)
		}
	}
	return hamiplacement.Placement{
		JobUID: job.UID, SourceName: ref.Name, SourceUID: ref.UID, NodeName: node, GPUUUIDs: uuids, Reservation: reservation,
		ExpiresAt:           metav1.NewTime(job.CreationTimestamp.Add(job.Spec.TTL.Duration).Truncate(time.Second)),
		AllowedOccupantUIDs: occupants, RequiredTarget: required,
	}, nil
}

func (r *Reconciler) failHami(ctx context.Context, job *sev1alpha1.PodMigrationJob, reason, message string) (reconcile.Result, error) {
	job.Status.Phase = sev1alpha1.PodMigrationJobFailed
	job.Status.Reason = reason
	job.Status.Message = message
	return reconcile.Result{}, r.Status().Update(ctx, job)
}

func (r *Reconciler) hamiLock(ctx context.Context, job *sev1alpha1.PodMigrationJob) (*corev1.ConfigMap, error) {
	locks := &corev1.ConfigMapList{}
	if err := r.hamiReader.List(ctx, locks, client.InNamespace(job.Spec.PodRef.Namespace),
		client.MatchingLabels{hamiplacement.AnnotationJobUID: string(job.UID)}); err != nil {
		return nil, err
	}
	switch len(locks.Items) {
	case 0:
		return nil, nil
	case 1:
		if ownedByHamiJob(&locks.Items[0], job) {
			return &locks.Items[0], nil
		}
	}
	return nil, fmt.Errorf("migration %s has conflicting placement locks", job.Name)
}

func (r *Reconciler) migrateHami(ctx context.Context, job *sev1alpha1.PodMigrationJob) (reconcile.Result, error) {
	if r.hamiReader == nil {
		return reconcile.Result{}, nil
	}
	if slices.Contains(job.Finalizers, hamiEvictionRecoveryFinalizer) {
		if err := r.recoverHamiEviction(ctx, job); err != nil {
			return reconcile.Result{}, err
		}
	}
	if !job.DeletionTimestamp.IsZero() {
		if slices.Contains(job.Finalizers, hamiEvictionRecoveryFinalizer) {
			return hamiRequeue, nil
		}
		if slices.Contains(job.Finalizers, hamiTargetOwnerFinalizer) {
			pending, err := r.retireHamiTarget(ctx, job)
			if pending || err != nil {
				return hamiRequeue, err
			}
		}
		return reconcile.Result{}, nil
	}
	switch job.Status.Phase {
	case "", sev1alpha1.PodMigrationJobPending, sev1alpha1.PodMigrationJobRunning:
	case sev1alpha1.PodMigrationJobFailed, sev1alpha1.PodMigrationJobAborted, sev1alpha1.PodMigrationJobSucceeded:
		pending, err := r.cleanupHami(ctx, job)
		if pending || slices.Contains(job.Finalizers, hamiEvictionRecoveryFinalizer) {
			return hamiRequeue, err
		}
		return reconcile.Result{}, err
	default:
		return reconcile.Result{}, nil
	}
	placement, err := hamiTarget(job)
	if err != nil {
		return r.failHami(ctx, job, "InvalidHamiPlacement", err.Error())
	}
	if !r.clock.Now().Before(placement.ExpiresAt.Time) {
		message := "HAMi placement deadline expired"
		if job.Status.Message != "" {
			message += ": " + job.Status.Message
		}
		return r.failHami(ctx, job, sev1alpha1.PodMigrationJobReasonTimeout, message)
	}
	evictsSource := job.Annotations[hamiplacement.AnnotationPlacementOnly] != "true"
	deferredGrant := len(placement.AllowedOccupantUIDs) > 0
	if job.Spec.Paused || (evictsSource && !hamiplacement.PassedArbitration(job)) {
		return hamiRequeue, nil
	}

	if r.args.DryRun || r.hamiDryRun ||
		(r.args.DefaultDeleteOptions != nil && len(r.args.DefaultDeleteOptions.DryRun) > 0) {
		return r.failHami(ctx, job, "HamiPlacementDryRun", "MigrationController dryRun is enabled")
	}
	if deferredGrant {
		if err := r.ensureHamiTargetCleanup(ctx, job); err != nil {
			return reconcile.Result{}, err
		}
		if job.Annotations[hamiTargetNamespace] != job.Spec.PodRef.Namespace {
			return r.failHami(ctx, job, "HamiPlacementChanged", "The deferred reservation namespace changed")
		}
	}

	_, condition := util.GetCondition(&job.Status, hamiplacement.ReadyCondition)
	armed := condition != nil && condition.Status == sev1alpha1.PodMigrationJobConditionStatusTrue
	ref := job.Spec.PodRef
	source := &corev1.Pod{}
	if err := r.hamiReader.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, source); client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, err
	}
	sourcePresent := source.UID == ref.UID
	sourceActive := hamiplacement.SourceActive(source, ref.UID)
	if !armed && !sourceActive {
		return r.failHami(ctx, job, "MissingSourcePod", "Source Pod departed before HAMi placement was ready")
	}
	if sourceActive && evictsSource && source.Labels[hamiplacement.RecoveryEvictableLabel] != "true" {
		return r.failHami(ctx, job, "HamiEvictionNotAuthorized", "Source Pod does not have hami.io/recovery-evictable=true")
	}
	if condition == nil {
		return hamiRequeue, r.updateCondition(ctx, job, &sev1alpha1.PodMigrationJobCondition{
			Type: hamiplacement.ReadyCondition, Status: sev1alpha1.PodMigrationJobConditionStatusFalse,
			Reason: "PreparingHamiPlacement", Message: "Preparing replacement placement",
		})
	}

	_, lockCondition := util.GetCondition(&job.Status, hamiplacement.LockCondition)
	lock, err := r.hamiLock(ctx, job)
	if err != nil {
		return reconcile.Result{}, err
	}
	if lock == nil {
		if armed || lockCondition != nil {
			return r.failHami(ctx, job, "MissingHamiPlacement", "Armed placement ConfigMap is missing")
		}
		return r.prepareHamiPlacement(ctx, job, source, placement)
	}
	persisted, err := hamiplacement.FromConfigMap(lock)
	if err != nil {
		return r.failHami(ctx, job, "InvalidHamiPlacement", err.Error())
	}
	if persisted.SourceUID != placement.SourceUID || persisted.SourceName != placement.SourceName || persisted.NodeName != placement.NodeName ||
		!slices.Equal(persisted.GPUUUIDs, placement.GPUUUIDs) || persisted.Reservation != placement.Reservation ||
		!sets.New(persisted.AllowedOccupantUIDs...).Equal(sets.New(placement.AllowedOccupantUIDs...)) ||
		!ptr.Equal(persisted.RequiredTarget, placement.RequiredTarget) ||
		!persisted.ExpiresAt.Equal(&placement.ExpiresAt) {
		return r.failHami(ctx, job, "HamiPlacementChanged", "The HAMi placement request changed after preparation")
	}

	if lockCondition == nil {
		rs, err := r.hamiReplicaSet(ctx, source)
		if err != nil {
			return reconcile.Result{}, err
		}
		matches, err := r.hamiMembershipMatches(ctx, rs, persisted)
		if err != nil {
			return reconcile.Result{}, err
		}
		if !matches {
			return r.failHami(ctx, job, "HamiWorkloadChanged", "Placement ConfigMap does not match the source ReplicaSet")
		}
		condition := &sev1alpha1.PodMigrationJobCondition{
			Type: hamiplacement.LockCondition, Status: sev1alpha1.PodMigrationJobConditionStatusTrue,
			Reason: "HamiPlacementPersisted", Message: string(lock.UID),
		}
		util.UpdateCondition(&job.Status, condition)
		return hamiRequeue, r.Status().Update(ctx, job)
	}
	if lock.UID == "" || lockCondition.Message != string(lock.UID) {
		return r.failHami(ctx, job, "HamiPlacementChanged", "Placement ConfigMap identity changed")
	}

	_, eviction := util.GetCondition(&job.Status, sev1alpha1.PodMigrationJobConditionEviction)
	if sourceActive && evictsSource && eviction != nil {
		if eviction.Reason == sev1alpha1.PodMigrationJobReasonEvicting {
			return hamiRequeue, nil
		}
		if eviction.Reason == hamiEvictionSubmitting {
			if persisted.RequiredTarget != nil && (slices.Contains(job.Finalizers, hamiEvictionRecoveryFinalizer) || r.hamiDependencyPreparing(job)) {
				return hamiRequeue, nil
			}
			reservation, err := r.hamiReservation(ctx, job, persisted, false)
			if client.IgnoreNotFound(err) != nil {
				return reconcile.Result{}, err
			}
			if reservation != nil {
				if _, held := reservation.GetAnnotations()[hamiSourceEvictionAnnotation]; held {
					return hamiRequeue, nil
				}
			}
		}
	}
	shouldProbe := sourceActive && (!armed || evictsSource)
	if shouldProbe {
		rs, err := r.hamiReplicaSet(ctx, source)
		if err != nil {
			return reconcile.Result{}, err
		}
		matches, err := r.hamiMembershipMatches(ctx, rs, persisted)
		if err != nil {
			return reconcile.Result{}, err
		}
		if !matches {
			return r.failHami(ctx, job, "HamiWorkloadChanged", "ReplicaSet membership changed during HAMi placement preparation")
		}
		if rejected, err := r.verifyHamiAdmission(ctx, rs, persisted); err != nil {
			reason := "HamiAdmissionUnavailable"
			if rejected {
				reason = "HamiAdmissionRejected"
				if placement.Reservation != "" && !deferredGrant {
					if err := r.releaseHamiReservation(ctx, job, true); err != nil {
						return reconcile.Result{}, err
					}
				}
			}
			condition := &sev1alpha1.PodMigrationJobCondition{
				Type: "HamiAdmission", Status: sev1alpha1.PodMigrationJobConditionStatusFalse,
				Reason: reason, Message: err.Error(),
			}
			if rejected && ((armed && placement.Reservation != "") || (deferredGrant && job.Annotations[hamiplacement.AnnotationTargetUID] != "")) {
				util.UpdateCondition(&job.Status, condition)
				return r.failHami(ctx, job, reason, err.Error())
			}
			return hamiRequeue, r.updateCondition(ctx, job, condition)
		}
	}

	var reservation *unstructured.Unstructured
	if placement.Reservation != "" {
		allowCreate := !armed && (!deferredGrant || job.Annotations[hamiplacement.AnnotationTargetUID] == "")
		reservation, err = r.hamiReservation(ctx, job, persisted, allowCreate)
		if apierrors.IsNotFound(err) {
			return r.failHami(ctx, job, "MissingGPUReservation", "The armed HAMi GPUReservation is missing")
		}
		if err != nil {
			return reconcile.Result{}, err
		}
		if deferredGrant {
			managed, err := r.manageHamiTarget(ctx, job, reservation)
			if !managed || err != nil {
				return hamiRequeue, err
			}
			changed, err := r.reportHamiTargetProtection(ctx, job, reservation, armed)
			if changed || err != nil {
				return hamiRequeue, err
			}
		}
		if _, held := reservation.GetAnnotations()[hamiSourceEvictionAnnotation]; held {
			return hamiRequeue, nil
		}
		if reservation.GetDeletionTimestamp() != nil {
			return r.failHami(ctx, job, "GPUReservationUnavailable", "Armed GPUReservation is being deleted")
		}
		switch phase := reservationPhase(reservation); phase {
		case "Pending", "":
			if !deferredGrant {
				return hamiRequeue, nil
			}
		case "Protecting":
			if !deferredGrant || !reservationProtected(reservation) {
				return r.failHami(ctx, job, "GPUReservationUnprotected", "Protecting GPUReservation lacks deferred grant configuration or scheduler protection")
			}
		case "Reserved":
			if !reservationProtected(reservation) {
				return r.failHami(ctx, job, "GPUReservationUnprotected", "Reserved GPUReservation lacks scheduler protection")
			}
			if !usableHamiReservation(reservation, r.clock.Now()) {
				return hamiRequeue, nil
			}
		case "Consumed":
			if job.Status.PodRef == nil {
				return r.failHami(ctx, job, "UnexpectedReservationConsumer", "GPUReservation was consumed before selecting a replacement")
			}
		default:
			return r.failHami(ctx, job, "GPUReservationUnavailable", fmt.Sprintf("GPUReservation %s has phase %q", placement.Reservation, phase))
		}
	}

	if shouldProbe {
		if !armed {
			job.Status.Phase = sev1alpha1.PodMigrationJobRunning
			return hamiRequeue, r.updateCondition(ctx, job, &sev1alpha1.PodMigrationJobCondition{
				Type: hamiplacement.ReadyCondition, Status: sev1alpha1.PodMigrationJobConditionStatusTrue,
				Reason: "HamiAdmissionVerified", Message: "Replacement placement is armed",
			})
		}
		if evictsSource {
			return r.evictHamiSource(ctx, job, source, persisted)
		}
	}
	if sourcePresent && source.Status.Phase != corev1.PodFailed && source.Status.Phase != corev1.PodSucceeded {
		return hamiRequeue, nil
	}
	return r.reconcileHamiReplacement(ctx, job, persisted, reservation)
}

func (r *Reconciler) cleanupHami(ctx context.Context, job *sev1alpha1.PodMigrationJob) (bool, error) {
	deferred := slices.Contains(job.Finalizers, hamiTargetOwnerFinalizer) ||
		job.Annotations[hamiplacement.AnnotationTargetOccupants] != "" || job.Annotations[hamiplacement.AnnotationTargetUID] != ""
	if deferred {
		pending, err := r.retireHamiTarget(ctx, job)
		if pending || err != nil {
			return pending, err
		}
	}
	if job.Spec.PodRef == nil || job.Spec.PodRef.Namespace == "" {
		return false, nil
	}
	if !deferred && job.Annotations[hamiplacement.AnnotationPlacementOnly] != "true" {
		if err := r.releaseHamiReservation(ctx, job, false); err != nil {
			return false, err
		}
	}
	_, ready := util.GetCondition(&job.Status, hamiplacement.ReadyCondition)
	if job.Status.Phase == sev1alpha1.PodMigrationJobSucceeded || ready == nil || ready.Status != sev1alpha1.PodMigrationJobConditionStatusTrue {
		lock, err := r.hamiLock(ctx, job)
		if err != nil || lock == nil {
			return false, err
		}
		return false, client.IgnoreNotFound(r.Delete(ctx, lock, client.Preconditions{UID: &lock.UID}))
	}
	return false, nil
}

func (r *Reconciler) hamiReplicaSet(ctx context.Context, source *corev1.Pod) (*appsv1.ReplicaSet, error) {
	owner := hamiplacement.ReplicaSetOwner(source)
	if owner == nil {
		return nil, fmt.Errorf("source Pod %s/%s requires a ReplicaSet controller", source.Namespace, source.Name)
	}
	rs := &appsv1.ReplicaSet{}
	if err := r.hamiReader.Get(ctx, client.ObjectKey{Namespace: source.Namespace, Name: owner.Name}, rs); err != nil {
		return nil, err
	}
	if rs.UID != owner.UID || !rs.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("source ReplicaSet %s/%s changed", rs.Namespace, rs.Name)
	}
	return rs, nil
}

func (r *Reconciler) prepareHamiPlacement(ctx context.Context, job *sev1alpha1.PodMigrationJob, source *corev1.Pod, placement hamiplacement.Placement) (reconcile.Result, error) {
	ns := &corev1.Namespace{}
	if err := r.hamiReader.Get(ctx, client.ObjectKey{Name: source.Namespace}, ns); err != nil {
		return reconcile.Result{}, err
	}
	if ns.Labels[hamiplacement.ManagedNamespaceLabel] != "true" || source.Spec.SchedulerName != hamiplacement.SchedulerName || source.Spec.NodeName == "" ||
		hamiplacement.ReplicaSetOwner(source) == nil {
		return r.failHami(ctx, job, "UnsupportedHamiWorkload", "Source must be a scheduled ReplicaSet Pod using hami-scheduler in an opted-in namespace")
	}
	rs, err := r.hamiReplicaSet(ctx, source)
	if err != nil {
		return reconcile.Result{}, err
	}
	if err := validateHamiTemplate(rs, placement); err != nil {
		return r.failHami(ctx, job, "UnsupportedHamiWorkload", err.Error())
	}
	placement.OwnerUID = rs.UID
	pods, err := r.hamiOwnerPods(ctx, source.Namespace, rs.UID)
	if err != nil {
		return reconcile.Result{}, err
	}
	if len(pods) != int(ptr.Deref(rs.Spec.Replicas, 1)) {
		return hamiRequeue, nil
	}
	for i := range pods {
		if pods[i].UID != source.UID {
			placement.SiblingUIDs = append(placement.SiblingUIDs, pods[i].UID)
		}
	}
	if len(placement.SiblingUIDs) != len(pods)-1 {
		return hamiRequeue, nil
	}
	lock, err := placement.ConfigMap(source.Namespace, hamiJobOwner(job))
	if err != nil {
		return reconcile.Result{}, err
	}
	if err := r.Create(ctx, lock); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return hamiRequeue, nil
		}
		return reconcile.Result{}, err
	}
	return hamiRequeue, nil
}

func validateHamiTemplate(rs *appsv1.ReplicaSet, placement hamiplacement.Placement) error {
	spec := &rs.Spec.Template.Spec
	if spec.SchedulerName != hamiplacement.SchedulerName || spec.NodeName != "" {
		return fmt.Errorf("ReplicaSet template must explicitly use hami-scheduler without a preassigned node")
	}
	gpuResource := corev1.ResourceName("nvidia.com/gpu")
	containers := 0
	var gpuCount int64
	for _, container := range spec.Containers {
		count := container.Resources.Limits[gpuResource]
		if count.IsZero() {
			count = container.Resources.Requests[gpuResource]
		}
		if count.Sign() > 0 {
			containers++
			gpuCount = count.Value()
		}
	}
	for _, container := range spec.InitContainers {
		limit := container.Resources.Limits[gpuResource]
		request := container.Resources.Requests[gpuResource]
		if !limit.IsZero() || !request.IsZero() {
			return fmt.Errorf("targeted NVIDIA GPU placement does not support GPU requests in init containers")
		}
	}
	if containers != 1 || gpuCount != int64(len(placement.GPUUUIDs)) {
		return fmt.Errorf("targeted placement requires one GPU container requesting exactly %d NVIDIA GPUs", len(placement.GPUUUIDs))
	}
	return nil
}

func definitiveHamiAPIRejection(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) ||
		apierrors.IsInvalid(err) || apierrors.IsBadRequest(err)
}

func (r *Reconciler) verifyHamiAdmission(ctx context.Context, rs *appsv1.ReplicaSet, placement hamiplacement.Placement) (rejected bool, err error) {
	if err := validateHamiTemplate(rs, placement); err != nil {
		return true, err
	}
	owner := metav1.NewControllerRef(rs, appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))
	owner.BlockOwnerDeletion = nil
	probe := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: rs.Namespace, GenerateName: "hami-placement-probe-",
			Labels: rs.Spec.Template.Labels, Annotations: rs.Spec.Template.Annotations,
			OwnerReferences: []metav1.OwnerReference{*owner},
		},
		Spec: *rs.Spec.Template.Spec.DeepCopy(),
	}
	if err := r.Create(ctx, probe, client.DryRunAll); err != nil {
		return definitiveHamiAPIRejection(err), fmt.Errorf("verify HAMi placement admission: %w", err)
	}
	if !hamiplacement.HasGate(probe) || !isHamiPlacementPod(probe, placement) {
		return true, fmt.Errorf("dry-run Pod was not gated by the HAMi placement webhook")
	}
	const poolAnnotation = "hami.io/resource-pool"
	if pool := strings.TrimSpace(rs.Spec.Template.Annotations[poolAnnotation]); pool != "" &&
		(probe.Annotations[poolAnnotation] != pool || probe.Annotations["resource.hami.io/revision"] == "" ||
			probe.Annotations["resource.hami.io/pool-selection-revision"] == "") {
		return true, fmt.Errorf("dry-run Pod is missing RPM authorization for resource pool %q", pool)
	}
	node := &corev1.Node{}
	if err := r.hamiReader.Get(ctx, client.ObjectKey{Name: placement.NodeName}, node); err != nil {
		return apierrors.IsNotFound(err), err
	}
	fits, err := nodeaffinity.GetRequiredNodeAffinity(probe).Match(node)
	if err != nil || !fits {
		return true, fmt.Errorf("target node %s does not match the replacement's required node affinity", node.Name)
	}
	return false, nil
}

func (r *Reconciler) evictHamiSource(ctx context.Context, job *sev1alpha1.PodMigrationJob, source *corev1.Pod, placement hamiplacement.Placement) (reconcile.Result, error) {
	if r.checkPodExceedObjectLimiter(source) {
		return hamiRequeue, nil
	}
	// A condition update also enqueues the job. Keep definite denials
	// visible to the orchestrator for a full retry interval.
	_, previous := util.GetCondition(&job.Status, sev1alpha1.PodMigrationJobConditionEviction)
	if previous != nil && r.clock.Since(previous.LastProbeTime.Time) < defaultRequeueAfter {
		return hamiRequeue, nil
	}
	eviction := job.DeepCopy()
	if eviction.Spec.DeleteOptions == nil {
		eviction.Spec.DeleteOptions = r.args.DefaultDeleteOptions.DeepCopy()
	}
	if eviction.Spec.DeleteOptions == nil {
		eviction.Spec.DeleteOptions = &metav1.DeleteOptions{}
	}
	if eviction.Spec.DeleteOptions.Preconditions == nil {
		eviction.Spec.DeleteOptions.Preconditions = &metav1.Preconditions{}
	}
	preconditions := eviction.Spec.DeleteOptions.Preconditions
	if source.ResourceVersion == "" ||
		(preconditions.ResourceVersion != nil && *preconditions.ResourceVersion != source.ResourceVersion) {
		return hamiRequeue, nil
	}
	preconditions.UID = &source.UID
	preconditions.ResourceVersion = &source.ResourceVersion
	condition := &sev1alpha1.PodMigrationJobCondition{
		Type: sev1alpha1.PodMigrationJobConditionEviction, Status: sev1alpha1.PodMigrationJobConditionStatusFalse,
		Reason: hamiEvictionSubmitting, Message: "Waiting for the source eviction outcome",
		LastProbeTime: metav1.NewTime(r.clock.Now()),
	}
	if placement.RequiredTarget == nil {
		if err := r.addHamiEvictionRecovery(ctx, job); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				return hamiRequeue, nil
			}
			return hamiRequeue, err
		}
		if err := r.updateCondition(ctx, job, condition); err != nil {
			if apierrors.IsConflict(err) {
				return hamiRequeue, nil
			}
			return reconcile.Result{}, err
		}
	}

	deferEviction := func(job *sev1alpha1.PodMigrationJob, message string) error {
		condition.Reason = "HamiEvictionDeferred"
		condition.Message = message
		condition.LastProbeTime = metav1.NewTime(r.clock.Now())
		return r.updateCondition(ctx, job, condition)
	}
	reservation, err := r.hamiReservation(ctx, job, placement, false)
	if err != nil || !usableHamiReservation(reservation, r.clock.Now()) {
		return hamiRequeue, errors.Join(client.IgnoreNotFound(err),
			deferEviction(job, "Could not confirm an active GPUReservation before eviction submission"))
	}
	if consumer, _, err := unstructured.NestedMap(reservation.Object, "spec", "consumerRef"); err != nil || consumer != nil {
		return r.failHami(ctx, job, "UnexpectedReservationConsumer", "GPUReservation has a consumer before source eviction")
	}

	current := &sev1alpha1.PodMigrationJob{}
	if err := r.hamiReader.Get(ctx, client.ObjectKeyFromObject(job), current); err != nil {
		return hamiRequeue, client.IgnoreNotFound(err)
	}
	if current.UID != job.UID || !current.DeletionTimestamp.IsZero() {
		return hamiRequeue, nil
	}
	if current.ResourceVersion != job.ResourceVersion {
		return hamiRequeue, nil
	}
	if current.Spec.Paused ||
		!usableHamiReservation(reservation, r.clock.Now()) {
		return hamiRequeue, deferEviction(current, "Migration changed or reached its deadline before eviction submission")
	}

	marker, err := hamiEvictionMarker(source, hamiEvictionCheckpointPrefix+job.ResourceVersion+"/"+string(job.UID))
	if err != nil {
		return hamiRequeue, err
	}
	var target *unstructured.Unstructured
	var prepareUntil time.Time
	attempt := string(reservation.GetUID()) + "/" + marker
	if placement.RequiredTarget != nil {
		target, err = r.hamiEvictionDependency(ctx, source, placement.RequiredTarget, "")
		if err != nil {
			return hamiRequeue, err
		}
		if target == nil {
			return hamiRequeue, deferEviction(job, "Required target GPUReservation does not have an available protection pin")
		}
		prepareUntil = r.clock.Now().Add(hamiEvictionPreparationTime).Truncate(time.Microsecond)
		// Establish the preparation claim before adding the recovery
		// finalizer. Neither pin can be written without both.
		setHamiEvictionCondition(job, condition)
		if err := r.checkpointHamiDependency(ctx, job, reservation, target, marker, prepareUntil); err != nil {
			return hamiRequeue, err
		}
		if !r.clock.Now().Before(prepareUntil) {
			return hamiRequeue, nil
		}
		if err := r.addHamiEvictionRecovery(ctx, job); err != nil {
			return hamiRequeue, client.IgnoreNotFound(err)
		}
	}
	clearDestination := func() error {
		return r.clearHamiEviction(ctx, reservation, hamiSourceEvictionAnnotation, marker)
	}
	abandon := func() error {
		// A dependent attempt's checkpoint also fences pin writes whose
		// responses were lost. Reconciliation settles both reservations.
		if target != nil {
			condition.Reason = "HamiEvictionDeferred"
			condition.Message = "Eviction preparation ended before submission"
			condition.LastProbeTime = metav1.NewTime(r.clock.Now())
			return r.recordHamiEvictionOutcome(ctx, job, condition, attempt, true)
		}
		return clearDestination()
	}
	if target != nil {
		if !r.clock.Now().Before(prepareUntil) {
			return hamiRequeue, abandon()
		}
		if err := r.commitHamiEviction(ctx, target, hamiDependentEvictionAnnotation, marker); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				return hamiRequeue, abandon()
			}
			return hamiRequeue, errors.Join(err, abandon())
		}
	}
	err = r.commitHamiEviction(ctx, reservation, hamiSourceEvictionAnnotation, marker)
	if err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			if target != nil {
				return hamiRequeue, abandon()
			}
			return hamiRequeue, deferEviction(job, "GPUReservation changed before eviction submission")
		}
		// Even if the API applied the write, we have not issued an eviction.
		return hamiRequeue, errors.Join(err, abandon())
	}
	if target != nil {
		live, err := r.hamiEvictionDependency(ctx, source, placement.RequiredTarget, marker)
		if err != nil || live == nil {
			return hamiRequeue, errors.Join(err, abandon())
		}
		target = live
	}
	// This read protects pauses that landed while the reservation CAS was in
	// flight. Once submitted, the eviction and PMJ remain separate API calls.
	if err := r.hamiReader.Get(ctx, client.ObjectKeyFromObject(job), current); err != nil {
		return hamiRequeue, errors.Join(client.IgnoreNotFound(err), abandon())
	}
	if current.UID != job.UID || !current.DeletionTimestamp.IsZero() {
		return hamiRequeue, abandon()
	}
	if current.ResourceVersion != job.ResourceVersion {
		return hamiRequeue, abandon()
	}
	if current.Spec.Paused ||
		!r.clock.Now().Before(placement.ExpiresAt.Time) {
		if target != nil {
			return hamiRequeue, abandon()
		}
		return hamiRequeue, errors.Join(abandon(),
			deferEviction(current, "Migration changed or reached its deadline before eviction submission"))
	}

	if target != nil && !r.clock.Now().Before(prepareUntil) {
		return hamiRequeue, abandon()
	}
	if err := r.startHamiEviction(ctx, current, attempt); err != nil {
		// This submitter never called Eviction, even if the checkpoint
		// write reached the server and its response was lost.
		return hamiRequeue, errors.Join(err, abandon())
	}
	now := r.clock.Now()
	if target != nil && !now.Before(prepareUntil) {
		return hamiRequeue, abandon()
	}
	targetExpired := target != nil && !currentHamiProtection(target, now)
	expired := !now.Before(placement.ExpiresAt.Time) || targetExpired
	if !expired {
		err = r.hamiEvict(ctx, eviction, source)
	}
	condition.LastProbeTime = metav1.NewTime(r.clock.Now())
	switch {
	case expired:
		condition.Reason = "HamiEvictionDeferred"
		condition.Message = "Migration reached its deadline before eviction submission"
		if targetExpired {
			condition.Message = "Required target protection reached its deadline before eviction submission"
		}
	case apierrors.IsConflict(err), apierrors.IsNotFound(err), errors.Is(err, evictor.ErrTooManyEvictions):
		condition.Reason = "HamiEvictionDeferred"
		condition.Message = err.Error()
	case apierrors.IsTooManyRequests(err), definitiveHamiAPIRejection(err):
		condition.Reason = "HamiEvictionBlocked"
		condition.Message = err.Error()
	case err != nil:
		// The Eviction API may still be processing this request. A changed
		// Pod resourceVersion is insufficient evidence that deletion stopped.
		return reconcile.Result{}, err
	default:
		r.trackEvictedPod(source)
		condition.Reason = sev1alpha1.PodMigrationJobReasonEvicting
		condition.Message = "Eviction accepted for the source Pod"
	}
	if err := r.recordHamiEvictionOutcome(ctx, job, condition, attempt, false); err != nil {
		return hamiRequeue, err
	}
	if target != nil && condition.Reason == sev1alpha1.PodMigrationJobReasonEvicting {
		return hamiRequeue, nil
	}
	if err := clearDestination(); err != nil {
		return hamiRequeue, err
	}
	return hamiRequeue, r.clearHamiEviction(ctx, target, hamiDependentEvictionAnnotation, marker)
}

func (r *Reconciler) hamiOwnerPods(ctx context.Context, namespace string, ownerUID types.UID) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := r.hamiReader.List(ctx, pods, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return slices.DeleteFunc(pods.Items, func(pod corev1.Pod) bool {
		owner := hamiplacement.ReplicaSetOwner(&pod)
		return owner == nil || owner.UID != ownerUID || !pod.DeletionTimestamp.IsZero() ||
			pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded
	}), nil
}

func (r *Reconciler) hamiMembershipMatches(ctx context.Context, rs *appsv1.ReplicaSet, placement hamiplacement.Placement) (bool, error) {
	if rs.UID != placement.OwnerUID {
		return false, nil
	}
	pods, err := r.hamiOwnerPods(ctx, rs.Namespace, rs.UID)
	if err != nil {
		return false, err
	}
	if len(pods) != len(placement.SiblingUIDs)+1 || len(pods) != int(ptr.Deref(rs.Spec.Replicas, 1)) ||
		slices.Contains(placement.SiblingUIDs, placement.SourceUID) ||
		sets.New(placement.SiblingUIDs...).Len() != len(placement.SiblingUIDs) {
		return false, nil
	}
	return containsHamiSiblings(pods, placement.SiblingUIDs) &&
		slices.ContainsFunc(pods, func(pod corev1.Pod) bool { return pod.UID == placement.SourceUID }), nil
}

func containsHamiSiblings(pods []corev1.Pod, siblings []types.UID) bool {
	for _, uid := range siblings {
		if !slices.ContainsFunc(pods, func(pod corev1.Pod) bool { return pod.UID == uid }) {
			return false
		}
	}
	return true
}

func isHamiPlacementPod(pod *corev1.Pod, placement hamiplacement.Placement) bool {
	return pod.Spec.SchedulerName == hamiplacement.SchedulerName && hamiplacement.PinnedToNode(pod, placement.NodeName) &&
		pod.Annotations[hamiplacement.AnnotationJobUID] == string(placement.JobUID) &&
		pod.Annotations[hamiplacement.AnnotationReservation] == placement.Reservation &&
		pod.Annotations[hamiplacement.AnnotationUseUUID] == strings.Join(placement.GPUUUIDs, ",")
}

func (r *Reconciler) reconcileHamiReplacement(ctx context.Context, job *sev1alpha1.PodMigrationJob, placement hamiplacement.Placement, reservation *unstructured.Unstructured) (reconcile.Result, error) {
	pods, err := r.hamiOwnerPods(ctx, job.Spec.PodRef.Namespace, placement.OwnerUID)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !containsHamiSiblings(pods, placement.SiblingUIDs) {
		return r.failHami(ctx, job, "HamiWorkloadChanged", "A sibling Pod departed during HAMi migration")
	}
	candidates := slices.DeleteFunc(pods, func(pod corev1.Pod) bool {
		return slices.Contains(placement.SiblingUIDs, pod.UID)
	})
	selected := job.Status.PodRef
	if len(candidates) == 0 && selected == nil {
		return hamiRequeue, nil
	}
	if len(candidates) != 1 {
		return r.failHami(ctx, job, "AmbiguousHamiReplacement", "Expected exactly one active replacement from the original ReplicaSet")
	}
	pod := &candidates[0]
	if !isHamiPlacementPod(pod, placement) || pod.UID == "" {
		return r.failHami(ctx, job, "UnmanagedHamiReplacement", "Replacement is missing HAMi placement identity or constraints")
	}
	if selected == nil {
		if !hamiplacement.HasGate(pod) || pod.Spec.NodeName != "" {
			return r.failHami(ctx, job, "UngatedHamiReplacement", "Replacement was schedulable before selection")
		}
		job.Status.PodRef = &corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}
		return hamiRequeue, r.Status().Update(ctx, job)
	}
	if selected.UID != pod.UID || selected.Name != pod.Name || selected.Namespace != pod.Namespace {
		return r.failHami(ctx, job, "HamiReplacementChanged", "Selected replacement Pod identity changed")
	}

	if hamiplacement.HasGate(pod) {
		if reservation != nil {
			live, err := r.hamiReservation(ctx, job, placement, false)
			if err != nil {
				return hamiRequeue, client.IgnoreNotFound(err)
			}
			if live.GetUID() != reservation.GetUID() || !usableHamiReservation(live, r.clock.Now()) {
				return hamiRequeue, nil
			}
			if err := r.assignHamiReservation(ctx, live, pod); err != nil {
				return reconcile.Result{}, err
			}
			if !hamiReservationConsumerMatches(live, pod) {
				return r.failHami(ctx, job, "UnexpectedReservationConsumer", "GPUReservation did not persist the selected replacement as its consumer")
			}
			if !usableHamiReservation(live, r.clock.Now()) {
				return hamiRequeue, nil
			}
		}
		before := pod.DeepCopy()
		pod.Spec.SchedulingGates = slices.DeleteFunc(pod.Spec.SchedulingGates, func(gate corev1.PodSchedulingGate) bool {
			return gate.Name == hamiplacement.SchedulingGate
		})
		return hamiRequeue, r.Patch(ctx, pod, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	}
	if reservation != nil && !hamiReservationConsumerMatches(reservation, pod) {
		return r.failHami(ctx, job, "UnexpectedReservationConsumer", "GPUReservation consumer does not match the selected replacement")
	}
	if pod.Spec.NodeName == "" {
		return hamiRequeue, nil
	}
	if pod.Spec.NodeName != placement.NodeName || !allocatedHamiGPUs(pod, placement.GPUUUIDs) {
		return r.failHami(ctx, job, "HamiPlacementMismatch", "Replacement bound without the expected node and exact GPU UUIDs")
	}
	if !slices.ContainsFunc(pod.Status.Conditions, func(condition corev1.PodCondition) bool {
		return condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue
	}) || (reservation != nil && reservationPhase(reservation) != "Consumed") {
		return hamiRequeue, nil
	}
	job.Status.Phase = sev1alpha1.PodMigrationJobSucceeded
	job.Status.Status = "Complete"
	job.Status.Reason = ""
	job.Status.Message = "Replacement Pod is Ready on the target node and GPUs"
	job.Status.NodeName = pod.Spec.NodeName
	return reconcile.Result{}, r.Status().Update(ctx, job)
}

func allocatedHamiGPUs(pod *corev1.Pod, expected []string) bool {
	var actual []string
	for _, container := range strings.Split(pod.Annotations[hamiplacement.AnnotationAllocated], ";") {
		for _, device := range strings.Split(container, ":") {
			if device == "" {
				continue
			}
			fields := strings.Split(device, ",")
			if len(fields) < 4 || len(fields) > 5 || fields[0] == "" || fields[1] != "NVIDIA" {
				return false
			}
			for _, amount := range fields[2:] {
				if value, err := strconv.ParseInt(amount, 10, 64); err != nil || value < 0 {
					return false
				}
			}
			actual = append(actual, fields[0])
		}
	}
	slices.Sort(actual)
	return slices.Equal(actual, expected)
}
