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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	sev1alpha1 "github.com/koordinator-sh/koordinator/apis/scheduling/v1alpha1"
	deschedulerconfig "github.com/koordinator-sh/koordinator/pkg/descheduler/apis/config"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/arbitrator"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/hamiplacement"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/util"
)

type hamiFixture struct {
	*Reconciler
	job        *sev1alpha1.PodMigrationJob
	source     *corev1.Pod
	sibling    *corev1.Pod
	now        *testingclock.FakeClock
	probeError error
	onProbe    func(context.Context, client.Client, *corev1.Pod)
}

func newHamiFixture(t *testing.T, placementOnly bool) *hamiFixture {
	t.Helper()
	now := metav1.NewTime(time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC))
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "inference", Labels: map[string]string{hamiplacement.ManagedNamespaceLabel: "true"}}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "target"}}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "model-rs", Namespace: ns.Name, UID: "replicaset-uid"},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: ptr.To(int32(2)), Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				SchedulerName: hamiplacement.SchedulerName,
				Containers: []corev1.Container{{Name: "model", Image: "model:v1", Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("2")},
				}}},
			}},
		},
	}
	owner := metav1.NewControllerRef(rs, appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))
	source := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "source", Namespace: ns.Name, UID: "source-uid", OwnerReferences: []metav1.OwnerReference{*owner},
		Labels: map[string]string{hamiplacement.RecoveryEvictableLabel: "true"},
	}, Spec: *rs.Spec.Template.Spec.DeepCopy()}
	source.Spec.NodeName = "original"
	sibling := source.DeepCopy()
	sibling.Name, sibling.UID, sibling.Spec.NodeName = "sibling", "sibling-uid", "retained"
	job := &sev1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Name: "migrate", UID: "job-uid", CreationTimestamp: now, Annotations: map[string]string{
			hamiplacement.AnnotationNode: node.Name, hamiplacement.AnnotationUUIDs: "GPU-b,GPU-a",
		}},
		Spec: sev1alpha1.PodMigrationJobSpec{
			PodRef: &corev1.ObjectReference{Name: source.Name, Namespace: source.Namespace, UID: source.UID},
			Mode:   sev1alpha1.PodMigrationJobModeEvictionDirectly,
			TTL:    &metav1.Duration{Duration: time.Minute},
		},
	}
	if placementOnly {
		job.Annotations[hamiplacement.AnnotationPlacementOnly] = "true"
	}
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, sev1alpha1.AddToScheme(scheme))
	f := &hamiFixture{job: job, source: source, sibling: sibling, now: testingclock.NewFakeClock(now.Time)}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(job, &corev1.Pod{}, gpuReservationObject(job)).
		WithObjects(ns, node, rs, source, sibling, job).
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			pod, isPod := obj.(*corev1.Pod)
			if isPod && len((&client.CreateOptions{}).ApplyOptions(opts).DryRun) > 0 {
				pod.ObjectMeta = *pod.ObjectMeta.DeepCopy()
				if f.onProbe != nil {
					f.onProbe(ctx, c, pod)
				}
				if f.probeError != nil {
					return f.probeError
				}
				cm := &corev1.ConfigMap{}
				owner := hamiplacement.ReplicaSetOwner(pod)
				if err := c.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: hamiplacement.LockName(owner.UID)}, cm); err != nil {
					return err
				}
				p, err := hamiplacement.FromConfigMap(cm)
				if err != nil {
					return err
				}
				return p.Apply(pod)
			}
			if cm, ok := obj.(*corev1.ConfigMap); ok {
				cm.UID = "placement-uid"
			}
			if obj.GetObjectKind().GroupVersionKind() == gpuReservationGVK {
				obj.SetUID("reservation-uid")
			}
			return c.Create(ctx, obj, opts...)
		}}).Build()
	f.Reconciler = &Reconciler{Client: c, hamiReader: c, args: &deschedulerconfig.MigrationControllerArgs{}, clock: f.now}
	return f
}

func (f *hamiFixture) step(t *testing.T) {
	t.Helper()
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.job), f.job))
	_, err := f.doMigrate(t.Context(), f.job)
	require.NoError(t, err)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.job), f.job))
}

func (f *hamiFixture) approveArbitration(t *testing.T) {
	t.Helper()
	util.UpdateCondition(&f.job.Status, &sev1alpha1.PodMigrationJobCondition{
		Type: hamiplacement.ArbitrationCondition, Status: sev1alpha1.PodMigrationJobConditionStatusTrue,
		Message: string(f.job.Spec.PodRef.UID),
	})
	require.NoError(t, f.Status().Update(t.Context(), f.job))
}

func (f *hamiFixture) setPhase(t *testing.T, phase string) {
	t.Helper()
	obj := gpuReservationObject(f.job)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(obj), obj))
	if phase == "Reserved" && !reservationProtected(obj) {
		obj.SetFinalizers([]string{gpuReservationProtectionFinalizer})
		require.NoError(t, f.Update(t.Context(), obj))
	}
	require.NoError(t, unstructured.SetNestedMap(obj.Object, map[string]any{"phase": phase}, "status"))
	require.NoError(t, f.Status().Update(t.Context(), obj))
	if phase != "Reserved" && reservationProtected(obj) {
		obj.SetFinalizers(nil)
		require.NoError(t, f.Update(t.Context(), obj))
	}
}

func (f *hamiFixture) replacement(t *testing.T) *corev1.Pod {
	t.Helper()
	lock, err := f.hamiLock(t.Context(), f.job)
	require.NoError(t, err)
	require.NotNil(t, lock)
	placement, err := hamiplacement.FromConfigMap(lock)
	require.NoError(t, err)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.source.Namespace, Name: "replacement", UID: "replacement-uid", OwnerReferences: f.source.OwnerReferences},
		Spec:       *f.source.Spec.DeepCopy(),
	}
	pod.Spec.NodeName = ""
	require.NoError(t, placement.Apply(pod))
	require.NoError(t, f.Create(t.Context(), pod))
	return pod
}

func TestHamiMigrationRequiresArbitrationReservationAndReady(t *testing.T) {
	f := newHamiFixture(t, false)
	reservation := gpuReservationObject(f.job)
	calls := 0
	f.hamiEvict = func(ctx context.Context, eviction *sev1alpha1.PodMigrationJob, source *corev1.Pod) error {
		calls++
		preconditions := eviction.Spec.DeleteOptions.Preconditions
		require.Equal(t, f.source.UID, *preconditions.UID)
		require.Equal(t, source.ResourceVersion, *preconditions.ResourceVersion)
		require.NoError(t, f.Get(ctx, client.ObjectKeyFromObject(reservation), reservation))
		var hold hamiSourceEviction
		require.NoError(t, json.Unmarshal([]byte(reservation.GetAnnotations()[hamiSourceEvictionAnnotation]), &hold))
		require.Equal(t, source.UID, hold.UID)
		require.Equal(t, source.ResourceVersion, hold.ResourceVersion)
		if calls == 1 {
			return apierrors.NewTooManyRequests("disruption budget blocked the eviction", 0)
		}
		return f.Delete(ctx, source)
	}
	f.job.Annotations[arbitrator.AnnotationPassedArbitration] = "true"
	require.NoError(t, f.Update(t.Context(), f.job))
	f.step(t)
	require.True(t, apierrors.IsNotFound(f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation)))
	f.approveArbitration(t)
	for range 4 {
		f.step(t)
	}
	require.Zero(t, calls)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	f.setPhase(t, "Reserved")
	f.step(t)
	require.Zero(t, calls)
	require.Equal(t, sev1alpha1.PodMigrationJobRunning, f.job.Status.Phase)
	f.step(t)
	require.Equal(t, 1, calls)
	require.Equal(t, "HamiEvictionBlocked", f.job.Status.Reason)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	require.Empty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation], "429 releases the submission hold")
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.source), &corev1.Pod{}))
	f.step(t)
	require.Equal(t, 1, calls, "a status update must not cause an immediate retry")
	f.now.Step(defaultRequeueAfter)
	f.step(t)
	require.Equal(t, 2, calls)

	pod := f.replacement(t)
	f.step(t)
	require.Equal(t, pod.UID, f.job.Status.PodRef.UID)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(pod), pod))
	require.True(t, hamiplacement.HasGate(pod))
	// A fresh reconciler can resume using the stored replacement reference.
	resumed := &Reconciler{Client: f.Client, hamiReader: f.hamiReader, args: f.args, clock: f.clock, hamiEvict: f.hamiEvict}
	f.Reconciler = resumed
	f.step(t)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(pod), pod))
	require.False(t, hamiplacement.HasGate(pod))
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	uid, _, err := unstructured.NestedString(reservation.Object, "spec", "consumerRef", "uid")
	require.NoError(t, err)
	require.Equal(t, string(pod.UID), uid)
	pod.Spec.NodeName = "target"
	pod.Annotations[hamiplacement.AnnotationAllocated] = "GPU-b,NVIDIA,1024,10:GPU-a,NVIDIA,1024,10,1:;"
	require.NoError(t, f.Update(t.Context(), pod))
	f.setPhase(t, "Consumed")
	f.step(t)
	require.Equal(t, sev1alpha1.PodMigrationJobRunning, f.job.Status.Phase)
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	require.NoError(t, f.Status().Update(t.Context(), pod))
	f.step(t)
	require.Equal(t, sev1alpha1.PodMigrationJobSucceeded, f.job.Status.Phase)
	require.Equal(t, "target", f.job.Status.NodeName)
	f.step(t)
	require.True(t, apierrors.IsNotFound(f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation)))
	lock, err := f.hamiLock(t.Context(), f.job)
	require.NoError(t, err)
	require.Nil(t, lock)
}

func TestHamiPlacementOnlyArmsWithoutCapacity(t *testing.T) {
	f := newHamiFixture(t, true)
	f.hamiEvict = func(context.Context, *sev1alpha1.PodMigrationJob, *corev1.Pod) error {
		return fmt.Errorf("unexpected eviction")
	}
	for range 5 {
		f.step(t)
	}
	_, condition := util.GetCondition(&f.job.Status, hamiplacement.ReadyCondition)
	require.NotNil(t, condition)
	require.Equal(t, sev1alpha1.PodMigrationJobConditionStatusTrue, condition.Status)
	require.Equal(t, sev1alpha1.PodMigrationJobRunning, f.job.Status.Phase)
	reservation := gpuReservationObject(f.job)
	require.True(t, apierrors.IsNotFound(f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation)))
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.source), &corev1.Pod{}))
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.source), f.source))
	f.source.Status.Phase = corev1.PodFailed
	require.NoError(t, f.Status().Update(t.Context(), f.source))
	pod := f.replacement(t)
	f.step(t)
	f.step(t)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(pod), pod))
	require.Empty(t, pod.Annotations[hamiplacement.AnnotationReservation])
	require.Equal(t, "GPU-a,GPU-b", pod.Annotations[hamiplacement.AnnotationUseUUID])
	require.True(t, hamiplacement.PinnedToNode(pod, "target"))
	require.False(t, hamiplacement.HasGate(pod))
	f.step(t)
	require.Equal(t, sev1alpha1.PodMigrationJobRunning, f.job.Status.Phase)
	pod.Spec.NodeName = "target"
	pod.Annotations[hamiplacement.AnnotationAllocated] = "GPU-a,NVIDIA,1024,10:GPU-other,NVIDIA,1024,10:;"
	require.NoError(t, f.Update(t.Context(), pod))
	f.step(t)
	require.Equal(t, sev1alpha1.PodMigrationJobFailed, f.job.Status.Phase)
	require.Equal(t, "HamiPlacementMismatch", f.job.Status.Reason)
	f.step(t)
	lock, err := f.hamiLock(t.Context(), f.job)
	require.NoError(t, err)
	require.NotNil(t, lock)
}

func TestExpiredHamiMigrationReleasesReservationWithoutEviction(t *testing.T) {
	f := newHamiFixture(t, false)
	f.approveArbitration(t)
	for range 4 {
		f.step(t)
	}
	f.setPhase(t, "Reserved")
	f.now.Step(time.Minute)
	f.step(t)
	require.Equal(t, sev1alpha1.PodMigrationJobFailed, f.job.Status.Phase)
	require.Equal(t, sev1alpha1.PodMigrationJobReasonTimeout, f.job.Status.Reason)
	f.step(t)
	reservation := gpuReservationObject(f.job)
	key := client.ObjectKeyFromObject(reservation)
	require.NoError(t, f.Get(t.Context(), key, reservation))
	require.NotNil(t, reservation.GetDeletionTimestamp())
	f.setPhase(t, "Expired")
	require.True(t, apierrors.IsNotFound(f.Get(t.Context(), key, reservation)))
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.source), &corev1.Pod{}))
}

func TestHamiPlacementRejectsReusedReplacementName(t *testing.T) {
	f := newHamiFixture(t, true)
	for range 4 {
		f.step(t)
	}
	require.NoError(t, f.Delete(t.Context(), f.source))
	pod := f.replacement(t)
	f.step(t)
	require.Equal(t, pod.UID, f.job.Status.PodRef.UID)
	require.NoError(t, f.Delete(t.Context(), pod))
	pod.UID = "different-pod-uid"
	pod.ResourceVersion = ""
	require.NoError(t, f.Create(t.Context(), pod))
	f.step(t)
	require.Equal(t, "HamiReplacementChanged", f.job.Status.Reason)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(pod), pod))
	require.True(t, hamiplacement.HasGate(pod))
}

func TestHamiMigrationObservesPauseDuringAdmissionProbe(t *testing.T) {
	f := newHamiFixture(t, false)
	f.approveArbitration(t)
	for range 4 {
		f.step(t)
	}
	f.setPhase(t, "Reserved")
	f.step(t)
	require.Equal(t, sev1alpha1.PodMigrationJobRunning, f.job.Status.Phase)
	f.hamiEvict = func(context.Context, *sev1alpha1.PodMigrationJob, *corev1.Pod) error {
		t.Fatal("paused migration must not evict its source")
		return nil
	}
	f.onProbe = func(ctx context.Context, c client.Client, _ *corev1.Pod) {
		job := &sev1alpha1.PodMigrationJob{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(f.job), job))
		job.Spec.Paused = true
		require.NoError(t, c.Update(ctx, job))
	}
	f.step(t)
	require.True(t, f.job.Spec.Paused)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.source), &corev1.Pod{}))
}

func TestHamiPlacementRequiresAdmissionForAnExplicitResourcePool(t *testing.T) {
	f := newHamiFixture(t, true)
	rs := &appsv1.ReplicaSet{}
	owner := hamiplacement.ReplicaSetOwner(f.source)
	require.NoError(t, f.Get(t.Context(), client.ObjectKey{Namespace: f.source.Namespace, Name: owner.Name}, rs))
	rs.Spec.Template.Annotations = map[string]string{"hami.io/resource-pool": "inference"}
	require.NoError(t, f.Update(t.Context(), rs))
	for range 4 {
		f.step(t)
	}
	require.Equal(t, "HamiAdmissionRejected", f.job.Status.Reason)
	require.Contains(t, f.job.Status.Message, "missing RPM authorization")
	f.onProbe = func(_ context.Context, _ client.Client, pod *corev1.Pod) {
		pod.Annotations["resource.hami.io/revision"] = "pool-revision"
		pod.Annotations["resource.hami.io/pool-selection-revision"] = "selection-revision"
	}
	f.step(t)
	require.Equal(t, sev1alpha1.PodMigrationJobRunning, f.job.Status.Phase)
}

func TestHamiAdmissionRejectionDoesNotHoldReservation(t *testing.T) {
	for _, stage := range []string{"before reservation", "pending reservation", "before eviction"} {
		t.Run(stage, func(t *testing.T) {
			f := newHamiFixture(t, false)
			f.approveArbitration(t)
			for range 3 {
				f.step(t)
			}
			reservation := gpuReservationObject(f.job)
			key := client.ObjectKeyFromObject(reservation)
			if stage != "before reservation" {
				f.step(t)
				require.NoError(t, f.Get(t.Context(), key, reservation))
			}
			if stage == "before eviction" {
				f.setPhase(t, "Reserved")
				f.step(t)
				require.Equal(t, sev1alpha1.PodMigrationJobRunning, f.job.Status.Phase)
			}
			f.hamiEvict = func(context.Context, *sev1alpha1.PodMigrationJob, *corev1.Pod) error {
				t.Fatal("rejected admission must not evict the source")
				return nil
			}
			f.probeError = apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "probe",
				fmt.Errorf("target GPU is outside the authorized resource pool"))
			f.step(t)
			require.Equal(t, "HamiAdmissionRejected", f.job.Status.Reason)
			if stage == "before eviction" {
				require.NoError(t, f.Get(t.Context(), key, reservation))
				require.NotNil(t, reservation.GetDeletionTimestamp())
				require.True(t, reservationProtected(reservation))
				f.setPhase(t, "Failed")
			}
			require.True(t, apierrors.IsNotFound(f.Get(t.Context(), key, reservation)))
			require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.source), &corev1.Pod{}))
		})
	}
}

func TestHamiAdmissionTimeoutKeepsExistingReservation(t *testing.T) {
	f := newHamiFixture(t, false)
	f.approveArbitration(t)
	for range 4 {
		f.step(t)
	}
	f.setPhase(t, "Reserved")
	f.step(t)
	reservation := gpuReservationObject(f.job)
	key := client.ObjectKeyFromObject(reservation)
	f.probeError = apierrors.NewTimeoutError("dry-run handler timed out", 0)
	f.step(t)
	require.Equal(t, "HamiAdmissionUnavailable", f.job.Status.Reason)
	require.NoError(t, f.Get(t.Context(), key, reservation))
	require.Equal(t, "Reserved", reservationPhase(reservation))
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.source), &corev1.Pod{}))
}

func TestHamiUncertainEvictionRetainsReservationAndIsNotResubmitted(t *testing.T) {
	f := armHamiMigration(t)
	reservation := gpuReservationObject(f.job)
	calls := 0
	f.hamiEvict = func(ctx context.Context, _ *sev1alpha1.PodMigrationJob, source *corev1.Pod) error {
		calls++
		// Eviction can update this condition before deleting the Pod. That
		// revision alone does not settle the outcome of the request.
		source.Status.Conditions = []corev1.PodCondition{{
			Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: "EvictionByEvictionAPI",
		}}
		require.NoError(t, f.Status().Update(ctx, source))
		return apierrors.NewTimeoutError("eviction response lost", 0)
	}
	_, err := f.doMigrate(t.Context(), f.job)
	require.True(t, apierrors.IsTimeout(err))
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	marker := reservation.GetAnnotations()[hamiSourceEvictionAnnotation]
	require.NotEmpty(t, marker)

	f.Reconciler = &Reconciler{Client: f.Client, hamiReader: f.hamiReader, args: f.args, clock: f.clock, hamiEvict: f.hamiEvict}
	f.probeError = apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "probe", fmt.Errorf("RPM rejected placement"))
	f.now.Step(defaultRequeueAfter)
	f.step(t)
	require.Equal(t, 1, calls, "a restart with unknown outcome must not submit another request")
	require.Equal(t, hamiEvictionSubmitting, f.job.Status.Reason)

	f.job.Spec.Paused = true
	require.NoError(t, f.Update(t.Context(), f.job))
	f.step(t)
	require.Equal(t, 1, calls)

	f.now.Step(f.job.Spec.TTL.Duration)
	f.step(t)
	require.Equal(t, sev1alpha1.PodMigrationJobReasonTimeout, f.job.Status.Reason)
	f.step(t)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	require.NotNil(t, reservation.GetDeletionTimestamp(), "cleanup requests cancellation")
	require.True(t, reservationProtected(reservation))
	require.Equal(t, "Reserved", reservationPhase(reservation))
	require.Equal(t, marker, reservation.GetAnnotations()[hamiSourceEvictionAnnotation], "timeout and pause cannot clear an unknown outcome")
	require.Equal(t, 1, calls)
}

func TestHamiRecoversIntentAfterCrashBeforeSubmissionCheckpoint(t *testing.T) {
	f := armHamiMigration(t)
	baseClient := f.Client.(client.WithWatch)
	reservation := gpuReservationObject(f.job)
	var staleSubmission *sev1alpha1.PodMigrationJob
	f.Client = interceptor.NewClient(baseClient, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			job, ok := obj.(*sev1alpha1.PodMigrationJob)
			if ok {
				if _, started := util.GetCondition(&job.Status, hamiEvictionStartCondition); started != nil {
					staleSubmission = job.DeepCopy()
					panic("process died before writing the submission checkpoint")
				}
			}
			return c.SubResource(subresource).Update(ctx, obj, opts...)
		},
	})
	f.hamiEvict = func(context.Context, *sev1alpha1.PodMigrationJob, *corev1.Pod) error {
		t.Fatal("recovery must not send the abandoned attempt")
		return nil
	}
	require.PanicsWithValue(t, "process died before writing the submission checkpoint", func() {
		_, _ = f.doMigrate(t.Context(), f.job)
	})
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	require.NotEmpty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation])

	f.Reconciler = &Reconciler{Client: baseClient, hamiReader: f.hamiReader, args: f.args, clock: f.clock, hamiEvict: f.hamiEvict}
	f.step(t)
	require.Equal(t, "HamiEvictionDeferred", f.job.Status.Reason)
	require.True(t, apierrors.IsConflict(f.Status().Update(t.Context(), staleSubmission)), "recovery must fence a competing submitter")
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	require.Empty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation])
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.source), &corev1.Pod{}))
}

func TestHamiKeepsIntentAfterCrashAfterSubmissionCheckpoint(t *testing.T) {
	f := armHamiMigration(t)
	baseClient := f.Client.(client.WithWatch)
	reservation := gpuReservationObject(f.job)
	f.Client = interceptor.NewClient(baseClient, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if err := c.SubResource(subresource).Update(ctx, obj, opts...); err != nil {
				return err
			}
			if job, ok := obj.(*sev1alpha1.PodMigrationJob); ok {
				if _, started := util.GetCondition(&job.Status, hamiEvictionStartCondition); started != nil {
					panic("process died after writing the submission checkpoint")
				}
			}
			return nil
		},
	})
	f.hamiEvict = func(context.Context, *sev1alpha1.PodMigrationJob, *corev1.Pod) error {
		t.Fatal("an authorized attempt with an unknown outcome must not be sent on restart")
		return nil
	}
	require.PanicsWithValue(t, "process died after writing the submission checkpoint", func() {
		_, _ = f.doMigrate(t.Context(), f.job)
	})
	key := client.ObjectKeyFromObject(reservation)
	require.NoError(t, f.Get(t.Context(), key, reservation))
	marker := reservation.GetAnnotations()[hamiSourceEvictionAnnotation]
	require.NotEmpty(t, marker)

	f.Reconciler = &Reconciler{Client: baseClient, hamiReader: f.hamiReader, args: f.args, clock: f.clock, hamiEvict: f.hamiEvict}
	f.now.Step(defaultRequeueAfter)
	f.step(t)
	require.Equal(t, hamiEvictionSubmitting, f.job.Status.Reason)
	require.NoError(t, f.Get(t.Context(), key, reservation))
	require.Equal(t, marker, reservation.GetAnnotations()[hamiSourceEvictionAnnotation])
	require.Equal(t, "Reserved", reservationPhase(reservation))
	require.True(t, reservationProtected(reservation))
}

func TestHamiEvictionDoesNotSubmitAfterCheckpointCrossesDeadline(t *testing.T) {
	f := armHamiMigration(t)
	crossedDeadline := false
	f.Client = interceptor.NewClient(f.Client.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if job, ok := obj.(*sev1alpha1.PodMigrationJob); ok && !crossedDeadline {
				if _, started := util.GetCondition(&job.Status, hamiEvictionStartCondition); started != nil {
					f.now.Step(f.job.Spec.TTL.Duration)
					crossedDeadline = true
				}
			}
			return c.SubResource(subresource).Update(ctx, obj, opts...)
		},
	})
	f.hamiEvict = func(context.Context, *sev1alpha1.PodMigrationJob, *corev1.Pod) error {
		t.Fatal("no eviction may be sent after the deadline")
		return nil
	}
	f.step(t)
	require.True(t, crossedDeadline)
	require.Equal(t, "HamiEvictionDeferred", f.job.Status.Reason)
	reservation := gpuReservationObject(f.job)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	require.Empty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation])
	f.step(t)
	require.Equal(t, sev1alpha1.PodMigrationJobReasonTimeout, f.job.Status.Reason)
}

func TestHamiEvictionRejectionRetainsIntentUntilOutcomePersists(t *testing.T) {
	f := armHamiMigration(t)
	f.hamiEvict = func(context.Context, *sev1alpha1.PodMigrationJob, *corev1.Pod) error {
		return apierrors.NewTooManyRequests("PDB rejected the eviction", 0)
	}
	f.Client = interceptor.NewClient(f.Client.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if job, ok := obj.(*sev1alpha1.PodMigrationJob); ok && job.Status.Reason == "HamiEvictionBlocked" {
				return apierrors.NewTimeoutError("could not persist PDB rejection", 0)
			}
			return c.SubResource(subresource).Update(ctx, obj, opts...)
		},
	})
	_, err := f.doMigrate(t.Context(), f.job)
	require.True(t, apierrors.IsTimeout(err))
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.job), f.job))
	require.Equal(t, hamiEvictionSubmitting, f.job.Status.Reason)
	require.Contains(t, f.job.Finalizers, hamiEvictionRecoveryFinalizer)
	reservation := gpuReservationObject(f.job)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	require.NotEmpty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation])
}

func TestHamiEvictionRecoversRecordedRejectionWhilePaused(t *testing.T) {
	f := armHamiMigration(t)
	reservation := gpuReservationObject(f.job)
	calls := 0
	f.hamiEvict = func(context.Context, *sev1alpha1.PodMigrationJob, *corev1.Pod) error {
		calls++
		return apierrors.NewTooManyRequests("PDB rejected the eviction", 0)
	}
	baseClient := f.Client.(client.WithWatch)
	f.Client = interceptor.NewClient(baseClient, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if obj.GetObjectKind().GroupVersionKind() == gpuReservationGVK && obj.GetAnnotations()[hamiSourceEvictionAnnotation] == "" {
				return apierrors.NewTimeoutError("failed to clear the intent", 0)
			}
			return c.Update(ctx, obj, opts...)
		},
	})
	_, err := f.doMigrate(t.Context(), f.job)
	require.True(t, apierrors.IsTimeout(err))
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.job), f.job))
	require.Equal(t, "HamiEvictionBlocked", f.job.Status.Reason)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	require.NotEmpty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation])

	// The restart only has the durable PMJ outcome and reservation.
	f.Reconciler = &Reconciler{Client: baseClient, hamiReader: f.hamiReader, args: f.args, clock: f.clock, hamiEvict: f.hamiEvict}
	f.job.Spec.Paused = true
	require.NoError(t, f.Update(t.Context(), f.job))
	f.step(t)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	require.Empty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation])
	f.step(t)
	require.NotContains(t, f.job.Finalizers, hamiEvictionRecoveryFinalizer)
	require.Equal(t, 1, calls)
}

func TestHamiMigrationDeletionWaitsForEvictionRecovery(t *testing.T) {
	f := armHamiMigration(t)
	f.hamiEvict = func(context.Context, *sev1alpha1.PodMigrationJob, *corev1.Pod) error {
		return apierrors.NewTimeoutError("eviction response lost", 0)
	}
	_, err := f.doMigrate(t.Context(), f.job)
	require.True(t, apierrors.IsTimeout(err))
	require.NoError(t, f.Delete(t.Context(), f.job))
	f.step(t)
	require.False(t, f.job.DeletionTimestamp.IsZero())
	require.Contains(t, f.job.Finalizers, hamiEvictionRecoveryFinalizer)
	reservation := gpuReservationObject(f.job)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	require.NotEmpty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation])
	require.True(t, reservationProtected(reservation))

	require.NoError(t, f.Delete(t.Context(), f.source))
	f.step(t)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	require.Empty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation])
	_, err = f.doMigrate(t.Context(), f.job)
	require.NoError(t, err)
	require.True(t, apierrors.IsNotFound(f.Get(t.Context(), client.ObjectKeyFromObject(f.job), f.job)))
}

func TestHamiRecoversEvictionAfterSourceDisappears(t *testing.T) {
	f := armHamiMigration(t)
	f.hamiEvict = func(ctx context.Context, _ *sev1alpha1.PodMigrationJob, source *corev1.Pod) error {
		require.NoError(t, f.Delete(ctx, source))
		return apierrors.NewTimeoutError("eviction response lost", 0)
	}
	_, err := f.doMigrate(t.Context(), f.job)
	require.True(t, apierrors.IsTimeout(err))
	reservation := gpuReservationObject(f.job)
	key := client.ObjectKeyFromObject(reservation)
	require.NoError(t, f.Get(t.Context(), key, reservation))
	require.NotEmpty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation])

	f.Reconciler = &Reconciler{Client: f.Client, hamiReader: f.hamiReader, args: f.args, clock: f.clock}
	pod := f.replacement(t)
	f.step(t)
	require.NoError(t, f.Get(t.Context(), key, reservation))
	require.Empty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation])
	require.Equal(t, "Reserved", reservationPhase(reservation))
	f.step(t)
	require.Equal(t, pod.UID, f.job.Status.PodRef.UID)
}

func armHamiMigration(t *testing.T) *hamiFixture {
	t.Helper()
	f := newHamiFixture(t, false)
	f.approveArbitration(t)
	for range 4 {
		f.step(t)
	}
	f.setPhase(t, "Reserved")
	f.step(t)
	return f
}

func TestHamiAdmissionReleaseConflictsWithConcurrentConsumer(t *testing.T) {
	f := newHamiFixture(t, false)
	f.approveArbitration(t)
	for range 4 {
		f.step(t)
	}
	f.setPhase(t, "Reserved")
	key := client.ObjectKeyFromObject(gpuReservationObject(f.job))
	before := gpuReservationObject(f.job)
	require.NoError(t, f.Get(t.Context(), key, before))
	f.Client = interceptor.NewClient(f.Client.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			req := (&client.DeleteOptions{}).ApplyOptions(opts)
			require.Equal(t, before.GetUID(), *req.Preconditions.UID)
			require.Equal(t, before.GetResourceVersion(), *req.Preconditions.ResourceVersion)
			updated := before.DeepCopy()
			require.NoError(t, unstructured.SetNestedMap(updated.Object, map[string]any{"name": "replacement", "uid": "replacement-uid"}, "spec", "consumerRef"))
			require.NoError(t, c.Update(ctx, updated))
			return c.Delete(ctx, obj, opts...)
		},
	})
	require.True(t, apierrors.IsConflict(f.releaseHamiReservation(t.Context(), f.job, true)))
	stored := gpuReservationObject(f.job)
	require.NoError(t, f.Get(t.Context(), key, stored))
	uid, _, err := unstructured.NestedString(stored.Object, "spec", "consumerRef", "uid")
	require.NoError(t, err)
	require.Equal(t, "replacement-uid", uid)
}

func TestHamiMigrationRejectsGrantWithoutSchedulerProtection(t *testing.T) {
	f := newHamiFixture(t, false)
	f.approveArbitration(t)
	for range 4 {
		f.step(t)
	}
	f.setPhase(t, "Reserved")
	reservation := gpuReservationObject(f.job)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	reservation.SetFinalizers(nil)
	require.NoError(t, f.Update(t.Context(), reservation))
	f.step(t)
	require.Equal(t, "GPUReservationUnprotected", f.job.Status.Reason)
	require.Equal(t, sev1alpha1.PodMigrationJobFailed, f.job.Status.Phase)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.source), &corev1.Pod{}))
}

func TestHamiCancelAssignedReservationLeavesFinalizationToScheduler(t *testing.T) {
	for _, finalPhase := range []string{"Consumed", "Failed"} {
		t.Run(finalPhase, func(t *testing.T) {
			f := newHamiFixture(t, false)
			f.approveArbitration(t)
			for range 4 {
				f.step(t)
			}
			f.setPhase(t, "Reserved")
			reservation := gpuReservationObject(f.job)
			key := client.ObjectKeyFromObject(reservation)
			require.NoError(t, f.Get(t.Context(), key, reservation))
			consumer := map[string]any{"name": "replacement", "uid": "replacement-uid"}
			require.NoError(t, unstructured.SetNestedMap(reservation.Object, consumer, "spec", "consumerRef"))
			require.NoError(t, f.Update(t.Context(), reservation))

			require.NoError(t, f.releaseHamiReservation(t.Context(), f.job, false))
			require.NoError(t, f.Get(t.Context(), key, reservation))
			require.NotNil(t, reservation.GetDeletionTimestamp())
			require.True(t, reservationProtected(reservation))
			require.Equal(t, "Reserved", reservationPhase(reservation))
			require.NoError(t, f.releaseHamiReservation(t.Context(), f.job, false))

			f.setPhase(t, finalPhase)
			require.True(t, apierrors.IsNotFound(f.Get(t.Context(), key, reservation)))
		})
	}
}

func TestHamiEvictionRechecksReservationAfterPersistingIntent(t *testing.T) {
	f := newHamiFixture(t, false)
	f.approveArbitration(t)
	for range 4 {
		f.step(t)
	}
	f.setPhase(t, "Reserved")
	f.step(t)
	f.hamiEvict = func(context.Context, *sev1alpha1.PodMigrationJob, *corev1.Pod) error {
		t.Fatal("withdrawing the grant must prevent eviction")
		return nil
	}
	reservation := gpuReservationObject(f.job)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	reads := 0
	f.hamiReader = interceptor.NewClient(f.Client.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if obj.GetObjectKind().GroupVersionKind() == gpuReservationGVK {
				reads++
				if reads == 2 {
					require.NoError(t, c.Delete(ctx, reservation))
				}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	f.step(t)
	require.Equal(t, "HamiEvictionDeferred", f.job.Status.Reason)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.source), &corev1.Pod{}))
}

func TestHamiCancellationWinsBetweenReservationAndPMJReads(t *testing.T) {
	for _, finalize := range []bool{false, true} {
		t.Run(fmt.Sprint("fully-deleted=", finalize), func(t *testing.T) {
			f := armHamiMigration(t)
			reservation := gpuReservationObject(f.job)
			key := client.ObjectKeyFromObject(reservation)
			require.NoError(t, f.Get(t.Context(), key, reservation))
			cancelled := false
			f.hamiReader = interceptor.NewClient(f.Client.(client.WithWatch), interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, readingJob := obj.(*sev1alpha1.PodMigrationJob); readingJob && !cancelled {
						cancelled = true
						require.NoError(t, c.Delete(ctx, reservation))
						if finalize {
							require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(reservation), reservation))
							reservation.SetFinalizers(nil)
							require.NoError(t, c.Update(ctx, reservation))
						}
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			f.hamiEvict = func(context.Context, *sev1alpha1.PodMigrationJob, *corev1.Pod) error {
				t.Fatal("cancellation won the reservation update, so no eviction may be sent")
				return nil
			}
			f.step(t)
			require.True(t, cancelled)
			require.Equal(t, "HamiEvictionDeferred", f.job.Status.Reason)
			require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.source), &corev1.Pod{}))
		})
	}
}

func TestHamiLostReservationCommitResponseDoesNotEvict(t *testing.T) {
	f := armHamiMigration(t)
	reservation := gpuReservationObject(f.job)
	f.Client = interceptor.NewClient(f.Client.(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			err := c.Update(ctx, obj, opts...)
			if err == nil && obj.GetObjectKind().GroupVersionKind() == gpuReservationGVK &&
				obj.GetAnnotations()[hamiSourceEvictionAnnotation] != "" {
				return apierrors.NewTimeoutError("reservation update response lost", 0)
			}
			return err
		},
	})
	f.hamiEvict = func(context.Context, *sev1alpha1.PodMigrationJob, *corev1.Pod) error {
		t.Fatal("without an acknowledged reservation commit, no eviction may be sent")
		return nil
	}
	_, err := f.doMigrate(t.Context(), f.job)
	require.True(t, apierrors.IsTimeout(err))
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	require.Empty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation], "this caller never sent an eviction")
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(f.source), &corev1.Pod{}))
}

func TestHamiEvictionCommitsBeforeCancellation(t *testing.T) {
	f := armHamiMigration(t)
	reservation := gpuReservationObject(f.job)
	key := client.ObjectKeyFromObject(reservation)
	require.NoError(t, f.Get(t.Context(), key, reservation))
	oldVersion, uid := reservation.GetResourceVersion(), reservation.GetUID()
	calls := 0
	f.hamiEvict = func(ctx context.Context, _ *sev1alpha1.PodMigrationJob, source *corev1.Pod) error {
		calls++
		err := f.Delete(ctx, reservation, client.Preconditions{UID: &uid, ResourceVersion: &oldVersion})
		require.True(t, apierrors.IsConflict(err), "cancellation from the old grant version loses")
		require.NoError(t, f.Get(ctx, key, reservation))
		version := reservation.GetResourceVersion()
		require.NoError(t, f.Delete(ctx, reservation, client.Preconditions{UID: &uid, ResourceVersion: &version}))
		require.NoError(t, f.Get(ctx, key, reservation))
		require.NotNil(t, reservation.GetDeletionTimestamp())
		require.True(t, reservationProtected(reservation))
		require.NotEmpty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation])
		return f.Delete(ctx, source)
	}
	f.step(t)
	require.Equal(t, 1, calls)
	require.Equal(t, sev1alpha1.PodMigrationJobReasonEvicting, f.job.Status.Reason)
	require.NoError(t, f.Get(t.Context(), key, reservation))
	require.Empty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation], "accepted eviction settles the hold")
}

func TestHamiEvictionObservesPauseAfterReservationCommit(t *testing.T) {
	f := armHamiMigration(t)
	reservation := gpuReservationObject(f.job)
	paused := false
	f.hamiReader = interceptor.NewClient(f.Client.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if job, readingJob := obj.(*sev1alpha1.PodMigrationJob); readingJob && !paused {
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(reservation), reservation))
				if reservation.GetAnnotations()[hamiSourceEvictionAnnotation] != "" {
					require.NoError(t, c.Get(ctx, key, job))
					job.Spec.Paused = true
					require.NoError(t, c.Update(ctx, job))
					paused = true
				}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	f.hamiEvict = func(context.Context, *sev1alpha1.PodMigrationJob, *corev1.Pod) error {
		t.Fatal("pause observed before submitting the eviction")
		return nil
	}
	f.step(t)
	require.True(t, paused)
	require.True(t, f.job.Spec.Paused)
	require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(reservation), reservation))
	require.Empty(t, reservation.GetAnnotations()[hamiSourceEvictionAnnotation])
}

func TestHamiReplacementRechecksReservationBeforeAssigningConsumer(t *testing.T) {
	for _, event := range []string{"cancelled", "expired"} {
		t.Run(event, func(t *testing.T) {
			f := newHamiFixture(t, false)
			f.approveArbitration(t)
			for range 4 {
				f.step(t)
			}
			f.setPhase(t, "Reserved")
			f.step(t)
			f.hamiEvict = func(ctx context.Context, _ *sev1alpha1.PodMigrationJob, source *corev1.Pod) error {
				return f.Delete(ctx, source)
			}
			f.step(t)
			pod := f.replacement(t)
			f.step(t)
			require.Equal(t, pod.UID, f.job.Status.PodRef.UID)

			reservation := gpuReservationObject(f.job)
			key := client.ObjectKeyFromObject(reservation)
			require.NoError(t, f.Get(t.Context(), key, reservation))
			reads := 0
			f.hamiReader = interceptor.NewClient(f.Client.(client.WithWatch), interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if obj.GetObjectKind().GroupVersionKind() == gpuReservationGVK {
						reads++
						if reads == 2 {
							if event == "cancelled" {
								require.NoError(t, c.Delete(ctx, reservation))
							} else {
								f.now.Step(time.Minute)
							}
						}
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			f.step(t)
			require.Equal(t, 2, reads)
			require.NoError(t, f.Get(t.Context(), key, reservation))
			_, assigned, err := unstructured.NestedMap(reservation.Object, "spec", "consumerRef")
			require.NoError(t, err)
			require.False(t, assigned)
			require.NoError(t, f.Get(t.Context(), client.ObjectKeyFromObject(pod), pod))
			require.True(t, hamiplacement.HasGate(pod))
		})
	}
}
