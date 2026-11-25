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

package arbitrator

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	discoveryfake "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/koordinator-sh/koordinator/apis/extension"
	"github.com/koordinator-sh/koordinator/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/apis/config"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/controllerfinder"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/controllers/migration/hamiplacement"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/fieldindex"
	"github.com/koordinator-sh/koordinator/pkg/descheduler/framework"
)

type hamiArbitrationHandle struct {
	framework.Handle
	kube      kubernetes.Interface
	informers informers.SharedInformerFactory
}

func (h hamiArbitrationHandle) ClientSet() kubernetes.Interface     { return h.kube }
func (h hamiArbitrationHandle) EventRecorder() events.EventRecorder { return nil }
func (h hamiArbitrationHandle) GetPodsAssignedToNodeFunc() framework.GetPodsAssignedToNodeFunc {
	return nil
}
func (h hamiArbitrationHandle) SharedInformerFactory() informers.SharedInformerFactory {
	return h.informers
}

type twoReplicaWorkload struct {
	controllerfinder.Interface
	pods []*corev1.Pod
}

func (w twoReplicaWorkload) GetPodsForRef(*metav1.OwnerReference, string, *metav1.LabelSelector, bool) ([]*corev1.Pod, int32, error) {
	return w.pods, int32(len(w.pods)), nil
}

func TestHamiKServeEmptyDirArbitration(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "gpu-recovery-demo", Name: "demo-small", Labels: map[string]string{
			hamiplacement.RecoveryEvictableLabel: "true",
		}},
		Spec: corev1.PodSpec{
			SchedulerName: hamiplacement.SchedulerName,
			Volumes: []corev1.Volume{
				{Name: "home", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				{Name: "dshm", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}},
				{Name: "model-cache", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				{Name: "tmp-dir", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				{Name: "tls-certs", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{}}},
			},
		},
	}
	checkHamiKServeArbitration(t, pod)
}

func checkHamiKServeArbitration(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	pod.UID = "source-uid"
	pod.Spec.NodeName = "node-c"
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "demo-small-kserve", UID: "rs-uid", Controller: ptr.To(true),
	}}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	job := &v1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Name: "health", UID: "job-uid", Annotations: map[string]string{
			hamiplacement.AnnotationNode: "node-a", hamiplacement.AnnotationUUIDs: "GPU-a",
		}},
		Spec: v1alpha1.PodMigrationJobSpec{
			Mode:   v1alpha1.PodMigrationJobModeEvictionDirectly,
			PodRef: &corev1.ObjectReference{UID: pod.UID, Namespace: pod.Namespace, Name: pod.Name},
		},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	kube := kubefake.NewSimpleClientset()
	kube.Discovery().(*discoveryfake.FakeDiscovery).Resources = []*metav1.APIResourceList{
		{GroupVersion: "policy/v1", APIResources: []metav1.APIResource{{Name: "poddisruptionbudgets"}}},
		{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "pods/eviction", Kind: "Eviction", Group: "policy", Version: "v1"}}},
	}
	handle := hamiArbitrationHandle{kube: kube, informers: informers.NewSharedInformerFactory(kube, 0)}

	for _, tc := range []struct {
		name   string
		change func(*v1alpha1.PodMigrationJob, *corev1.Pod, *corev1.Namespace)
		busy   bool
		failed bool
		passed bool
	}{
		{name: "authorized health PMJ", passed: true},
		{name: "ordinary PMJ", change: func(job *v1alpha1.PodMigrationJob, _ *corev1.Pod, _ *corev1.Namespace) {
			job.Annotations = nil
		}, failed: true},
		{name: "source not opted in", change: func(_ *v1alpha1.PodMigrationJob, pod *corev1.Pod, _ *corev1.Namespace) {
			delete(pod.Labels, hamiplacement.RecoveryEvictableLabel)
		}, failed: true},
		{name: "namespace not opted in", change: func(_ *v1alpha1.PodMigrationJob, _ *corev1.Pod, ns *corev1.Namespace) {
			ns.Labels = nil
		}, failed: true},
		{name: "placement-only has no exemption", change: func(job *v1alpha1.PodMigrationJob, _ *corev1.Pod, _ *corev1.Namespace) {
			job.Annotations[hamiplacement.AnnotationPlacementOnly] = "true"
		}, failed: true},
		{name: "emptyDir with hostPath", change: func(_ *v1alpha1.PodMigrationJob, pod *corev1.Pod, _ *corev1.Namespace) {
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "local", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/data"}}})
		}, failed: true},
		{name: "emptyDir with persistent claim", change: func(_ *v1alpha1.PodMigrationJob, pod *corev1.Pod, _ *corev1.Namespace) {
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "persistent", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "models"}}})
		}, failed: true},
		{name: "eviction cost still enforced", change: func(_ *v1alpha1.PodMigrationJob, pod *corev1.Pod, _ *corev1.Namespace) {
			pod.Annotations = map[string]string{extension.AnnotationEvictionCost: "2147483647"}
		}, failed: true},
		{name: "critical priority still enforced", change: func(_ *v1alpha1.PodMigrationJob, pod *corev1.Pod, _ *corev1.Namespace) {
			pod.Spec.Priority = ptr.To(int32(2000000000))
		}, failed: true},
		{name: "migration limit still enforced", busy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod, job := pod.DeepCopy(), job.DeepCopy()
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: pod.Namespace, Labels: map[string]string{hamiplacement.ManagedNamespaceLabel: "true"}}}
			if tc.change != nil {
				tc.change(job, pod, ns)
			}
			sibling := pod.DeepCopy()
			sibling.Name, sibling.UID = "sibling", "sibling-uid"
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, pod, sibling).
				WithIndex(&v1alpha1.PodMigrationJob{}, fieldindex.IndexJobByPodNamespace, func(obj client.Object) []string {
					return []string{obj.(*v1alpha1.PodMigrationJob).Spec.PodRef.Namespace}
				})
			if tc.busy {
				existing := job.DeepCopy()
				existing.Name, existing.UID = "existing", "existing-uid"
				existing.Spec.PodRef.Name, existing.Spec.PodRef.UID = sibling.Name, sibling.UID
				existing.Status.Phase = v1alpha1.PodMigrationJobRunning
				builder.WithObjects(existing)
			}
			c := builder.Build()
			args := &config.MigrationControllerArgs{
				MaxMigratingPerWorkload:   ptr.To(intstr.FromInt(1)),
				MaxUnavailablePerWorkload: ptr.To(intstr.FromInt(1)),
			}
			f := &filter{client: c, args: args, controllerFinder: twoReplicaWorkload{pods: []*corev1.Pod{pod, sibling}}}
			require.NoError(t, f.initFilters(args, handle))
			require.False(t, f.nonRetryablePodFilter(pod), "default eviction policy must retain its storage check")
			arb := &arbitratorImpl{filter: f}
			failed, passed := arb.filtering(job, pod)
			require.Equal(t, tc.failed, failed)
			require.Equal(t, tc.passed, passed)
		})
	}
}
