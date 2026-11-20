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

package evictor

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	sev1alpha1 "github.com/koordinator-sh/koordinator/apis/scheduling/v1alpha1"
)

type evictionRoundTripper func(*http.Request) (*http.Response, error)

func (f evictionRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestNativeEvictOnceReturnsFirstResponseWithRetryAfter(t *testing.T) {
	for _, status := range []struct {
		code   int
		reason metav1.StatusReason
	}{
		{http.StatusTooManyRequests, metav1.StatusReasonTooManyRequests},
		{http.StatusServiceUnavailable, metav1.StatusReasonServiceUnavailable},
	} {
		t.Run(string(status.reason), func(t *testing.T) {
			calls := 0
			source := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: "workloads", UID: types.UID("source-uid"), ResourceVersion: "23"}}
			hc := &http.Client{Transport: evictionRoundTripper(func(req *http.Request) (*http.Response, error) {
				calls++
				require.Equal(t, http.MethodPost, req.Method)
				require.Equal(t, "/api/v1/namespaces/workloads/pods/source/eviction", req.URL.Path)
				var eviction policyv1.Eviction
				require.NoError(t, json.NewDecoder(req.Body).Decode(&eviction))
				require.Equal(t, source.UID, *eviction.DeleteOptions.Preconditions.UID)
				require.Equal(t, source.ResourceVersion, *eviction.DeleteOptions.Preconditions.ResourceVersion)
				return &http.Response{
					StatusCode: status.code,
					Header:     http.Header{"Content-Type": {"application/json"}, "Retry-After": {"0"}},
					Body: io.NopCloser(strings.NewReader(fmt.Sprintf(
						`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":%q,"code":%d}`, status.reason, status.code))),
				}, nil
			})}
			client, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: "https://kubernetes.invalid"}, hc)
			require.NoError(t, err)
			native := &NativeEvictor{client: client}
			job := &sev1alpha1.PodMigrationJob{Spec: sev1alpha1.PodMigrationJobSpec{DeleteOptions: &metav1.DeleteOptions{
				Preconditions: &metav1.Preconditions{UID: &source.UID, ResourceVersion: &source.ResourceVersion},
			}}}
			require.Equal(t, status.reason, apierrors.ReasonForError(native.evictOnce(t.Context(), job, source)))
			require.Equal(t, 1, calls)
		})
	}
}
