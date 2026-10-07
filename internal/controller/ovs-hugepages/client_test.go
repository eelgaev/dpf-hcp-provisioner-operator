/*
Copyright 2025.

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

package ovshugepages

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("newHostedClusterClient", func() {
	var scheme *runtime.Scheme

	BeforeEach(func() {
		scheme = runtime.NewScheme()
		Expect(corev1.AddToScheme(scheme)).To(Succeed())
	})

	It("builds a fresh client from the admin kubeconfig secret", func() {
		ctx := context.Background()
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "hc-admin-kubeconfig",
			},
			Data: map[string][]byte{
				"kubeconfig": kubeconfigForToken("token-a"),
			},
		}
		mgmtClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

		// Does not cache: each call reads the secret and builds a new client.
		first, err := newHostedClusterClient(ctx, mgmtClient, "ns", "hc")
		Expect(err).NotTo(HaveOccurred())
		Expect(first).NotTo(BeNil())

		second, err := newHostedClusterClient(ctx, mgmtClient, "ns", "hc")
		Expect(err).NotTo(HaveOccurred())
		Expect(second).NotTo(BeNil())
		Expect(second).NotTo(BeIdenticalTo(first))
	})

	It("fails when the admin kubeconfig secret is missing", func() {
		ctx := context.Background()
		mgmtClient := fake.NewClientBuilder().WithScheme(scheme).Build()

		_, err := newHostedClusterClient(ctx, mgmtClient, "ns", "hc")
		Expect(err).To(HaveOccurred())
	})
})

func kubeconfigForToken(token string) []byte {
	return []byte(`apiVersion: v1
kind: Config
current-context: ctx
clusters:
- name: cluster
  cluster:
    server: https://127.0.0.1:6443
contexts:
- name: ctx
  context:
    cluster: cluster
    user: user
users:
- name: user
  user:
    token: ` + token + `
`)
}
