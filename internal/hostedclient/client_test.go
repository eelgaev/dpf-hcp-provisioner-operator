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

package hostedclient

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("GetHostedClusterClient", func() {
	It("reuses the client until the admin kubeconfig secret changes", func() {
		ctx := context.Background()
		scheme := runtime.NewScheme()
		Expect(corev1.AddToScheme(scheme)).To(Succeed())

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
		cm := NewClientManager(mgmtClient)

		first, err := cm.GetHostedClusterClient(ctx, "ns", "hc")
		Expect(err).NotTo(HaveOccurred())
		second, err := cm.GetHostedClusterClient(ctx, "ns", "hc")
		Expect(err).NotTo(HaveOccurred())
		Expect(second).To(BeIdenticalTo(first))

		Expect(mgmtClient.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "hc-admin-kubeconfig"}, secret)).To(Succeed())
		secret.Data["kubeconfig"] = kubeconfigForToken("token-b")
		Expect(mgmtClient.Update(ctx, secret)).To(Succeed())

		third, err := cm.GetHostedClusterClient(ctx, "ns", "hc")
		Expect(err).NotTo(HaveOccurred())
		Expect(third).NotTo(BeIdenticalTo(first))
	})
})

func kubeconfigForToken(token string) []byte {
	return []byte(fmt.Sprintf(`apiVersion: v1
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
    token: %s
`, token))
}
