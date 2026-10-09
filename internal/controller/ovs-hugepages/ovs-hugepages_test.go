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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

var _ = Describe("buildDaemonSet", func() {
	It("reserves the configured hugepages amount on every node with no nodeSelector", func() {
		ds := buildDaemonSet(DefaultHugepagesSize, DefaultHugepagesAmount)

		Expect(ds.Name).To(Equal(DaemonSetName))
		Expect(ds.Namespace).To(Equal("openshift-doca-hugepages-holder"))

		podSpec := ds.Spec.Template.Spec
		// No nodeSelector: every node of the hosted cluster is a DPU.
		Expect(podSpec.NodeSelector).To(BeEmpty())
		// Tolerate all taints so the reservation lands on every node.
		Expect(podSpec.Tolerations).To(HaveLen(1))
		Expect(podSpec.Tolerations[0].Operator).To(Equal(corev1.TolerationOpExists))

		Expect(podSpec.Containers).To(HaveLen(1))
		c := podSpec.Containers[0]
		Expect(c.Name).To(Equal(containerName))
		Expect(c.Image).To(Equal(DummyPodImage))
		// The pause image idles on its own entrypoint — no command/shell needed.
		Expect(c.Command).To(BeEmpty())

		resourceName := corev1.ResourceName("hugepages-" + DefaultHugepagesSize)
		// 250 pages x 2Mi = 500Mi.
		want := resource.MustParse("500Mi")
		req := c.Resources.Requests[resourceName]
		lim := c.Resources.Limits[resourceName]
		// Hugepages requests must equal limits.
		Expect(req.Equal(want)).To(BeTrue(), "request should be 500Mi")
		Expect(lim.Equal(want)).To(BeTrue(), "limit should be 500Mi")
	})

	It("computes amount x size for a custom size and page count", func() {
		// 4 pages x 1Gi = 4Gi.
		ds := buildDaemonSet("1Gi", 4)
		c := ds.Spec.Template.Spec.Containers[0]

		resourceName := corev1.ResourceName("hugepages-1Gi")
		want := resource.MustParse("4Gi")
		req := c.Resources.Requests[resourceName]
		Expect(req.Equal(want)).To(BeTrue(), "request should be 4Gi of hugepages-1Gi")
	})
})

var _ = Describe("ensureNamespace", func() {
	var (
		ctx context.Context
		cs  kubernetes.Interface
	)

	BeforeEach(func() {
		ctx = context.Background()
		cs = fake.NewSimpleClientset()
	})

	It("creates the reservation namespace when missing and is idempotent", func() {
		Expect(ensureNamespace(ctx, cs)).To(Succeed())

		ns, err := cs.CoreV1().Namespaces().Get(ctx, OVSHugepagesNamespace, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(ns.Name).To(Equal("openshift-doca-hugepages-holder"))

		Expect(ensureNamespace(ctx, cs)).To(Succeed())
	})
})

var _ = Describe("ensureDaemonSet", func() {
	var (
		ctx context.Context
		cs  kubernetes.Interface
	)

	BeforeEach(func() {
		ctx = context.Background()
		cs = fake.NewSimpleClientset(&corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: OVSHugepagesNamespace},
		})
	})

	It("creates the DaemonSet on first call", func() {
		Expect(ensureDaemonSet(ctx, cs, DefaultHugepagesSize, DefaultHugepagesAmount)).To(Succeed())

		ds, err := cs.AppsV1().DaemonSets(OVSHugepagesNamespace).Get(ctx, DaemonSetName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(ds.Name).To(Equal(DaemonSetName))
	})

	It("does not modify the DaemonSet on an unchanged subsequent call", func() {
		Expect(ensureDaemonSet(ctx, cs, DefaultHugepagesSize, DefaultHugepagesAmount)).To(Succeed())

		before, err := cs.AppsV1().DaemonSets(OVSHugepagesNamespace).Get(ctx, DaemonSetName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		Expect(ensureDaemonSet(ctx, cs, DefaultHugepagesSize, DefaultHugepagesAmount)).To(Succeed())

		after, err := cs.AppsV1().DaemonSets(OVSHugepagesNamespace).Get(ctx, DaemonSetName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		// No write happened, so the resourceVersion is unchanged.
		Expect(after.ResourceVersion).To(Equal(before.ResourceVersion))

		// Still exactly one DaemonSet.
		list, err := cs.AppsV1().DaemonSets(OVSHugepagesNamespace).List(ctx, metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(list.Items).To(HaveLen(1))
	})

	It("updates the DaemonSet when the reservation amount changes", func() {
		Expect(ensureDaemonSet(ctx, cs, DefaultHugepagesSize, DefaultHugepagesAmount)).To(Succeed())
		Expect(ensureDaemonSet(ctx, cs, DefaultHugepagesSize, DefaultHugepagesAmount*2)).To(Succeed())

		ds, err := cs.AppsV1().DaemonSets(OVSHugepagesNamespace).Get(ctx, DaemonSetName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		resourceName := corev1.ResourceName("hugepages-" + DefaultHugepagesSize)
		// 500 pages x 2Mi = 1000Mi.
		want := resource.MustParse("1000Mi")
		req := ds.Spec.Template.Spec.Containers[0].Resources.Requests[resourceName]
		Expect(req.Equal(want)).To(BeTrue(), "request should reflect the doubled amount")
	})

	It("deletes the DaemonSet when amount is zero", func() {
		Expect(ensureDaemonSet(ctx, cs, DefaultHugepagesSize, DefaultHugepagesAmount)).To(Succeed())

		Expect(ensureDaemonSet(ctx, cs, DefaultHugepagesSize, 0)).To(Succeed())

		_, err := cs.AppsV1().DaemonSets(OVSHugepagesNamespace).Get(ctx, DaemonSetName, metav1.GetOptions{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("is a no-op when amount is zero and nothing exists", func() {
		Expect(ensureDaemonSet(ctx, cs, DefaultHugepagesSize, 0)).To(Succeed())
	})
})
