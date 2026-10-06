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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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
		cl  client.Client
	)

	BeforeEach(func() {
		ctx = context.Background()
		cl = fake.NewClientBuilder().WithScheme(clientscheme.Scheme).Build()
	})

	It("creates the reservation namespace when missing and is idempotent", func() {
		Expect(ensureNamespace(ctx, cl)).To(Succeed())

		ns := &corev1.Namespace{}
		Expect(cl.Get(ctx, types.NamespacedName{Name: OVSHugepagesNamespace}, ns)).To(Succeed())
		Expect(ns.Name).To(Equal("openshift-doca-hugepages-holder"))

		Expect(ensureNamespace(ctx, cl)).To(Succeed())
	})
})

var _ = Describe("ensureDaemonSet", func() {
	var (
		ctx   context.Context
		cl    client.Client
		dsKey types.NamespacedName
	)

	BeforeEach(func() {
		ctx = context.Background()
		cl = fake.NewClientBuilder().
			WithScheme(clientscheme.Scheme).
			WithObjects(&corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: OVSHugepagesNamespace},
			}).
			Build()
		dsKey = types.NamespacedName{Namespace: OVSHugepagesNamespace, Name: DaemonSetName}
	})

	It("creates the DaemonSet on first call and reports Created", func() {
		op, err := ensureDaemonSet(ctx, cl, DefaultHugepagesSize, DefaultHugepagesAmount)
		Expect(err).NotTo(HaveOccurred())
		Expect(op).To(Equal(controllerutil.OperationResultCreated))

		ds := &appsv1.DaemonSet{}
		Expect(cl.Get(ctx, dsKey, ds)).To(Succeed())
		Expect(ds.Name).To(Equal(DaemonSetName))
	})

	It("reports None on an unchanged subsequent call", func() {
		op, err := ensureDaemonSet(ctx, cl, DefaultHugepagesSize, DefaultHugepagesAmount)
		Expect(err).NotTo(HaveOccurred())
		Expect(op).To(Equal(controllerutil.OperationResultCreated))

		op, err = ensureDaemonSet(ctx, cl, DefaultHugepagesSize, DefaultHugepagesAmount)
		Expect(err).NotTo(HaveOccurred())
		Expect(op).To(Equal(controllerutil.OperationResultNone))

		// Still exactly one DaemonSet.
		list := &appsv1.DaemonSetList{}
		Expect(cl.List(ctx, list, client.InNamespace(OVSHugepagesNamespace))).To(Succeed())
		Expect(list.Items).To(HaveLen(1))
	})

	It("reports Updated when the reservation amount changes", func() {
		_, err := ensureDaemonSet(ctx, cl, DefaultHugepagesSize, DefaultHugepagesAmount)
		Expect(err).NotTo(HaveOccurred())

		op, err := ensureDaemonSet(ctx, cl, DefaultHugepagesSize, DefaultHugepagesAmount*2)
		Expect(err).NotTo(HaveOccurred())
		Expect(op).To(Equal(controllerutil.OperationResultUpdated))
	})

	It("deletes the DaemonSet when amount is zero", func() {
		_, err := ensureDaemonSet(ctx, cl, DefaultHugepagesSize, DefaultHugepagesAmount)
		Expect(err).NotTo(HaveOccurred())

		op, err := ensureDaemonSet(ctx, cl, DefaultHugepagesSize, 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(op).To(Equal(controllerutil.OperationResultNone))

		ds := &appsv1.DaemonSet{}
		err = cl.Get(ctx, dsKey, ds)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("is a no-op when amount is zero and nothing exists", func() {
		op, err := ensureDaemonSet(ctx, cl, DefaultHugepagesSize, 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(op).To(Equal(controllerutil.OperationResultNone))
	})
})
