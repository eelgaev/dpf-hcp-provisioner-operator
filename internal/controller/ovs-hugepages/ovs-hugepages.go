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

// Package ovshugepages reconciles a DaemonSet inside the hosted (guest) cluster that
// reserves hugepages on every node. Kubelet does not report hugepages utilized by
// the system, so the DaemonSet runs one idle "dummy" pod per node to occupy the
// hugepages needed by OVS and keep them from being consumed by other workloads.
// See https://github.com/kubernetes/enhancements/pull/6253.
package ovshugepages

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	provisioningv1alpha1 "github.com/rh-ecosystem-edge/dpf-hcp-provisioner-operator/api/v1alpha1"
	"github.com/rh-ecosystem-edge/dpf-hcp-provisioner-operator/internal/hostedclient"
)

// ─────────────────────────────────────────────────────────────────────────────
// Tunables — the knobs most likely to change live here so they are easy to spot.
// ─────────────────────────────────────────────────────────────────────────────
const (
	// DummyPodImage is the image used by the reservation pods. We use the OpenShift
	// pause/infra ("pod") image: it does nothing but idle forever, needs no command
	// or shell, is multi-arch (the DPU nodes are aarch64), and is already present on
	// every OpenShift node so there is effectively no extra pull and no disconnected
	// mirroring concern.
	DummyPodImage = "registry.redhat.io/openshift4/ose-pod:latest"

	// DefaultHugepagesSize is the fallback hugepage size when the operator config does
	// not specify one. It selects the "hugepages-<size>" extended resource.
	DefaultHugepagesSize = "2Mi"

	// DefaultHugepagesAmount is the fallback number of hugepages each dummy pod reserves
	// per node when the operator config does not specify one. 250 pages x 2Mi = 500Mi.
	DefaultHugepagesAmount = 250
)

const (
	// OVSHugepagesNamespace holds the reservation DaemonSet in the hosted cluster.
	OVSHugepagesNamespace = "openshift-doca-hugepages-holder"
	// DaemonSetName is the name of the reservation DaemonSet in the hosted cluster.
	DaemonSetName = "ovs-hugepages-reservation"
	// containerName is the name of the reservation container.
	containerName = "allocate"
	// reservationPriorityClassName ranks the reservation pods above ordinary
	// workloads so they reliably schedule (and, if needed, preempt) to hold the
	// OVS-reserved hugepages on every node. It is a built-in OpenShift priority
	// class that always exists on the hosted cluster, so referencing it never
	// blocks pod admission.
	reservationPriorityClassName = "system-node-critical"
)

// Manager reconciles the hugepages reservation DaemonSet inside hosted clusters.
type Manager struct {
	clientManager *hostedclient.ClientManager
	recorder      record.EventRecorder
}

// NewManager creates a new hugepages reservation manager. It shares the hosted-cluster
// client manager with the other reconcilers so there is a single client path.
func NewManager(clientManager *hostedclient.ClientManager, recorder record.EventRecorder) *Manager {
	return &Manager{
		clientManager: clientManager,
		recorder:      recorder,
	}
}

// ReconcileHugepagesDaemonSet ensures the hugepages reservation namespace and
// DaemonSet exist in the hosted cluster for the given DPFHCPProvisioner. It is
// idempotent and safe to call on every reconcile.
//
// size is the hugepage size (selects the "hugepages-<size>" resource, e.g. "2Mi")
// and amount is the number of pages reserved per node (e.g. 250). The reserved
// quantity is amount x size (e.g. 250 x 2Mi = 500Mi). An empty size falls back to
// DefaultHugepagesSize; an amount of zero disables the reservation. An omitted
// amount is defaulted to DefaultHugepagesAmount by the DPFHCPProvisionerConfig CRD.
func (m *Manager) ReconcileHugepagesDaemonSet(ctx context.Context, cr *provisioningv1alpha1.DPFHCPProvisioner, size string, amount int32) error {
	if size == "" {
		size = DefaultHugepagesSize
	}
	if amount < 0 {
		amount = DefaultHugepagesAmount
	}

	hcClient, err := m.clientManager.GetHostedClusterClient(ctx, cr.Namespace, cr.Name)
	if err != nil {
		return fmt.Errorf("failed to get hosted cluster client: %w", err)
	}

	if amount > 0 {
		if err := ensureNamespace(ctx, hcClient); err != nil {
			return err
		}
	}

	if err := ensureDaemonSet(ctx, hcClient, size, amount); err != nil {
		return err
	}

	return nil
}

// ensureNamespace creates the reservation namespace in the hosted cluster if missing.
func ensureNamespace(ctx context.Context, hcClient kubernetes.Interface) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: OVSHugepagesNamespace}}
	nsClient := hcClient.CoreV1().Namespaces()
	_, err := hostedclient.CreateOrUpdate(ctx, ns,
		func(ctx context.Context) error {
			got, err := nsClient.Get(ctx, OVSHugepagesNamespace, metav1.GetOptions{})
			if err != nil {
				return err
			}
			*ns = *got
			return nil
		},
		func(ctx context.Context) error {
			_, err := nsClient.Create(ctx, ns, metav1.CreateOptions{})
			return err
		},
		func(ctx context.Context) error {
			_, err := nsClient.Update(ctx, ns, metav1.UpdateOptions{})
			return err
		},
		func() error {
			if ns.Labels == nil {
				ns.Labels = map[string]string{}
			}
			for k, v := range labels() {
				ns.Labels[k] = v
			}
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed to ensure namespace %s: %w", OVSHugepagesNamespace, err)
	}
	return nil
}

// ensureDaemonSet reconciles the reservation DaemonSet in the hosted cluster and logs what it
// did. An amount of zero removes any existing DaemonSet so its pods stop reserving pages.
func ensureDaemonSet(ctx context.Context, hcClient kubernetes.Interface, size string, amount int32) error {
	log := logf.FromContext(ctx)
	dsClient := hcClient.AppsV1().DaemonSets(OVSHugepagesNamespace)

	if amount == 0 {
		// Remove any existing reservation so its pods stop holding hugepages.
		if err := dsClient.Delete(ctx, DaemonSetName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete DaemonSet %s/%s: %w", OVSHugepagesNamespace, DaemonSetName, err)
		}
		log.V(1).Info("Hugepages reservation disabled in hosted cluster",
			"namespace", OVSHugepagesNamespace, "name", DaemonSetName)
		return nil
	}

	desired := buildDaemonSet(size, amount)
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: DaemonSetName, Namespace: OVSHugepagesNamespace}}
	op, err := hostedclient.CreateOrUpdate(ctx, ds,
		func(ctx context.Context) error {
			got, err := dsClient.Get(ctx, DaemonSetName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			*ds = *got
			return nil
		},
		func(ctx context.Context) error {
			_, err := dsClient.Create(ctx, ds, metav1.CreateOptions{})
			return err
		},
		func(ctx context.Context) error {
			_, err := dsClient.Update(ctx, ds, metav1.UpdateOptions{})
			return err
		},
		func() error {
			ds.Labels = desired.Labels
			// The selector is immutable after creation, so only set it on create.
			if ds.CreationTimestamp.IsZero() {
				ds.Spec.Selector = desired.Spec.Selector
			}
			ds.Spec.Template = desired.Spec.Template
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed to ensure DaemonSet %s/%s: %w", OVSHugepagesNamespace, DaemonSetName, err)
	}

	switch op {
	case hostedclient.OperationResultCreated:
		log.V(1).Info("Created hugepages reservation DaemonSet in hosted cluster",
			"namespace", OVSHugepagesNamespace, "name", DaemonSetName,
			"hugepagesSize", size, "hugepagesCount", amount)
	case hostedclient.OperationResultUpdated:
		log.V(1).Info("Updated hugepages reservation DaemonSet in hosted cluster (drift corrected)",
			"namespace", OVSHugepagesNamespace, "name", DaemonSetName,
			"hugepagesSize", size, "hugepagesCount", amount)
	default:
		log.V(1).Info("Hugepages reservation DaemonSet up to date in hosted cluster",
			"namespace", OVSHugepagesNamespace, "name", DaemonSetName)
	}
	return nil
}

// buildDaemonSet builds the reservation DaemonSet. The pod does nothing but idle;
// its resource requests/limits reserve the hugepages on every node it lands on.
// size selects the "hugepages-<size>" resource and amount is the number of pages;
// the reserved quantity is amount x size.
func buildDaemonSet(size string, amount int32) *appsv1.DaemonSet {
	hugepagesResource := corev1.ResourceName("hugepages-" + size)
	hugepagesQty := hugepagesQuantity(size, amount)

	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DaemonSetName,
			Namespace: OVSHugepagesNamespace,
			Labels:    labels(),
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels()},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels(),
				},
				Spec: corev1.PodSpec{
					// Rank above ordinary workloads so the reservation reliably holds
					// the hugepages that must not be consumed by other pods.
					PriorityClassName: reservationPriorityClassName,
					// Every node of the hosted cluster is a DPU, so there is no
					// nodeSelector — we want one pod on every node. Tolerate all
					// taints so the reservation lands even on tainted/not-ready nodes.
					Tolerations: []corev1.Toleration{
						{Operator: corev1.TolerationOpExists},
					},
					Containers: []corev1.Container{
						{
							Name:  containerName,
							Image: DummyPodImage,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("1m"),
									corev1.ResourceMemory: resource.MustParse("32Mi"),
									hugepagesResource:     hugepagesQty,
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("1m"),
									corev1.ResourceMemory: resource.MustParse("64Mi"),
									hugepagesResource:     hugepagesQty,
								},
							},
						},
					},
				},
			},
		},
	}
}

// hugepagesQuantity returns the total hugepages quantity to reserve: amount pages
// each of the given page size (e.g. size "2Mi", amount 250 -> 500Mi). The result is
// always a multiple of the page size, as Kubernetes requires for hugepage resources.
func hugepagesQuantity(size string, amount int32) resource.Quantity {
	pageSize := resource.MustParse(size)
	return *resource.NewQuantity(pageSize.Value()*int64(amount), resource.BinarySI)
}

// labels returns the common labels applied to the reservation resources.
func labels() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       DaemonSetName,
		"app.kubernetes.io/managed-by": "dpf-hcp-provisioner-operator",
	}
}
