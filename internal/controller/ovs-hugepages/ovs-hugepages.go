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
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	provisioningv1alpha1 "github.com/rh-ecosystem-edge/dpf-hcp-provisioner-operator/api/v1alpha1"
	"github.com/rh-ecosystem-edge/dpf-hcp-provisioner-operator/internal/common"
	"github.com/rh-ecosystem-edge/dpf-hcp-provisioner-operator/internal/controller/bfocplookup"
	"github.com/rh-ecosystem-edge/dpf-hcp-provisioner-operator/internal/controller/dpuservicetemplate"
	"github.com/rh-ecosystem-edge/dpf-hcp-provisioner-operator/internal/hostedclient"
)

// ─────────────────────────────────────────────────────────────────────────────
// Tunables — the knobs most likely to change live here so they are easy to spot.
// ─────────────────────────────────────────────────────────────────────────────
const (
	// pausePayloadImage is the name of the pause/sandbox image in an OCP release
	// payload's image-references manifest (the "pod" tag). Resolving it yields the
	// exact "ose-pod" image the hosted cluster's own nodes already run, so the
	// reservation pods reuse an image already present on every node (no extra pull, no
	// mirroring gap).
	pausePayloadImage = "pod" // resolves to the "ose-pod" image

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
	mgmtClient    client.Client
	clientManager *hostedclient.ClientManager
	releaseReader dpuservicetemplate.ReleaseImageReader
	recorder      record.EventRecorder

	// pauseImages caches the pause image resolved from each hosted cluster's release
	// payload (keyed by release image ref). Resolving pulls and extracts the release
	// payload, so we do it once per release rather than on every reconcile; the release
	// image changes only on cluster upgrade. Reconciles run serially (the controller
	// uses the default concurrency of 1), so no locking is needed.
	pauseImages map[string]string
}

// NewManager creates a new hugepages reservation manager. It shares the hosted-cluster
// client manager with the other reconcilers so there is a single client path. mgmtClient
// and releaseReader are used to resolve the reservation pod's pause image from the hosted
// cluster's OCP release payload at runtime.
func NewManager(mgmtClient client.Client, clientManager *hostedclient.ClientManager, releaseReader dpuservicetemplate.ReleaseImageReader, recorder record.EventRecorder) *Manager {
	return &Manager{
		mgmtClient:    mgmtClient,
		clientManager: clientManager,
		releaseReader: releaseReader,
		recorder:      recorder,
		pauseImages:   make(map[string]string),
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

	// When disabled (amount == 0) no image is needed: ensureDaemonSet just removes
	// any existing reservation.
	if amount == 0 {
		return ensureDaemonSet(ctx, hcClient, "", size, amount)
	}

	if err := ensureNamespace(ctx, hcClient); err != nil {
		return err
	}

	// Resolve the pause image from the hosted cluster's own release payload so the
	// reservation pods run the exact image their nodes already have. On failure we
	// return an error so the reconcile requeues rather than deploying a guessed image.
	pauseImage, err := m.resolvePauseImage(ctx, cr)
	if err != nil {
		return err
	}

	return ensureDaemonSet(ctx, hcClient, pauseImage, size, amount)
}

// resolvePauseImage returns the pause/sandbox image for the reservation pods, resolved
// from the hosted cluster's OCP release payload and cached per release image. It returns
// an error on resolution failure (nothing is cached) so the caller can requeue and retry.
func (m *Manager) resolvePauseImage(ctx context.Context, cr *provisioningv1alpha1.DPFHCPProvisioner) (string, error) {
	log := logf.FromContext(ctx)
	releaseImage := cr.Spec.OCPReleaseImage

	if cached, ok := m.pauseImages[releaseImage]; ok {
		return cached, nil
	}

	resolved, err := m.resolvePauseImageFromRelease(ctx, cr, releaseImage)
	if err != nil {
		return "", fmt.Errorf("resolving pause image from release %q: %w", releaseImage, err)
	}

	m.pauseImages[releaseImage] = resolved
	log.V(1).Info("Resolved hugepages reservation pause image from release payload",
		"releaseImage", releaseImage, "pauseImage", resolved)
	return resolved, nil
}

// resolvePauseImageFromRelease extracts the aarch64 pause ("pod") image from the hosted
// cluster's release payload. DPU nodes are aarch64, so it targets the aarch64 release
// variant, mirroring how the DPUServiceTemplate reconciler resolves arch-specific images.
func (m *Manager) resolvePauseImageFromRelease(ctx context.Context, cr *provisioningv1alpha1.DPFHCPProvisioner, releaseImage string) (string, error) {
	keychain, err := common.KeychainFromPullSecret(ctx, m.mgmtClient, cr.Spec.PullSecretRef.Name, cr.Namespace)
	if err != nil {
		return "", fmt.Errorf("getting pull secret keychain: %w", err)
	}

	version, err := bfocplookup.ExtractOCPVersion(ctx, releaseImage, keychain)
	if err != nil {
		return "", fmt.Errorf("extracting OCP version from %q: %w", releaseImage, err)
	}

	// Build the aarch64 release ref from the registry/repo portion and the version.
	registry := releaseImage
	if idx := strings.Index(registry, "@"); idx > 0 {
		registry = registry[:idx]
	} else if idx := strings.LastIndex(registry, ":"); idx > 0 {
		registry = registry[:idx]
	}
	aarch64Ref := fmt.Sprintf("%s:%s-aarch64", registry, version)

	pauseImage, err := m.releaseReader.GetComponentImage(ctx, aarch64Ref, pausePayloadImage, keychain)
	if err != nil {
		return "", fmt.Errorf("resolving %q image from release %q: %w", pausePayloadImage, aarch64Ref, err)
	}
	return pauseImage, nil
}

// ensureNamespace creates the reservation namespace in the hosted cluster if missing.
func ensureNamespace(ctx context.Context, hcClient kubernetes.Interface) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: OVSHugepagesNamespace}}
	_, err := hostedclient.CreateOrUpdate(ctx, hcClient.CoreV1().Namespaces(), OVSHugepagesNamespace, ns,
		func(ns *corev1.Namespace) error {
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
func ensureDaemonSet(ctx context.Context, hcClient kubernetes.Interface, image, size string, amount int32) error {
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

	desired := buildDaemonSet(image, size, amount)
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: DaemonSetName, Namespace: OVSHugepagesNamespace}}
	op, err := hostedclient.CreateOrUpdate(ctx, dsClient, DaemonSetName, ds,
		func(ds *appsv1.DaemonSet) error {
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
// image is the pause image the reservation pods run; size selects the
// "hugepages-<size>" resource and amount is the number of pages; the reserved
// quantity is amount x size.
func buildDaemonSet(image, size string, amount int32) *appsv1.DaemonSet {
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
							Image: image,
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
