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

// Package hostedclient builds and caches clients for hosted (guest) clusters.
package hostedclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClientManager manages hosted cluster client lifecycle
type ClientManager struct {
	mgmtClient client.Client
	// mu protects concurrent access to hcClients map
	// Multiple reconciliations can run concurrently, so we need to protect map access
	mu sync.RWMutex
	// hcClients caches Kubernetes clientsets for hosted clusters to avoid recreating them on every reconciliation.
	// Each DPFHCPProvisioner creates a hosted cluster with its own API server. This map stores one clientset
	// per hosted cluster (keyed by "namespace/name") to reuse connections and avoid parsing the kubeconfig
	// and establishing a new connection on every reconcile.
	//
	// The entry is replaced when the admin kubeconfig secret's resourceVersion changes, so a rotated
	// token or CA is picked up on the next call. A failed connection does not drop the entry: the same
	// kubeconfig would only build another client that fails the same way.
	hcClients map[string]*cachedHostedClient
}

// cachedHostedClient holds the hosted-cluster clients built from a specific admin kubeconfig secret
// revision. Both clients talk to the same API server; the typed clientset serves callers that use the
// client-go API, while the controller-runtime client serves callers that use controllerutil helpers.
type cachedHostedClient struct {
	clientset         *kubernetes.Clientset
	ctrlClient        client.Client
	kubeconfigVersion string
}

// NewClientManager creates a new client manager
func NewClientManager(mgmtClient client.Client) *ClientManager {
	return &ClientManager{
		mgmtClient: mgmtClient,
		hcClients:  make(map[string]*cachedHostedClient),
	}
}

// GetHostedClusterClient retrieves or creates a typed clientset for the hosted cluster.
// The admin kubeconfig secret is read on every call. The cached clientset is reused while that
// secret's resourceVersion is unchanged, and rebuilt when the secret is updated.
func (cm *ClientManager) GetHostedClusterClient(ctx context.Context, namespace, name string) (*kubernetes.Clientset, error) {
	cached, err := cm.getOrBuild(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return cached.clientset, nil
}

// GetHostedClusterCtrlClient retrieves or creates a controller-runtime client for the hosted cluster.
// It shares the same cache and revision semantics as GetHostedClusterClient; use it when a caller
// needs controllerutil helpers (e.g. CreateOrUpdate) rather than the typed client-go API.
func (cm *ClientManager) GetHostedClusterCtrlClient(ctx context.Context, namespace, name string) (client.Client, error) {
	cached, err := cm.getOrBuild(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return cached.ctrlClient, nil
}

// getOrBuild returns the cached hosted-cluster clients, building and caching them when the admin
// kubeconfig secret revision has changed (or nothing is cached yet).
func (cm *ClientManager) getOrBuild(ctx context.Context, namespace, name string) (*cachedHostedClient, error) {
	key := namespace + "/" + name

	kubeconfigData, version, err := cm.getKubeconfig(ctx, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	if cached, ok := cm.cachedClient(key, version); ok {
		return cached, nil
	}

	// Create new clients (outside lock to avoid holding lock during slow operation)
	cached, err := cm.createHostedClusterClient(kubeconfigData, namespace, name)
	if err != nil {
		return nil, err
	}

	return cm.storeClient(key, version, cached), nil
}

// cachedClient returns the cached clients when they were built from the same kubeconfig secret revision.
func (cm *ClientManager) cachedClient(key, version string) (*cachedHostedClient, bool) {
	if version == "" {
		return nil, false
	}

	cm.mu.RLock()
	defer cm.mu.RUnlock()
	cached, ok := cm.hcClients[key]
	if !ok || cached.kubeconfigVersion != version {
		return nil, false
	}
	return cached, true
}

// storeClient caches the clients for the kubeconfig secret revision and returns the entry to use.
// A concurrent caller may have stored the same revision first; that entry is kept.
func (cm *ClientManager) storeClient(key, version string, cached *cachedHostedClient) *cachedHostedClient {
	if version == "" {
		return cached
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()
	if existing, ok := cm.hcClients[key]; ok && existing.kubeconfigVersion == version {
		return existing
	}
	cached.kubeconfigVersion = version
	cm.hcClients[key] = cached
	return cached
}

// InvalidateClient removes a cached client so the next call builds a new one.
func (cm *ClientManager) InvalidateClient(namespace, name string) {
	key := namespace + "/" + name
	cm.mu.Lock()
	delete(cm.hcClients, key)
	cm.mu.Unlock()
}

// createHostedClusterClient builds the typed and controller-runtime clients for the hosted cluster
// from kubeconfig bytes. Both are built from the same REST config so they share endpoint, timeouts
// and credentials.
func (cm *ClientManager) createHostedClusterClient(kubeconfigData []byte, namespace, name string) (*cachedHostedClient, error) {
	config, err := cm.buildRestConfig(kubeconfigData, namespace, name)
	if err != nil {
		return nil, err
	}

	// Create typed clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %w", err)
	}

	// Create controller-runtime client. The client-go scheme registers all built-in API types
	// (core, apps, …), which covers the resources reconciled in hosted clusters today.
	ctrlClient, err := client.New(config, client.Options{Scheme: clientscheme.Scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create controller-runtime client: %w", err)
	}

	return &cachedHostedClient{clientset: clientset, ctrlClient: ctrlClient}, nil
}

// buildRestConfig parses the admin kubeconfig, rewrites its endpoint to the internal service DNS name
// and applies the hosted-cluster REST tunables.
func (cm *ClientManager) buildRestConfig(kubeconfigData []byte, namespace, name string) (*rest.Config, error) {
	// Parse kubeconfig
	kubeconfig, err := clientcmd.Load(kubeconfigData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse kubeconfig: %w", err)
	}

	// Replace external endpoint with internal service DNS name
	// The HyperShift admin-kubeconfig uses external endpoints (LoadBalancer IP or NodePort)
	// which are not accessible from inside the operator pod's network.
	// We need to use the internal service endpoint for in-cluster access.
	if err := replaceServerWithInternalEndpoint(kubeconfig, namespace, name); err != nil {
		return nil, fmt.Errorf("failed to replace server endpoint: %w", err)
	}

	// Create REST config from modified kubeconfig
	config, err := clientcmd.NewDefaultClientConfig(*kubeconfig, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create rest config from kubeconfig: %w", err)
	}

	// Set reasonable timeouts for hosted-cluster API operations.
	// Callers use request/response calls (not watches), so a 30s timeout is appropriate.
	config.Timeout = 30 * time.Second
	config.QPS = 5
	config.Burst = 10

	return config, nil
}

// GetKubeconfigData retrieves the kubeconfig data from the admin secret.
func (cm *ClientManager) GetKubeconfigData(ctx context.Context, namespace, name string) ([]byte, error) {
	data, _, err := cm.getKubeconfig(ctx, namespace, name)
	return data, err
}

// getKubeconfig retrieves the kubeconfig bytes and the secret resourceVersion.
// The resourceVersion identifies the revision the cached hosted-cluster client was built from.
func (cm *ClientManager) getKubeconfig(ctx context.Context, namespace, name string) ([]byte, string, error) {
	// The kubeconfig secret name follows HyperShift convention: <hostedcluster-name>-admin-kubeconfig
	secretName := name + "-admin-kubeconfig"

	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: namespace,
		Name:      secretName,
	}

	if err := cm.mgmtClient.Get(ctx, secretKey, secret); err != nil {
		return nil, "", fmt.Errorf("failed to get kubeconfig secret %s: %w", secretKey, err)
	}

	kubeconfigData, ok := secret.Data["kubeconfig"]
	if !ok {
		return nil, "", fmt.Errorf("kubeconfig key not found in secret %s", secretKey)
	}

	if len(kubeconfigData) == 0 {
		return nil, "", fmt.Errorf("kubeconfig data is empty in secret %s", secretKey)
	}

	return kubeconfigData, secret.ResourceVersion, nil
}

// replaceServerWithInternalEndpoint modifies the kubeconfig to use internal service DNS name
// instead of the external LoadBalancer IP or NodePort. This allows the operator pod (running inside the cluster)
// to reach the hosted cluster API server via the internal Kubernetes service.
//
// HyperShift creates admin-kubeconfig with external endpoints:
// - LoadBalancer: https://10.6.135.42:6443 (example external IP, not accessible from operator pod)
// - NodePort: https://<node-ip>:31039 (example NodePort, dynamically allocated per cluster)
//
// This function replaces it with the internal service DNS name:
// https://kube-apiserver.<namespace>-<name>.svc.cluster.local:6443
//
// Port 6443 is hardcoded to match HyperShift's implementation. HyperShift itself hardcodes
// the kube-apiserver port as a constant (KASSVCPort = 6443) in their codebase.
func replaceServerWithInternalEndpoint(kubeconfig *clientcmdapi.Config, hostedClusterNamespace, hostedClusterName string) error {
	if kubeconfig == nil {
		return fmt.Errorf("kubeconfig is nil")
	}

	// Find the current context
	currentContext := kubeconfig.CurrentContext
	if currentContext == "" {
		return fmt.Errorf("kubeconfig has no current context")
	}

	ctxConfig, ok := kubeconfig.Contexts[currentContext]
	if !ok {
		return fmt.Errorf("context %s not found in kubeconfig", currentContext)
	}

	// Find the cluster referenced by the context
	clusterName := ctxConfig.Cluster
	cluster, ok := kubeconfig.Clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster %s not found in kubeconfig", clusterName)
	}

	// Construct the service namespace following HyperShift convention
	serviceNamespace := fmt.Sprintf("%s-%s", hostedClusterNamespace, hostedClusterName)

	// Construct internal service DNS name with hardcoded port 6443 (matching HyperShift's approach)
	internalServer := fmt.Sprintf("https://kube-apiserver.%s.svc.cluster.local:6443", serviceNamespace)

	// Replace the server URL
	cluster.Server = internalServer

	return nil
}

// TestConnection verifies the hosted cluster client can connect to the API server
func TestConnection(ctx context.Context, clientset *kubernetes.Clientset) error {
	_, err := clientset.Discovery().ServerVersion()
	if err != nil {
		return fmt.Errorf("failed to connect to hosted cluster API server: %w", err)
	}
	return nil
}
