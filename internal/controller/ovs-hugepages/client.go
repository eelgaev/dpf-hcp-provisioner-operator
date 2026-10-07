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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// newHostedClusterClient builds a fresh typed clientset for the hosted cluster. It caches
// nothing: every call reads the admin kubeconfig and builds a new client. The hugepages
// reconcile is infrequent and effectively one-shot, so there is nothing to gain from caching
// (unlike CSR approval, which polls every 30s and keeps its own cached client).
func newHostedClusterClient(ctx context.Context, mgmtClient client.Client, namespace, name string) (*kubernetes.Clientset, error) {
	kubeconfigData, err := fetchKubeconfig(ctx, mgmtClient, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	config, err := buildRestConfig(kubeconfigData, namespace, name)
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %w", err)
	}

	return clientset, nil
}

// fetchKubeconfig retrieves the kubeconfig bytes from the hosted cluster's admin secret.
func fetchKubeconfig(ctx context.Context, mgmtClient client.Client, namespace, name string) ([]byte, error) {
	// The kubeconfig secret name follows HyperShift convention: <hostedcluster-name>-admin-kubeconfig
	secretName := name + "-admin-kubeconfig"

	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: namespace,
		Name:      secretName,
	}

	if err := mgmtClient.Get(ctx, secretKey, secret); err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig secret %s: %w", secretKey, err)
	}

	kubeconfigData, ok := secret.Data["kubeconfig"]
	if !ok {
		return nil, fmt.Errorf("kubeconfig key not found in secret %s", secretKey)
	}

	if len(kubeconfigData) == 0 {
		return nil, fmt.Errorf("kubeconfig data is empty in secret %s", secretKey)
	}

	return kubeconfigData, nil
}

// buildRestConfig parses the admin kubeconfig, rewrites its endpoint to the internal service DNS name
// and applies the hosted-cluster REST tunables.
func buildRestConfig(kubeconfigData []byte, namespace, name string) (*rest.Config, error) {
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
