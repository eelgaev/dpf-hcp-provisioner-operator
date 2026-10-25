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

package common

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

//go:embed dpfversions.json
var dpfVersionsJSON []byte

// embeddedDefaultDPFVersionKey is the JSON map key used when the running DPF
// major.minor version has no dedicated entry (e.g. DPF main or a new GA minor).
const embeddedDefaultDPFVersionKey = "default"

// OVNTemplateValues holds the chart settings for the OVN DPUServiceTemplate.
type OVNTemplateValues struct {
	ChartRepoURL string `json:"chartRepoURL"`
	ChartName    string `json:"chartName"`
	ChartVersion string `json:"chartVersion"`
}

// DTSTemplateValues holds the chart and image settings for the DOCA Telemetry Service DPUServiceTemplate.
type DTSTemplateValues struct {
	ChartRepoURL   string `json:"chartRepoURL"`
	ChartName      string `json:"chartName"`
	ChartVersion   string `json:"chartVersion"`
	ImageDTS       string `json:"imageDTS"`
	PrometheusPort int32  `json:"prometheusPort"`
}

// HBNTemplateValues holds the chart and image settings for the HBN DPUServiceTemplate.
type HBNTemplateValues struct {
	ChartRepoURL string `json:"chartRepoURL"`
	ChartName    string `json:"chartName"`
	ChartVersion string `json:"chartVersion"`
	ImageRepo    string `json:"imageRepo"`
	ImageTag     string `json:"imageTag"`
	BFSFCount    int32  `json:"bfSFCount"`
}

// DPUServiceTemplateValues holds all values for creating DPUServiceTemplates in a particular DPF version.
type DPUServiceTemplateValues struct {
	OVN             OVNTemplateValues `json:"ovn"`
	DTS             DTSTemplateValues `json:"dts"`
	HBN             HBNTemplateValues `json:"hbn"`
	ImagePullSecret string            `json:"-"`
}

// DPFVersionValues holds the version-specific settings for a DPF major.minor
// version: the DOCA major.minor version (e.g. "3.4") that BlueField OCP layer
// images are built with, and the DPUServiceTemplate chart/image pins.
type DPFVersionValues struct {
	DocaVersion string `json:"docaVersion"`
	DPUServiceTemplateValues
}

// dpfVersionValuesMap maps DPF major.minor versions (e.g. "26.4") to their
// version-specific values. When a new DPF version is released, add a new entry
// to dpfversions.json and keep "default" pointing at the values to use for
// versions without a dedicated entry.
var dpfVersionValuesMap map[string]DPFVersionValues

// loggedDefaultFallbacks records the DPF versions for which the fallback to the
// "default" row has already been logged. Several controllers resolve the values
// on every reconcile, so the fallback is logged once per version per process.
var loggedDefaultFallbacks sync.Map

func init() {
	if err := json.Unmarshal(dpfVersionsJSON, &dpfVersionValuesMap); err != nil {
		panic(fmt.Sprintf("failed to parse embedded dpfversions.json: %v", err))
	}
}

// DPFVersionValuesForVersion returns the embedded values for the given DPF
// major.minor version (e.g. "26.4"). Versions without a dedicated entry use
// the "default" row, so DPF main and unreleased minors resolve to the latest
// known values instead of failing.
func DPFVersionValuesForVersion(ctx context.Context, version string) (*DPFVersionValues, error) {
	if version == "" {
		return nil, fmt.Errorf("unsupported DPF version %q", version)
	}

	if values, ok := dpfVersionValuesMap[version]; ok {
		return &values, nil
	}

	if values, ok := dpfVersionValuesMap[embeddedDefaultDPFVersionKey]; ok {
		if _, logged := loggedDefaultFallbacks.LoadOrStore(version, struct{}{}); !logged {
			logf.FromContext(ctx).Info("No embedded values for DPF version; using default",
				"requestedVersion", version, "defaultDocaVersion", values.DocaVersion)
		}
		return &values, nil
	}

	return nil, fmt.Errorf("unsupported DPF version %q: no embedded entry and no %q defaults", version, embeddedDefaultDPFVersionKey)
}

// SupportedDPFVersions returns the sorted list of DPF major.minor versions
// with a dedicated entry in the embedded table (excluding "default").
func SupportedDPFVersions() []string {
	supported := make([]string, 0, len(dpfVersionValuesMap))
	for v := range dpfVersionValuesMap {
		if v == embeddedDefaultDPFVersionKey {
			continue
		}
		supported = append(supported, v)
	}
	sort.Strings(supported)
	return supported
}
