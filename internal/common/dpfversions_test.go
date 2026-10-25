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
	"testing"
)

func TestDPFVersionValuesForVersion(t *testing.T) {
	tests := []struct {
		name     string
		version  string
		wantDoca string
		wantErr  bool
	}{
		{name: "DPF 26.4 maps to DOCA 3.4", version: "26.4", wantDoca: "3.4"},
		{name: "unknown DPF minor falls back to default row", version: "26.12", wantDoca: "3.4"},
		{name: "empty version errors", version: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values, err := DPFVersionValuesForVersion(context.Background(), tt.version)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got values %+v", values)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if values.DocaVersion != tt.wantDoca {
				t.Errorf("DPF %s: docaVersion = %q, want %q", tt.version, values.DocaVersion, tt.wantDoca)
			}
		})
	}
}

func TestDPFVersionValuesDefaultRow(t *testing.T) {
	want := dpfVersionValuesMap[embeddedDefaultDPFVersionKey]
	values, err := DPFVersionValuesForVersion(context.Background(), "26.12")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *values != want {
		t.Errorf("values = %+v, want default row %+v", *values, want)
	}
}

func TestSupportedDPFVersions(t *testing.T) {
	got := SupportedDPFVersions()
	want := []string{"26.4"}
	if len(got) != len(want) {
		t.Fatalf("SupportedDPFVersions() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SupportedDPFVersions() = %v, want %v", got, want)
		}
	}
}
