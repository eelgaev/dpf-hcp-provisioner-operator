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
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// IsMultiArchReleaseImage queries the registry to check whether the release
// image is a manifest list (multi-arch) or a single-arch image. Uses HEAD
// (no image download) so the check is lightweight.
func IsMultiArchReleaseImage(ctx context.Context, image string, keychain authn.Keychain) bool {
	if strings.HasSuffix(image, "-multi") {
		return true
	}
	ref, err := name.ParseReference(image)
	if err != nil {
		return false
	}
	desc, err := remote.Head(ref, remote.WithAuthFromKeychain(keychain), remote.WithContext(ctx))
	if err != nil {
		return false
	}
	return desc.MediaType.IsIndex()
}

// Aarch64ReleaseImage returns the aarch64-specific release image reference for
// the given release image and OCP version, e.g.
// "quay.io/openshift-release-dev/ocp-release:4.18.24-aarch64". The registry
// portion is taken from releaseImage, stripping any digest or tag.
func Aarch64ReleaseImage(releaseImage, version string) string {
	registry := releaseImage
	if idx := strings.Index(registry, "@"); idx > 0 {
		registry = registry[:idx]
	} else if idx := strings.LastIndex(registry, ":"); idx > 0 {
		registry = registry[:idx]
	}
	return fmt.Sprintf("%s:%s-aarch64", registry, version)
}
