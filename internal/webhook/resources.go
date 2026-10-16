/*
Copyright 2026.

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

package webhook

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// isExtendedResource reports whether name is an extended resource such as
// nvidia.com/gpu or nvidia.com/mig-1g.10gb: namespaced, and not under the
// kubernetes.io domain that Kubernetes reserves for native resources.
func isExtendedResource(name corev1.ResourceName) bool {
	n := string(name)
	return strings.Contains(n, "/") && !strings.HasPrefix(n, "kubernetes.io/") && !strings.HasPrefix(n, "requests.")
}

// hasExtendedResource reports whether rr asks for any extended resource, which
// is what makes Kubernetes and the device plugin allocate a GPU or MIG slice.
func hasExtendedResource(rr corev1.ResourceRequirements) bool {
	for name := range rr.Limits {
		if isExtendedResource(name) {
			return true
		}
	}
	for name := range rr.Requests {
		if isExtendedResource(name) {
			return true
		}
	}
	return false
}

// validateResources rejects resource requirements Kubernetes would later refuse
// to create a pod with. That would otherwise surface as a reconcile error on a
// pod the user never wrote, rather than at admission against the spec they did.
// The rules are the ones the API server applies to a container: no negative
// quantities, requests not above limits, and for extended resources a whole
// number that is set in limits and, if also in requests, equal to it.
func validateResources(path string, rr corev1.ResourceRequirements) []error {
	names := map[corev1.ResourceName]struct{}{}
	for n := range rr.Requests {
		names[n] = struct{}{}
	}
	for n := range rr.Limits {
		names[n] = struct{}{}
	}
	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, string(n))
	}
	sort.Strings(sorted)

	var errs []error
	for _, s := range sorted {
		name := corev1.ResourceName(s)
		if problems := validation.IsQualifiedName(s); len(problems) > 0 {
			errs = append(errs, fmt.Errorf("%s: %q is not a valid resource name: %s", path, s, strings.Join(problems, "; ")))
			continue
		}
		req, hasReq := rr.Requests[name]
		lim, hasLim := rr.Limits[name]
		if (hasReq && req.Sign() < 0) || (hasLim && lim.Sign() < 0) {
			errs = append(errs, fmt.Errorf("%s: %s must not be negative", path, s))
			continue
		}
		if hasReq && hasLim && req.Cmp(lim) > 0 {
			errs = append(errs, fmt.Errorf("%s: %s request %s exceeds its limit %s", path, s, req.String(), lim.String()))
		}
		if !isExtendedResource(name) {
			continue
		}
		if hasReq && !hasLim {
			errs = append(errs, fmt.Errorf("%s: extended resource %s must be set in limits (it cannot be overcommitted)", path, s))
		}
		if hasReq && hasLim && req.Cmp(lim) != 0 {
			errs = append(errs, fmt.Errorf("%s: extended resource %s request and limit must be equal", path, s))
		}
		for _, q := range []struct {
			set bool
			val int64
		}{{hasReq, req.MilliValue()}, {hasLim, lim.MilliValue()}} {
			if q.set && q.val%1000 != 0 {
				errs = append(errs, fmt.Errorf("%s: extended resource %s must be a whole number", path, s))
				break
			}
		}
	}
	return errs
}

func derefResources(rr *corev1.ResourceRequirements) corev1.ResourceRequirements {
	if rr == nil {
		return corev1.ResourceRequirements{}
	}
	return *rr
}
