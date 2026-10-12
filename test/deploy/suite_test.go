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

// Package deploy installs amphora from the manifests that ship in config/
// (config/samples/podtls: default + the TLS component) onto a throwaway kind
// cluster with a real cert-manager, and checks that it works. The e2e suite
// in test/e2e hand-writes its own manifests and mints its own certificates,
// so it cannot tell whether the install path users actually follow works.
package deploy

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDeploy(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "deploy suite (shipped manifests + real cert-manager)")
}
