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

package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestNodeLabelsChangedPredicate(t *testing.T) {
	node := func(region string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", Labels: map[string]string{RegionLabel: region}}}
	}
	if !nodeLabelsChanged.Create(event.CreateEvent{Object: node("a")}) {
		t.Error("a new node must trigger")
	}
	if !nodeLabelsChanged.Delete(event.DeleteEvent{Object: node("a")}) {
		t.Error("a deleted node must trigger")
	}
	if !nodeLabelsChanged.Update(event.UpdateEvent{ObjectOld: node("a"), ObjectNew: node("b")}) {
		t.Error("a label change must trigger")
	}
	heartbeat := node("a")
	heartbeat.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	if nodeLabelsChanged.Update(event.UpdateEvent{ObjectOld: node("a"), ObjectNew: heartbeat}) {
		t.Error("a status-only update (kubelet heartbeat) must not trigger")
	}
}
