/*
Copyright The Volcano Authors.
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

package test

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	kubetesting "k8s.io/client-go/testing"
)

// ControllerRevisions gives synchronous unit fixtures a lister over their fake
// tracker. Informer integration tests use an actual indexer and watch delivery.
// No API List/Get action is issued by this test lister.
func ControllerRevisions(client kubernetes.Interface) appslisters.ControllerRevisionLister {
	tracked, ok := client.(interface {
		Tracker() kubetesting.ObjectTracker
	})
	if !ok {
		return FailingControllerRevisions(fmt.Errorf("revision fixture requires a fake tracker"))
	}
	return &revisionLister{tracker: tracked.Tracker()}
}

func FailingControllerRevisions(err error) appslisters.ControllerRevisionLister {
	return &revisionLister{err: err}
}

type revisionLister struct {
	tracker kubetesting.ObjectTracker
	err     error
}

func (l *revisionLister) List(selector labels.Selector) ([]*appsv1.ControllerRevision, error) {
	return l.list(metav1.NamespaceAll, selector)
}

func (l *revisionLister) list(namespace string, selector labels.Selector) ([]*appsv1.ControllerRevision, error) {
	if l.err != nil {
		return nil, l.err
	}
	result, err := l.tracker.List(appsv1.SchemeGroupVersion.WithResource("controllerrevisions"), appsv1.SchemeGroupVersion.WithKind("ControllerRevision"), namespace)
	if err != nil {
		return nil, err
	}
	var revisions []*appsv1.ControllerRevision
	for _, revision := range result.(*appsv1.ControllerRevisionList).Items {
		if selector.Matches(labels.Set(revision.Labels)) {
			revisions = append(revisions, revision.DeepCopy())
		}
	}
	return revisions, nil
}

func (l *revisionLister) ControllerRevisions(namespace string) appslisters.ControllerRevisionNamespaceLister {
	return &revisionNamespaceLister{l, namespace}
}

type revisionNamespaceLister struct {
	parent    *revisionLister
	namespace string
}

func (l *revisionNamespaceLister) List(selector labels.Selector) ([]*appsv1.ControllerRevision, error) {
	return l.parent.list(l.namespace, selector)
}
func (l *revisionNamespaceLister) Get(name string) (*appsv1.ControllerRevision, error) {
	if l.parent.err != nil {
		return nil, l.parent.err
	}
	obj, err := l.parent.tracker.Get(appsv1.SchemeGroupVersion.WithResource("controllerrevisions"), l.namespace, name)
	if err != nil {
		return nil, err
	}
	return obj.(*appsv1.ControllerRevision), nil
}
