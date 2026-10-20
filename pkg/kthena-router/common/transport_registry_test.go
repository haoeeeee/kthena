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

package common

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/volcano-sh/kthena/pkg/apis/networking/v1alpha1"
)

func int32Ptr(v int32) *int32 { return &v }

func TestTransportRegistryGetMissReturnsNil(t *testing.T) {
	r := NewTransportRegistry()
	assert.Nil(t, r.Get(types.NamespacedName{Namespace: "ns", Name: "ms"}))
}

func TestTransportRegistryUpdateCreatesAndCaches(t *testing.T) {
	r := NewTransportRegistry()
	name := types.NamespacedName{Namespace: "ns", Name: "ms"}

	r.Update(name, &v1alpha1.ConnectionPool{MaxIdleConnectionsPerHost: int32Ptr(42)})
	got := r.Get(name)
	assert.NotNil(t, got)
	assert.Equal(t, 42, got.MaxIdleConnsPerHost)

	// A second Get returns the same transport pointer.
	assert.Same(t, got, r.Get(name))
}

func TestTransportRegistryUpdateNoOpOnUnchangedConfig(t *testing.T) {
	r := NewTransportRegistry()
	name := types.NamespacedName{Namespace: "ns", Name: "ms"}
	cp := &v1alpha1.ConnectionPool{MaxIdleConnectionsPerHost: int32Ptr(42)}

	r.Update(name, cp)
	first := r.Get(name)
	assert.NotNil(t, first)

	// Re-applying the same config must not rebuild the transport.
	r.Update(name, cp)
	assert.Same(t, first, r.Get(name))
}

func TestTransportRegistryUpdateRebuildsOnChangedConfig(t *testing.T) {
	r := NewTransportRegistry()
	name := types.NamespacedName{Namespace: "ns", Name: "ms"}

	r.Update(name, &v1alpha1.ConnectionPool{MaxIdleConnectionsPerHost: int32Ptr(42)})
	first := r.Get(name)

	r.Update(name, &v1alpha1.ConnectionPool{MaxIdleConnectionsPerHost: int32Ptr(7)})
	second := r.Get(name)

	assert.NotSame(t, first, second)
	assert.Equal(t, 7, second.MaxIdleConnsPerHost)
}

func TestTransportRegistryDeleteRemovesEntry(t *testing.T) {
	r := NewTransportRegistry()
	name := types.NamespacedName{Namespace: "ns", Name: "ms"}

	r.Update(name, &v1alpha1.ConnectionPool{MaxIdleConnectionsPerHost: int32Ptr(42)})
	require.NotNil(t, r.Get(name))

	// Delete must remove the entry and not panic on a transport holding idle
	// connections; CloseIdleConnections is the documented cleanup path.
	r.Delete(name)
	assert.Nil(t, r.Get(name))
}

func TestTransportRegistryDeleteMissingIsNoOp(t *testing.T) {
	r := NewTransportRegistry()
	assert.NotPanics(t, func() {
		r.Delete(types.NamespacedName{Namespace: "ns", Name: "missing"})
	})
}

func TestTransportRegistryUpdateNilConfigCreatesDefaultPool(t *testing.T) {
	r := NewTransportRegistry()
	name := types.NamespacedName{Namespace: "ns", Name: "ms"}

	// A nil connectionPool must still yield an isolated transport using the
	// default config, not fall back to the shared pool (issue #1734 isolation).
	r.Update(name, nil)
	got := r.Get(name)
	require.NotNil(t, got)
	def := DefaultPoolConfig()
	assert.Equal(t, def.MaxIdleConns, got.MaxIdleConns)
	assert.Equal(t, def.MaxIdleConnsPerHost, got.MaxIdleConnsPerHost)
	assert.Equal(t, def.MaxConnsPerHost, got.MaxConnsPerHost)
	assert.Equal(t, def.IdleConnTimeout, got.IdleConnTimeout)
}

func TestTransportRegistryIsolatesModelServers(t *testing.T) {
	r := NewTransportRegistry()
	ms1 := types.NamespacedName{Namespace: "ns", Name: "ms1"}
	ms2 := types.NamespacedName{Namespace: "ns", Name: "ms2"}

	// Two ModelServers without connectionPool each get their own transport, so
	// they do not share the global pool even with identical default settings.
	r.Update(ms1, nil)
	r.Update(ms2, nil)

	t1 := r.Get(ms1)
	t2 := r.Get(ms2)
	require.NotNil(t, t1)
	require.NotNil(t, t2)
	assert.NotSame(t, t1, t2, "each ModelServer must have an isolated transport")
}

func TestTransportRegistryConcurrentGet(t *testing.T) {
	r := NewTransportRegistry()
	name := types.NamespacedName{Namespace: "ns", Name: "ms"}
	r.Update(name, &v1alpha1.ConnectionPool{MaxIdleConnectionsPerHost: int32Ptr(42)})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NotNil(t, r.Get(name))
		}()
	}
	wg.Wait()
}
