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
	"net/http"
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	v1alpha1 "github.com/volcano-sh/kthena/pkg/apis/networking/v1alpha1"
)

// PoolConfigFromCRD converts a ConnectionPool CRD field into a PoolConfig,
// filling omitted knobs with the defaults from DefaultPoolConfig.
func PoolConfigFromCRD(cp *v1alpha1.ConnectionPool) PoolConfig {
	cfg := DefaultPoolConfig()
	if cp == nil {
		return cfg
	}
	if cp.MaxIdleConnections != nil {
		cfg.MaxIdleConns = int(*cp.MaxIdleConnections)
	}
	if cp.MaxIdleConnectionsPerHost != nil {
		cfg.MaxIdleConnsPerHost = int(*cp.MaxIdleConnectionsPerHost)
	}
	if cp.MaxConnectionsPerHost != nil {
		cfg.MaxConnsPerHost = int(*cp.MaxConnectionsPerHost)
	}
	if cp.IdleTimeout != nil {
		cfg.IdleConnTimeout = cp.IdleTimeout.Duration
	}
	return cfg
}

type registryEntry struct {
	transport *http.Transport
	cfg       PoolConfig
}

// TransportRegistry holds per-ModelServer upstream transports so each
// ModelServer can tune its own connection pool. Writes are driven by the
// ModelServer controller's single worker (serialized); reads from the request
// hot path are concurrent and served by a sync.Map.
type TransportRegistry struct {
	transports sync.Map // map[types.NamespacedName]*registryEntry
}

// NewTransportRegistry creates an empty registry.
func NewTransportRegistry() *TransportRegistry {
	return &TransportRegistry{}
}

// Get returns the transport for a ModelServer, or nil if none is registered
// (callers fall back to a shared default transport).
func (r *TransportRegistry) Get(name types.NamespacedName) *http.Transport {
	v, ok := r.transports.Load(name)
	if !ok {
		return nil
	}
	return v.(*registryEntry).transport
}

// Update ensures a per-ModelServer transport for name. A nil cp uses default
// settings, so every ModelServer keeps an isolated pool (no global sharing).
// Unchanged config is a no-op. On rebuild the retired transport's idle
// connections are closed immediately; active streams are not interrupted.
func (r *TransportRegistry) Update(name types.NamespacedName, cp *v1alpha1.ConnectionPool) {
	cfg := PoolConfigFromCRD(cp)
	if v, ok := r.transports.Load(name); ok {
		entry := v.(*registryEntry)
		if entry.cfg == cfg {
			return
		}
		if entry.transport != nil {
			entry.transport.CloseIdleConnections()
		}
	}

	transport := NewPooledTransportWithConfig(cfg)
	r.transports.Store(name, &registryEntry{transport: transport, cfg: cfg})
	klog.Infof("Connection pool for ModelServer %s updated: maxIdle=%d maxIdlePerHost=%d maxConnsPerHost=%d idleTimeout=%s",
		name, cfg.MaxIdleConns, cfg.MaxIdleConnsPerHost, cfg.MaxConnsPerHost, cfg.IdleConnTimeout)
}

// Delete removes the entry for name and closes its idle connections. Active
// streams are not forcibly interrupted.
func (r *TransportRegistry) Delete(name types.NamespacedName) {
	v, ok := r.transports.LoadAndDelete(name)
	if !ok {
		return
	}
	if t := v.(*registryEntry).transport; t != nil {
		t.CloseIdleConnections()
	}
	klog.Infof("Connection pool for ModelServer %s removed", name)
}
