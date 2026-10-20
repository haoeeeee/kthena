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
	"net"
	"net/http"
	"time"
)

// Fallback values mirroring http.DefaultTransport, used only when Clone() is
// unavailable (DefaultTransport is not *http.Transport).
const (
	pooledMaxIdleConns        = 100
	pooledIdleConnTimeout     = 90 * time.Second
	pooledDialTimeout         = 30 * time.Second
	pooledDialKeepAlive       = 30 * time.Second
	pooledTLSHandshakeTimeout = 10 * time.Second
)

// Default per-host idle pool widened over the stdlib default of 2; see
// NewPooledTransport.
const defaultMaxIdleConnsPerHost = 64

// PoolConfig holds the tunable knobs of a pooled upstream transport.
type PoolConfig struct {
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	MaxConnsPerHost     int // 0 means unlimited
	IdleConnTimeout     time.Duration
}

// DefaultPoolConfig returns the pool defaults carried by http.DefaultTransport
// plus the widened per-host idle pool (64).
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxIdleConns:        pooledMaxIdleConns,
		MaxIdleConnsPerHost: defaultMaxIdleConnsPerHost,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     pooledIdleConnTimeout,
	}
}

// NewPooledTransport clones http.DefaultTransport and widens only the per-host
// idle pool. The stdlib default (DefaultMaxIdleConnsPerHost=2) churns
// connections under high concurrency against a single upstream pod, surfacing
// as EOF/500 on streaming responses that cannot be retried once begun. Other
// fields are left at the stdlib defaults Clone() already carries.
func NewPooledTransport(maxIdleConnsPerHost int) *http.Transport {
	cfg := DefaultPoolConfig()
	cfg.MaxIdleConnsPerHost = maxIdleConnsPerHost
	return NewPooledTransportWithConfig(cfg)
}

// NewPooledTransportWithConfig builds a pooled transport from an explicit
// PoolConfig. It clones http.DefaultTransport and overrides the four pool
// knobs; 0 for MaxConnsPerHost means unlimited, matching the stdlib semantics.
func NewPooledTransportWithConfig(cfg PoolConfig) *http.Transport {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   pooledDialTimeout,
				KeepAlive: pooledDialKeepAlive,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          cfg.MaxIdleConns,
			MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
			MaxConnsPerHost:       cfg.MaxConnsPerHost,
			IdleConnTimeout:       cfg.IdleConnTimeout,
			TLSHandshakeTimeout:   pooledTLSHandshakeTimeout,
			ExpectContinueTimeout: time.Second,
		}
	}

	t := base.Clone()
	t.MaxIdleConns = cfg.MaxIdleConns
	t.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
	t.MaxConnsPerHost = cfg.MaxConnsPerHost
	t.IdleConnTimeout = cfg.IdleConnTimeout
	return t
}
