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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/volcano-sh/kthena/pkg/apis/networking/v1alpha1"
)

const testMaxIdleConnsPerHost = 64

func TestNewPooledTransportOverridesPerHostPool(t *testing.T) {
	transport := NewPooledTransport(testMaxIdleConnsPerHost)
	assert.Equal(t, testMaxIdleConnsPerHost, transport.MaxIdleConnsPerHost)
	assert.NotNil(t, transport.DialContext)
}

// DefaultTransport stores 0 in MaxIdleConnsPerHost and applies
// DefaultMaxIdleConnsPerHost(=2) internally, so compare against the constant.
func TestNewPooledTransportWiderThanStdlibDefault(t *testing.T) {
	assert.Greater(t, NewPooledTransport(testMaxIdleConnsPerHost).MaxIdleConnsPerHost,
		http.DefaultMaxIdleConnsPerHost)
}

// Streaming prefill/decode must not be cut off mid-flight.
func TestNewPooledTransportNoResponseHeaderTimeout(t *testing.T) {
	assert.Zero(t, NewPooledTransport(testMaxIdleConnsPerHost).ResponseHeaderTimeout)
}

func TestNewPooledTransportWithConfig(t *testing.T) {
	cfg := PoolConfig{
		MaxIdleConns:        7,
		MaxIdleConnsPerHost: 9,
		MaxConnsPerHost:     3,
		IdleConnTimeout:     5 * time.Second,
	}
	transport := NewPooledTransportWithConfig(cfg)
	assert.Equal(t, 7, transport.MaxIdleConns)
	assert.Equal(t, 9, transport.MaxIdleConnsPerHost)
	assert.Equal(t, 3, transport.MaxConnsPerHost)
	assert.Equal(t, 5*time.Second, transport.IdleConnTimeout)
}

func TestDefaultPoolConfig(t *testing.T) {
	cfg := DefaultPoolConfig()
	assert.Equal(t, 100, cfg.MaxIdleConns)
	assert.Equal(t, 64, cfg.MaxIdleConnsPerHost)
	assert.Zero(t, cfg.MaxConnsPerHost)
	assert.Equal(t, 90*time.Second, cfg.IdleConnTimeout)
}

func TestPoolConfigFromCRD(t *testing.T) {
	maxIdle := int32(8)
	perHost := int32(5)
	maxConns := int32(2)

	tests := []struct {
		name string
		cp   *v1alpha1.ConnectionPool
		want PoolConfig
	}{
		{
			name: "nil connectionPool applies defaults",
			cp:   nil,
			want: DefaultPoolConfig(),
		},
		{
			name: "empty connectionPool applies defaults",
			cp:   &v1alpha1.ConnectionPool{},
			want: DefaultPoolConfig(),
		},
		{
			name: "explicit fields override defaults",
			cp: &v1alpha1.ConnectionPool{
				MaxIdleConnections:        &maxIdle,
				MaxIdleConnectionsPerHost: &perHost,
				MaxConnectionsPerHost:     &maxConns,
				IdleTimeout:               &metav1.Duration{Duration: 13 * time.Second},
			},
			want: PoolConfig{MaxIdleConns: 8, MaxIdleConnsPerHost: 5, MaxConnsPerHost: 2, IdleConnTimeout: 13 * time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, PoolConfigFromCRD(tt.cp))
		})
	}
}
