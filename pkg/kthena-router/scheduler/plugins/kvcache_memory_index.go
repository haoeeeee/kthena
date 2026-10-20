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

package plugins

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// The in-memory KV index ("memory" index mode) replaces the shared Redis
// block matrix with a per-router-process index kept in sync by streamed KV events:
//
//  1. The runtime sidecar next to each inference engine subscribes to the
//     engine's KV cache events, converts engine block hashes into
//     standardized token-block hashes, and keeps the authoritative
//     per-model block set for its pod
//     (python/kthena/runtime/memory_kv_manager.py).
//  2. Every router opens one long-lived connection to the runtime sidecar of
//     each model-serving pod in its datastore (kvcache_subscription.go). The
//     sidecar answers with a stream that starts with the pod's full block
//     set and continues with its stored/removed/cleared deltas.
//  3. Each router applies these events to its KVBlockMemoryIndex and scores
//     pods by longest prefix match exactly like the Redis backend; a
//     periodic GC drops entries not refreshed within the freshness window.
//
// Events are attributed to the pod the router connected to, never to
// anything in the stream, and the router exposes no endpoint for them.
// Store times are capped at the receive time and every pod's share of the
// index is bounded, so a misbehaving pod cannot pin entries past GC or grow
// router memory without limit.
//
// KV event types streamed by the runtime sidecar in memory sync mode.
// Kept in sync by hand with python/kthena/runtime/memory_kv_manager.py.
const (
	kvEventStored  = "stored"
	kvEventRemoved = "removed"
	kvEventCleared = "cleared"
)

// KVEvent is a single KV cache event carried in a KV events message. All block
// hashes are standardized hashes computed by the runtime sidecar.
type KVEvent struct {
	Type        string   `json:"type"`
	BlockHashes []uint64 `json:"block_hashes,omitempty"`
	Timestamp   int64    `json:"timestamp,omitempty"`
	// Timestamps optionally carries one unix-second store time per entry of
	// BlockHashes. The initial block set of a stream uses it to preserve
	// original store times instead of re-stamping every block when it is
	// sent, which would defeat the engine-restart freshness filter.
	Timestamps []int64 `json:"timestamps,omitempty"`
}

// KVEventPayload is one message, i.e. one line, of the KV events stream of a
// runtime sidecar.
type KVEventPayload struct {
	ModelName string    `json:"model_name"`
	Events    []KVEvent `json:"events"`
}

// An owner is the pod identifier of the inference engine pod that holds a
// block in its KV cache, in the "<podName>.<namespace>" form — the same
// identifier the Redis backend stores as hash field names, so lookup results
// flow through the shared scoring path unchanged.
//
// ownerModelKey indexes per-owner-per-model block sets for removal and clearing.
type ownerModelKey struct {
	owner string
	model string
}

// defaultMaxBlocksPerPod is the default upper bound of blocks a single pod may
// hold in the index across all models. It is far above the KV cache capacity
// of a single engine (1Mi blocks of 16 tokens is ~16.7M cached tokens) and only
// exists to bound router memory.
const defaultMaxBlocksPerPod = 1 << 20

// KVBlockMemoryIndex is an in-memory replacement for the Redis block matrix.
// It stores which pod (owner) currently caches which standardized token block
// hash, per model, together with the time the block was last stored.
type KVBlockMemoryIndex struct {
	mu sync.RWMutex
	// maxBlocksPerOwner caps ownerTotals; an owner at the cap evicts its
	// oldest blocks to store new ones, see evictOldestLocked.
	maxBlocksPerOwner int
	// model -> block hash -> owner(podName.namespace) -> unix seconds of last store
	blocks map[string]map[uint64]map[string]int64
	// reverse index for cleared events, pod removal and eviction:
	// owner+model -> block hash -> unix seconds of last store, same as in blocks
	ownerBlocks map[ownerModelKey]map[uint64]int64
	// owner -> number of blocks held across all models
	ownerTotals map[string]int
}

// NewKVBlockMemoryIndex creates an index that holds at most maxBlocksPerOwner
// blocks per pod; a non-positive value selects defaultMaxBlocksPerPod.
func NewKVBlockMemoryIndex(maxBlocksPerOwner int) *KVBlockMemoryIndex {
	if maxBlocksPerOwner <= 0 {
		maxBlocksPerOwner = defaultMaxBlocksPerPod
	}
	return &KVBlockMemoryIndex{
		maxBlocksPerOwner: maxBlocksPerOwner,
		blocks:            make(map[string]map[uint64]map[string]int64),
		ownerBlocks:       make(map[ownerModelKey]map[uint64]int64),
		ownerTotals:       make(map[string]int),
	}
}

// Apply updates the index with a KV events message received from owner.
func (idx *KVBlockMemoryIndex) Apply(owner string, payload *KVEventPayload) error {
	if owner == "" || payload.ModelName == "" {
		return fmt.Errorf("owner and model_name are required")
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	now := time.Now().Unix()
	evicted := 0
	for _, event := range payload.Events {
		if len(event.Timestamps) > 0 && len(event.Timestamps) != len(event.BlockHashes) {
			return fmt.Errorf("timestamps length %d does not match block_hashes length %d",
				len(event.Timestamps), len(event.BlockHashes))
		}
		switch event.Type {
		case kvEventStored:
			evicted += idx.storeLocked(payload.ModelName, owner, event.BlockHashes, event.Timestamp, event.Timestamps, now)
		case kvEventRemoved:
			idx.removeLocked(payload.ModelName, owner, event.BlockHashes)
		case kvEventCleared:
			idx.clearLocked(payload.ModelName, owner)
		default:
			return fmt.Errorf("unknown event type %q", event.Type)
		}
	}
	if evicted > 0 {
		klog.V(2).Infof("KVCacheAware: pod %s reached the limit of %d indexed blocks, evicted its %d oldest blocks",
			owner, idx.maxBlocksPerOwner, evicted)
	}
	return nil
}

// clampStoreTime returns the store time to record for a streamed block. Store
// times come from the pod: one in the future would never be garbage
// collected and would defeat the restart freshness check, so it is capped at
// the receive time; a missing one defaults to it.
func clampStoreTime(ts, now int64) int64 {
	if ts <= 0 || ts > now {
		return now
	}
	return ts
}

// storeLocked records hashes as owned by owner and returns how many blocks of
// owner were evicted to make room for them.
func (idx *KVBlockMemoryIndex) storeLocked(model, owner string, hashes []uint64, timestamp int64, timestamps []int64, now int64) int {
	if len(hashes) == 0 {
		return 0
	}
	key := ownerModelKey{owner: owner, model: model}
	modelBlocks, owned := idx.ownerModelLocked(key)
	evicted := 0
	for i, hash := range hashes {
		ts := timestamp
		if len(timestamps) > 0 && timestamps[i] > 0 {
			ts = timestamps[i]
		}
		ts = clampStoreTime(ts, now)
		if _, held := owned[hash]; !held {
			if idx.ownerTotals[owner] >= idx.maxBlocksPerOwner {
				evicted += idx.evictOldestLocked(owner)
				// Eviction deletes the maps it empties, possibly these.
				modelBlocks, owned = idx.ownerModelLocked(key)
			}
			idx.ownerTotals[owner]++
		}
		owned[hash] = ts
		owners, ok := modelBlocks[hash]
		if !ok {
			owners = make(map[string]int64)
			modelBlocks[hash] = owners
		}
		owners[owner] = ts
	}
	return evicted
}

// ownerModelLocked returns the blocks of key.model and the blocks owned by
// key.owner in that model, creating the maps if needed.
func (idx *KVBlockMemoryIndex) ownerModelLocked(key ownerModelKey) (map[uint64]map[string]int64, map[uint64]int64) {
	modelBlocks, ok := idx.blocks[key.model]
	if !ok {
		modelBlocks = make(map[uint64]map[string]int64)
		idx.blocks[key.model] = modelBlocks
	}
	owned, ok := idx.ownerBlocks[key]
	if !ok {
		owned = make(map[uint64]int64)
		idx.ownerBlocks[key] = owned
	}
	return modelBlocks, owned
}

// evictOldestLocked makes room for new blocks of owner, which holds
// maxBlocksPerOwner blocks, by evicting its least recently stored blocks
// across all models. Blocks left over from a previous incarnation of the pod
// are older than its current ones, so they go first. A tenth of the limit is
// evicted at once, so the scan over the owner's blocks is not repeated for
// every new block. It returns the number of evicted blocks.
func (idx *KVBlockMemoryIndex) evictOldestLocked(owner string) int {
	n := max(1, idx.maxBlocksPerOwner/10)

	// The n oldest blocks are those stored before cutoff, plus atCutoff of
	// those stored at cutoff.
	counts := make(map[int64]int)
	for key, owned := range idx.ownerBlocks {
		if key.owner != owner {
			continue
		}
		for _, ts := range owned {
			counts[ts]++
		}
	}
	var cutoff int64
	atCutoff := n
	for _, ts := range slices.Sorted(maps.Keys(counts)) {
		cutoff = ts
		if counts[ts] >= atCutoff {
			break
		}
		atCutoff -= counts[ts]
	}

	evicted := 0
	for key, owned := range idx.ownerBlocks {
		if key.owner != owner {
			continue
		}
		modelBlocks := idx.blocks[key.model]
		for hash, ts := range owned {
			if ts > cutoff || (ts == cutoff && atCutoff == 0) {
				continue
			}
			if ts == cutoff {
				atCutoff--
			}
			delete(owned, hash)
			if owners, ok := modelBlocks[hash]; ok {
				delete(owners, owner)
				if len(owners) == 0 {
					delete(modelBlocks, hash)
				}
			}
			evicted++
		}
		if len(owned) == 0 {
			delete(idx.ownerBlocks, key)
		}
		if len(modelBlocks) == 0 {
			delete(idx.blocks, key.model)
		}
	}
	idx.releaseLocked(owner, evicted)
	return evicted
}

// releaseLocked accounts for count blocks no longer held by owner.
func (idx *KVBlockMemoryIndex) releaseLocked(owner string, count int) {
	if remaining := idx.ownerTotals[owner] - count; remaining > 0 {
		idx.ownerTotals[owner] = remaining
	} else {
		delete(idx.ownerTotals, owner)
	}
}

func (idx *KVBlockMemoryIndex) removeLocked(model, owner string, hashes []uint64) {
	modelBlocks := idx.blocks[model]
	key := ownerModelKey{owner: owner, model: model}
	owned := idx.ownerBlocks[key]
	for _, hash := range hashes {
		if _, held := owned[hash]; !held {
			continue
		}
		delete(owned, hash)
		idx.releaseLocked(owner, 1)
		if owners, ok := modelBlocks[hash]; ok {
			delete(owners, owner)
			if len(owners) == 0 {
				delete(modelBlocks, hash)
			}
		}
	}
	if len(owned) == 0 {
		delete(idx.ownerBlocks, key)
	}
	if len(modelBlocks) == 0 {
		delete(idx.blocks, model)
	}
}

func (idx *KVBlockMemoryIndex) clearLocked(model, owner string) {
	key := ownerModelKey{owner: owner, model: model}
	modelBlocks := idx.blocks[model]
	owned := idx.ownerBlocks[key]
	for hash := range owned {
		if owners, ok := modelBlocks[hash]; ok {
			delete(owners, owner)
			if len(owners) == 0 {
				delete(modelBlocks, hash)
			}
		}
	}
	idx.releaseLocked(owner, len(owned))
	delete(idx.ownerBlocks, key)
	if len(modelBlocks) == 0 {
		delete(idx.blocks, model)
	}
}

// GetBlockOwners returns, for each requested block hash that is present, the
// owners and the unix-second timestamps of their last store. The timestamps
// are returned as strings so the result can flow through the same freshness
// filtering (freshOwners) as the Redis backend.
func (idx *KVBlockMemoryIndex) GetBlockOwners(model string, hashes []uint64) map[uint64]map[string]string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make(map[uint64]map[string]string)
	modelBlocks, ok := idx.blocks[model]
	if !ok {
		return result
	}
	for _, hash := range hashes {
		owners, ok := modelBlocks[hash]
		if !ok || len(owners) == 0 {
			continue
		}
		entries := make(map[string]string, len(owners))
		for owner, ts := range owners {
			entries[owner] = strconv.FormatInt(ts, 10)
		}
		result[hash] = entries
	}
	return result
}

// RemoveOwner drops every block owned by the given pod identifier across all
// models, e.g. once the KV events stream of the pod ended.
func (idx *KVBlockMemoryIndex) RemoveOwner(owner string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for key := range idx.ownerBlocks {
		if key.owner == owner {
			idx.clearLocked(key.model, key.owner)
		}
	}
}

// gcStaleEntries drops ownership entries older than freshDuration, mirroring
// the Redis backend GC so restarts or missed removal events cannot leave dead
// entries behind forever.
//
// The full map is traversed while holding the write lock, blocking concurrent
// lookups for the duration of the sweep. This is a deliberate simplicity
// trade-off: the index size is bounded by the aggregate engine KV cache
// capacity (one entry per cached block per pod), so a sweep is pure in-memory
// work finishing in milliseconds even at millions of entries, and it runs
// only once per kvCacheGCInterval (hourly). If the pause ever becomes a
// problem, the sweep can be made incremental, like the cursor-based SCAN GC
// of the Redis backend.
func (idx *KVBlockMemoryIndex) gcStaleEntries(freshDuration time.Duration) {
	cutoff := time.Now().Add(-freshDuration).Unix()

	idx.mu.Lock()
	defer idx.mu.Unlock()

	for model, modelBlocks := range idx.blocks {
		for hash, owners := range modelBlocks {
			for owner, ts := range owners {
				if ts < cutoff {
					delete(owners, owner)
					key := ownerModelKey{owner: owner, model: model}
					owned := idx.ownerBlocks[key]
					delete(owned, hash)
					idx.releaseLocked(owner, 1)
					// Remove emptied parent entries so replaced pods do not
					// leave empty maps behind indefinitely.
					if len(owned) == 0 {
						delete(idx.ownerBlocks, key)
					}
				}
			}
			if len(owners) == 0 {
				delete(modelBlocks, hash)
			}
		}
		if len(modelBlocks) == 0 {
			delete(idx.blocks, model)
		}
	}
}

// runGC periodically removes stale entries until ctx is done.
func (idx *KVBlockMemoryIndex) runGC(ctx context.Context, interval, freshDuration time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			idx.gcStaleEntries(freshDuration)
		}
	}
}
