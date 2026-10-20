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
	"maps"
	"slices"
	"strconv"
	"testing"
	"time"
)

// podKVEvents is a KV events message together with the pod it was streamed from.
type podKVEvents struct {
	owner   string
	payload KVEventPayload
}

func TestKVBlockMemoryIndex_Apply(t *testing.T) {
	now := time.Now().Unix()

	tests := []struct {
		name        string
		payloads    []podKVEvents
		wantErr     bool
		queryModel  string
		queryHashes []uint64
		wantOwners  map[uint64][]string
	}{
		{
			name: "stored blocks are queryable",
			payloads: []podKVEvents{{owner: "pod-1.default", payload: KVEventPayload{
				ModelName: "qwen",
				Events: []KVEvent{
					{Type: kvEventStored, BlockHashes: []uint64{1, 2}, Timestamp: now},
				},
			}}},
			queryModel:  "qwen",
			queryHashes: []uint64{1, 2, 3},
			wantOwners: map[uint64][]string{
				1: {"pod-1.default"},
				2: {"pod-1.default"},
			},
		},
		{
			name: "removed blocks disappear",
			payloads: []podKVEvents{
				{owner: "pod-1.default", payload: KVEventPayload{
					ModelName: "qwen",
					Events: []KVEvent{
						{Type: kvEventStored, BlockHashes: []uint64{1, 2}, Timestamp: now},
						{Type: kvEventRemoved, BlockHashes: []uint64{1}},
					},
				}},
			},
			queryModel:  "qwen",
			queryHashes: []uint64{1, 2},
			wantOwners: map[uint64][]string{
				2: {"pod-1.default"},
			},
		},
		{
			name: "cleared drops only the clearing owner",
			payloads: []podKVEvents{
				{owner: "pod-1.default", payload: KVEventPayload{
					ModelName: "qwen",
					Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}, Timestamp: now}},
				}},
				{owner: "pod-2.default", payload: KVEventPayload{
					ModelName: "qwen",
					Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}, Timestamp: now}},
				}},
				{owner: "pod-1.default", payload: KVEventPayload{
					ModelName: "qwen",
					Events:    []KVEvent{{Type: kvEventCleared}},
				}},
			},
			queryModel:  "qwen",
			queryHashes: []uint64{1},
			wantOwners: map[uint64][]string{
				1: {"pod-2.default"},
			},
		},
		{
			name: "mismatched timestamps length is rejected",
			payloads: []podKVEvents{{owner: "pod-1.default", payload: KVEventPayload{
				ModelName: "qwen",
				Events: []KVEvent{{
					Type:        kvEventStored,
					BlockHashes: []uint64{1, 2},
					Timestamps:  []int64{now},
				}},
			}}},
			wantErr:     true,
			queryModel:  "qwen",
			queryHashes: []uint64{1, 2},
			wantOwners:  map[uint64][]string{},
		},
		{
			name: "models are isolated",
			payloads: []podKVEvents{{owner: "pod-1.default", payload: KVEventPayload{
				ModelName: "qwen",
				Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}, Timestamp: now}},
			}}},
			queryModel:  "other-model",
			queryHashes: []uint64{1},
			wantOwners:  map[uint64][]string{},
		},
		{
			name: "missing owner is rejected",
			payloads: []podKVEvents{{payload: KVEventPayload{
				ModelName: "qwen",
				Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}}},
			}}},
			wantErr:     true,
			queryModel:  "qwen",
			queryHashes: []uint64{1},
			wantOwners:  map[uint64][]string{},
		},
		{
			name: "unknown event type is rejected",
			payloads: []podKVEvents{{owner: "pod-1.default", payload: KVEventPayload{
				ModelName: "qwen",
				Events:    []KVEvent{{Type: "bogus", BlockHashes: []uint64{1}}},
			}}},
			wantErr:     true,
			queryModel:  "qwen",
			queryHashes: []uint64{1},
			wantOwners:  map[uint64][]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := NewKVBlockMemoryIndex(0)
			var gotErr error
			for i := range tt.payloads {
				if err := idx.Apply(tt.payloads[i].owner, &tt.payloads[i].payload); err != nil {
					gotErr = err
				}
			}
			if (gotErr != nil) != tt.wantErr {
				t.Fatalf("Apply() error = %v, wantErr = %v", gotErr, tt.wantErr)
			}

			got := idx.GetBlockOwners(tt.queryModel, tt.queryHashes)
			if len(got) != len(tt.wantOwners) {
				t.Fatalf("GetBlockOwners() = %v, want owners for %v", got, tt.wantOwners)
			}
			for hash, wantPods := range tt.wantOwners {
				entries, ok := got[hash]
				if !ok {
					t.Fatalf("expected hash %d in result, got %v", hash, got)
				}
				if len(entries) != len(wantPods) {
					t.Fatalf("hash %d: got owners %v, want %v", hash, entries, wantPods)
				}
				for _, pod := range wantPods {
					if _, ok := entries[pod]; !ok {
						t.Errorf("hash %d: expected owner %s, got %v", hash, pod, entries)
					}
				}
			}
		})
	}
}

func TestKVBlockMemoryIndex_PreservesPerBlockTimestamps(t *testing.T) {
	idx := NewKVBlockMemoryIndex(0)
	tsOld := time.Now().Add(-2 * time.Hour).Unix()
	tsNew := time.Now().Unix()

	payload := KVEventPayload{
		ModelName: "qwen",
		Events: []KVEvent{{
			Type:        kvEventStored,
			BlockHashes: []uint64{1, 2},
			Timestamps:  []int64{tsOld, tsNew},
		}},
	}
	if err := idx.Apply("pod-1.default", &payload); err != nil {
		t.Fatalf("Apply() failed: %v", err)
	}

	got := idx.GetBlockOwners("qwen", []uint64{1, 2})
	if want := strconv.FormatInt(tsOld, 10); got[1]["pod-1.default"] != want {
		t.Errorf("hash 1 timestamp = %s, want %s", got[1]["pod-1.default"], want)
	}
	if want := strconv.FormatInt(tsNew, 10); got[2]["pod-1.default"] != want {
		t.Errorf("hash 2 timestamp = %s, want %s", got[2]["pod-1.default"], want)
	}
}

func TestKVBlockMemoryIndex_RemoveOwner(t *testing.T) {
	idx := NewKVBlockMemoryIndex(0)
	now := time.Now().Unix()
	for _, payload := range []podKVEvents{
		{owner: "pod-1.default", payload: KVEventPayload{
			ModelName: "qwen",
			Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1, 2}, Timestamp: now}},
		}},
		{owner: "pod-1.default", payload: KVEventPayload{
			ModelName: "llama",
			Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{9}, Timestamp: now}},
		}},
		{owner: "pod-2.default", payload: KVEventPayload{
			ModelName: "qwen",
			Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}, Timestamp: now}},
		}},
	} {
		p := payload
		if err := idx.Apply(p.owner, &p.payload); err != nil {
			t.Fatalf("Apply() failed: %v", err)
		}
	}

	idx.RemoveOwner("pod-1.default")

	qwen := idx.GetBlockOwners("qwen", []uint64{1, 2})
	if len(qwen) != 1 {
		t.Fatalf("expected only hash 1 to remain for qwen, got %v", qwen)
	}
	if _, ok := qwen[1]["pod-2.default"]; !ok {
		t.Errorf("expected pod-2.default to remain owner of hash 1, got %v", qwen)
	}
	if llama := idx.GetBlockOwners("llama", []uint64{9}); len(llama) != 0 {
		t.Errorf("expected llama blocks of pod-1 to be gone, got %v", llama)
	}
}

func TestKVBlockMemoryIndex_GCStaleEntries(t *testing.T) {
	idx := NewKVBlockMemoryIndex(0)
	fresh := time.Now().Unix()
	stale := time.Now().Add(-25 * time.Hour).Unix()

	for _, payload := range []podKVEvents{
		{owner: "pod-fresh.default", payload: KVEventPayload{
			ModelName: "qwen",
			Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1}, Timestamp: fresh}},
		}},
		{owner: "pod-stale.default", payload: KVEventPayload{
			ModelName: "qwen",
			Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1, 2}, Timestamp: stale}},
		}},
	} {
		p := payload
		if err := idx.Apply(p.owner, &p.payload); err != nil {
			t.Fatalf("Apply() failed: %v", err)
		}
	}

	idx.gcStaleEntries(kvCacheFieldFreshDuration)

	got := idx.GetBlockOwners("qwen", []uint64{1, 2})
	if len(got) != 1 {
		t.Fatalf("expected only hash 1 to survive GC, got %v", got)
	}
	if _, ok := got[1]["pod-fresh.default"]; !ok {
		t.Errorf("expected fresh owner to survive GC, got %v", got)
	}
	if _, ok := got[1]["pod-stale.default"]; ok {
		t.Errorf("expected stale owner to be removed, got %v", got)
	}
}

// TestKVBlockMemoryIndex_GCRemovesEmptyParentEntries guards against leaked
// empty owner/model maps: with repeated pod replacement every historical pod
// would otherwise leave an empty reverse-index entry behind forever.
func TestKVBlockMemoryIndex_GCRemovesEmptyParentEntries(t *testing.T) {
	idx := NewKVBlockMemoryIndex(0)
	stale := time.Now().Add(-25 * time.Hour).Unix()

	for _, payload := range []podKVEvents{
		{owner: "pod-1.default", payload: KVEventPayload{
			ModelName: "qwen",
			Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{1, 2}, Timestamp: stale}},
		}},
		{owner: "pod-2.default", payload: KVEventPayload{
			ModelName: "qwen",
			Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{2, 3}, Timestamp: stale}},
		}},
		{owner: "pod-3.default", payload: KVEventPayload{
			ModelName: "llama",
			Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{9}, Timestamp: stale}},
		}},
	} {
		p := payload
		if err := idx.Apply(p.owner, &p.payload); err != nil {
			t.Fatalf("Apply() failed: %v", err)
		}
	}

	idx.gcStaleEntries(kvCacheFieldFreshDuration)

	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if len(idx.blocks) != 0 {
		t.Errorf("expected forward index to be empty after GC expired everything, got %v", idx.blocks)
	}
	if len(idx.ownerBlocks) != 0 {
		t.Errorf("expected reverse index to be empty after GC expired everything, got %v", idx.ownerBlocks)
	}
	if len(idx.ownerTotals) != 0 {
		t.Errorf("expected per-owner totals to be empty after GC expired everything, got %v", idx.ownerTotals)
	}
}

func TestKVCacheAware_QueryMemoryForBlocks(t *testing.T) {
	idx := NewKVBlockMemoryIndex(0)
	now := time.Now().Unix()
	payload := KVEventPayload{
		ModelName: "qwen",
		Events:    []KVEvent{{Type: kvEventStored, BlockHashes: []uint64{111}, Timestamp: now}},
	}
	if err := idx.Apply("pod-1.default", &payload); err != nil {
		t.Fatalf("Apply() failed: %v", err)
	}

	plugin := &KVCacheAware{
		indexMode:   kvCacheIndexModeMemory,
		memoryIndex: idx,
	}

	result, err := plugin.queryBlocks([]uint64{111, 222}, "qwen", nil)
	if err != nil {
		t.Fatalf("queryBlocks returned error: %v", err)
	}
	if len(result) != 1 || len(result[111]) != 1 || result[111][0] != "pod-1.default" {
		t.Fatalf("unexpected result: %v", result)
	}

	// Ownership written before the owning pod's containers started is dropped.
	result, err = plugin.queryBlocks([]uint64{111}, "qwen",
		map[string]int64{"pod-1.default": now + 10})
	if err != nil {
		t.Fatalf("queryBlocks returned error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected stale ownership to be dropped, got %v", result)
	}
}

func TestKVBlockMemoryIndex_ClampsStoreTime(t *testing.T) {
	idx := NewKVBlockMemoryIndex(0)
	before := time.Now().Unix()
	past := time.Now().Add(-time.Hour).Unix()
	future := time.Now().Add(100 * 365 * 24 * time.Hour).Unix()

	payload := KVEventPayload{
		ModelName: "qwen",
		Events: []KVEvent{
			{Type: kvEventStored, BlockHashes: []uint64{1}, Timestamp: future},
			{Type: kvEventStored, BlockHashes: []uint64{2, 3, 4}, Timestamps: []int64{past, future, 0}},
			{Type: kvEventStored, BlockHashes: []uint64{5}},
		},
	}
	if err := idx.Apply("pod-1.default", &payload); err != nil {
		t.Fatalf("Apply() failed: %v", err)
	}
	after := time.Now().Unix()

	got := idx.GetBlockOwners("qwen", []uint64{1, 2, 3, 4, 5})
	if want := strconv.FormatInt(past, 10); got[2]["pod-1.default"] != want {
		t.Errorf("hash 2: timestamp = %q, want past timestamp %q to be preserved", got[2]["pod-1.default"], want)
	}
	for _, hash := range []uint64{1, 3, 4, 5} {
		ts, err := strconv.ParseInt(got[hash]["pod-1.default"], 10, 64)
		if err != nil {
			t.Fatalf("hash %d: invalid timestamp %q", hash, got[hash]["pod-1.default"])
		}
		if ts < before || ts > after {
			t.Errorf("hash %d: timestamp = %d, want receive time in [%d, %d]", hash, ts, before, after)
		}
	}

	// A clamped future timestamp must still expire.
	idx.mu.Lock()
	for _, owners := range idx.blocks["qwen"] {
		owners["pod-1.default"] = time.Now().Add(-25 * time.Hour).Unix()
	}
	idx.mu.Unlock()
	idx.gcStaleEntries(kvCacheFieldFreshDuration)
	if got := idx.GetBlockOwners("qwen", []uint64{1, 2, 3, 4, 5}); len(got) != 0 {
		t.Errorf("expected all entries to be garbage collected, got %v", got)
	}
}

func TestKVBlockMemoryIndex_MaxBlocksPerOwner(t *testing.T) {
	idx := NewKVBlockMemoryIndex(3)
	now := time.Now().Unix()
	apply := func(owner, model string, events ...KVEvent) {
		t.Helper()
		p := KVEventPayload{ModelName: model, Events: events}
		if err := idx.Apply(owner, &p); err != nil {
			t.Fatalf("Apply() failed: %v", err)
		}
	}
	total := func(owner string) int {
		idx.mu.RLock()
		defer idx.mu.RUnlock()
		return idx.ownerTotals[owner]
	}
	storedAt := func(age int64, hashes ...uint64) KVEvent {
		return KVEvent{Type: kvEventStored, BlockHashes: hashes, Timestamp: now - age}
	}

	// The cap spans all models of an owner.
	apply("pod-1.default", "qwen", storedAt(30, 1, 2))
	apply("pod-1.default", "llama", storedAt(20, 3))
	if got := total("pod-1.default"); got != 3 {
		t.Fatalf("ownerTotals = %d, want 3", got)
	}

	// Re-storing held blocks does not consume capacity.
	apply("pod-1.default", "qwen", storedAt(10, 1, 2))
	if got := total("pod-1.default"); got != 3 {
		t.Fatalf("ownerTotals after re-store = %d, want 3", got)
	}
	if got := idx.GetBlockOwners("llama", []uint64{3}); got[3]["pod-1.default"] == "" {
		t.Fatalf("expected re-storing held blocks to evict nothing, got %v", got)
	}

	// Other owners have their own budget.
	apply("pod-2.default", "qwen", storedAt(0, 1, 2, 3))
	if got := idx.GetBlockOwners("qwen", []uint64{3}); got[3]["pod-2.default"] == "" {
		t.Fatalf("expected pod-2 to store hash 3, got %v", got)
	}
	if got := total("pod-1.default"); got != 3 {
		t.Fatalf("ownerTotals of pod-1 after pod-2 stored = %d, want 3", got)
	}

	// Removing a block that is not held does not release capacity.
	apply("pod-1.default", "qwen", KVEvent{Type: kvEventRemoved, BlockHashes: []uint64{99}})
	if got := total("pod-1.default"); got != 3 {
		t.Fatalf("ownerTotals after removing unheld block = %d, want 3", got)
	}

	// Removal frees capacity: the next block is stored without eviction.
	apply("pod-1.default", "qwen", KVEvent{Type: kvEventRemoved, BlockHashes: []uint64{1}})
	apply("pod-1.default", "llama", storedAt(0, 4))
	if got := idx.GetBlockOwners("llama", []uint64{3, 4}); got[3]["pod-1.default"] == "" || got[4]["pod-1.default"] == "" {
		t.Fatalf("expected hashes 3 and 4 to be held after freeing capacity, got %v", got)
	}

	apply("pod-1.default", "llama", KVEvent{Type: kvEventCleared})
	if got := total("pod-1.default"); got != 1 {
		t.Fatalf("ownerTotals after clear = %d, want 1", got)
	}

	idx.RemoveOwner("pod-1.default")
	idx.mu.RLock()
	_, ok := idx.ownerTotals["pod-1.default"]
	idx.mu.RUnlock()
	if ok {
		t.Fatalf("expected ownerTotals entry to be removed with the owner")
	}
}

// indexedBlocks returns the sorted blocks held per owner and model, after
// checking that the forward index, the reverse index and the per-owner totals
// agree and hold no empty maps.
func indexedBlocks(t *testing.T, idx *KVBlockMemoryIndex) map[ownerModelKey][]uint64 {
	t.Helper()
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	held := make(map[ownerModelKey][]uint64)
	totals := make(map[string]int)
	for key, owned := range idx.ownerBlocks {
		if len(owned) == 0 {
			t.Errorf("empty reverse index entry for %v", key)
		}
		for hash, ts := range owned {
			if fts, ok := idx.blocks[key.model][hash][key.owner]; !ok || fts != ts {
				t.Errorf("block %d of %v has store time %d in the reverse index, but %d (present: %v) in the forward index",
					hash, key, ts, fts, ok)
			}
			held[key] = append(held[key], hash)
			totals[key.owner]++
		}
		slices.Sort(held[key])
	}
	for model, modelBlocks := range idx.blocks {
		if len(modelBlocks) == 0 {
			t.Errorf("empty forward index entry for model %s", model)
		}
		for hash, owners := range modelBlocks {
			if len(owners) == 0 {
				t.Errorf("empty owner set for block %d of model %s", hash, model)
			}
			for owner := range owners {
				if _, ok := idx.ownerBlocks[ownerModelKey{owner: owner, model: model}][hash]; !ok {
					t.Errorf("block %d of %s/%s is missing from the reverse index", hash, owner, model)
				}
			}
		}
	}
	if !maps.Equal(totals, idx.ownerTotals) {
		t.Errorf("ownerTotals = %v, want %v", idx.ownerTotals, totals)
	}
	return held
}

func TestKVBlockMemoryIndex_EvictsOldestBlocksAtLimit(t *testing.T) {
	type message struct {
		owner     string
		model     string
		eventType string
		hashes    []uint64
		// ages are the store times of hashes, in seconds before now.
		ages []int64
	}
	pod1Qwen := ownerModelKey{owner: "pod-1.default", model: "qwen"}
	pod1Llama := ownerModelKey{owner: "pod-1.default", model: "llama"}
	pod2Qwen := ownerModelKey{owner: "pod-2.default", model: "qwen"}
	seq := func(from, to uint64) []uint64 {
		var s []uint64
		for i := from; i <= to; i++ {
			s = append(s, i)
		}
		return s
	}
	repeat := func(age int64, n int) []int64 {
		return slices.Repeat([]int64{age}, n)
	}
	// Store times of seq(1, 20), from the oldest to the newest.
	descendingAges := []int64{20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}

	tests := []struct {
		name     string
		max      int
		messages []message
		// want is the expected blocks per owner and model, if deterministic.
		want       map[ownerModelKey][]uint64
		wantTotals map[string]int
	}{
		{
			name: "oldest blocks are evicted first, whatever their model",
			max:  3,
			messages: []message{
				{"pod-1.default", "qwen", kvEventStored, []uint64{1, 2}, []int64{300, 200}},
				{"pod-1.default", "llama", kvEventStored, []uint64{3}, []int64{100}},
				{"pod-1.default", "qwen", kvEventStored, []uint64{4, 5}, []int64{0, 0}},
			},
			want:       map[ownerModelKey][]uint64{pod1Qwen: {4, 5}, pod1Llama: {3}},
			wantTotals: map[string]int{"pod-1.default": 3},
		},
		{
			name: "a tenth of the limit is evicted at once",
			max:  20,
			messages: []message{
				{"pod-1.default", "qwen", kvEventStored, seq(1, 20), descendingAges},
				{"pod-1.default", "qwen", kvEventStored, []uint64{21}, []int64{0}},
			},
			want:       map[ownerModelKey][]uint64{pod1Qwen: seq(3, 21)},
			wantTotals: map[string]int{"pod-1.default": 19},
		},
		{
			name: "the room made by an eviction is used before evicting again",
			max:  20,
			messages: []message{
				{"pod-1.default", "qwen", kvEventStored, seq(1, 20), descendingAges},
				{"pod-1.default", "qwen", kvEventStored, []uint64{21}, []int64{0}},
				{"pod-1.default", "qwen", kvEventStored, []uint64{22}, []int64{0}},
			},
			want:       map[ownerModelKey][]uint64{pod1Qwen: seq(3, 22)},
			wantTotals: map[string]int{"pod-1.default": 20},
		},
		{
			name: "blocks stored in the same second are evicted only up to the batch",
			max:  20,
			messages: []message{
				{"pod-1.default", "qwen", kvEventStored, seq(1, 20), repeat(10, 20)},
				{"pod-1.default", "qwen", kvEventStored, []uint64{21}, []int64{0}},
			},
			wantTotals: map[string]int{"pod-1.default": 19},
		},
		{
			name: "re-stored blocks are kept",
			max:  3,
			messages: []message{
				{"pod-1.default", "qwen", kvEventStored, []uint64{1, 2, 3}, []int64{30, 20, 10}},
				{"pod-1.default", "qwen", kvEventStored, []uint64{1}, []int64{0}},
				{"pod-1.default", "qwen", kvEventStored, []uint64{4}, []int64{0}},
			},
			want:       map[ownerModelKey][]uint64{pod1Qwen: {1, 3, 4}},
			wantTotals: map[string]int{"pod-1.default": 3},
		},
		{
			name: "other pods keep a block evicted for one pod",
			max:  1,
			messages: []message{
				{"pod-1.default", "qwen", kvEventStored, []uint64{1}, []int64{10}},
				{"pod-2.default", "qwen", kvEventStored, []uint64{1}, []int64{10}},
				{"pod-1.default", "qwen", kvEventStored, []uint64{2}, []int64{0}},
			},
			want:       map[ownerModelKey][]uint64{pod1Qwen: {2}, pod2Qwen: {1}},
			wantTotals: map[string]int{"pod-1.default": 1, "pod-2.default": 1},
		},
		{
			name: "evicting the last block of another model removes its entries",
			max:  1,
			messages: []message{
				{"pod-1.default", "qwen", kvEventStored, []uint64{1}, []int64{10}},
				{"pod-1.default", "llama", kvEventStored, []uint64{2}, []int64{0}},
			},
			want:       map[ownerModelKey][]uint64{pod1Llama: {2}},
			wantTotals: map[string]int{"pod-1.default": 1},
		},
		{
			name: "evicting the last block of the model being stored",
			max:  1,
			messages: []message{
				{"pod-1.default", "qwen", kvEventStored, []uint64{1, 2}, []int64{10, 0}},
			},
			want:       map[ownerModelKey][]uint64{pod1Qwen: {2}},
			wantTotals: map[string]int{"pod-1.default": 1},
		},
		{
			name: "a message over the limit keeps its newest blocks",
			max:  3,
			messages: []message{
				{"pod-1.default", "qwen", kvEventStored, seq(1, 5), []int64{50, 40, 30, 20, 10}},
			},
			want:       map[ownerModelKey][]uint64{pod1Qwen: {3, 4, 5}},
			wantTotals: map[string]int{"pod-1.default": 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := NewKVBlockMemoryIndex(tt.max)
			now := time.Now().Unix()
			for _, p := range tt.messages {
				timestamps := make([]int64, len(p.ages))
				for i, age := range p.ages {
					timestamps[i] = now - age
				}
				payload := KVEventPayload{ModelName: p.model, Events: []KVEvent{
					{Type: p.eventType, BlockHashes: p.hashes, Timestamps: timestamps},
				}}
				if err := idx.Apply(p.owner, &payload); err != nil {
					t.Fatalf("Apply() failed: %v", err)
				}
			}

			held := indexedBlocks(t, idx)
			if tt.want != nil && !maps.EqualFunc(held, tt.want, slices.Equal[[]uint64]) {
				t.Errorf("held blocks = %v, want %v", held, tt.want)
			}
			idx.mu.RLock()
			defer idx.mu.RUnlock()
			if !maps.Equal(idx.ownerTotals, tt.wantTotals) {
				t.Errorf("ownerTotals = %v, want %v", idx.ownerTotals, tt.wantTotals)
			}
		})
	}
}
