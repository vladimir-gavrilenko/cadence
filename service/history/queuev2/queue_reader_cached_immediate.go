// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package queuev2

import (
	"context"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/shard"
)

// cachedImmediateQueueReader is the immediate (transfer) cached queue reader. It embeds the
// shared cachedQueueReaderBase and populates the cache purely from notification injection —
// there is no prefetch cycle, so it has no background lifecycle and implements only
// CachedQueueReader, not CachedQueueReaderDaemon. Unlike the timer reader, its cap guard drops
// the OLDEST tasks (LTrimBySize) so healthy domains keep serving the newest tasks from cache
// while a stalled domain's tasks fall through to the DB.
type cachedImmediateQueueReader struct {
	*cachedQueueReaderBase
}

func newCachedImmediateQueueReader(
	base QueueReader,
	queue InMemQueue,
	shard shard.Context,
	metricsScope metrics.Scope,
) *cachedImmediateQueueReader {
	config := shard.GetConfig()
	return newCachedImmediateQueueReaderWithOptions(
		base,
		queue,
		shard,
		shard.GetTimeSource(),
		shard.GetLogger().WithTags(tag.ComponentCachedQueueReader),
		metricsScope,
		&cachedQueueReaderOptions{
			Mode:                 config.TransferProcessorCachedQueueReaderMode,
			MaxSize:              config.TransferProcessorCacheMaxSize,
			ShadowSampleInterval: config.TransferProcessorCachedQueueReaderShadowSampleInterval,
		},
	)
}

func newCachedImmediateQueueReaderWithOptions(
	base QueueReader,
	queue InMemQueue,
	shard shard.Context,
	clockSource clock.TimeSource,
	logger log.Logger,
	metricsScope metrics.Scope,
	options *cachedQueueReaderOptions,
) *cachedImmediateQueueReader {
	return &cachedImmediateQueueReader{
		cachedQueueReaderBase: newCachedQueueReaderBase(
			base,
			queue,
			shard,
			clockSource,
			logger,
			metricsScope,
			options,
		),
	}
}

// LookAHead delegates straight to the base reader. Look-ahead is a scheduled/timer concern used by
// the scheduled queue's event loop to find when the next future timer fires; the immediate/transfer
// queue's event loop never calls it. This pass-through exists only to satisfy QueueReader, so the
// transfer reader carries no cache-aware look-ahead logic of its own.
func (q *cachedImmediateQueueReader) LookAHead(ctx context.Context, req *LookAHeadRequest) (*LookAHeadResponse, error) {
	return q.base.LookAHead(ctx, req)
}

// Inject adds transfer tasks that have just been persisted into the in-memory cache. Because
// task IDs are allocated and injected under the same shard write lock, in creation order, the
// cache is guaranteed to hold every transfer task on this host: injection both anchors the
// window and extends its upper bound. Tasks below the (already-evicted) lower bound are dropped;
// no-op when the cache is off.
func (q *cachedImmediateQueueReader) Inject(tasks []persistence.Task) {
	if q.isDisabled() {
		// Clear stale cache so re-enabling starts fresh instead of serving outdated
		// boundaries that cause cache misses.
		q.clearIfNotEmpty()
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	var injected, droppedBelow int64

	// anchoring is decided from the window state observed before this batch is inserted.
	wasEmpty := q.exclusiveUpperBound.Equal(persistence.MinimumHistoryTaskKey)

	var toPut []persistence.Task
	for _, t := range tasks {
		if t.GetTaskID() == 0 {
			// no tasks with taskID == 0 are expected
			continue
		}
		key := t.GetTaskKey()
		// Once the window is anchored, a key below the lower bound has already been evicted
		// (read-level or cap-guard). Reads for it correctly fall through to the DB, so drop it.
		if !wasEmpty && key.Less(q.inclusiveLowerBound) {
			droppedBelow++
			q.logger.Warn("task key is below the lower bound, dropping task",
				tag.Dynamic("taskKey", key),
				tag.Dynamic("cacheState", q.getState()),
			)
			continue
		}
		injected++
		if q.logger.DebugOn() {
			q.logger.Debug("injecting task",
				tag.Dynamic("taskKey", key),
				tag.Dynamic("cacheState", q.getState()),
			)
		}
		toPut = append(toPut, t)
	}

	q.emitInjectStatusCount(injectStatusInjected, injected)
	q.emitInjectStatusCount(injectStatusDroppedBelow, droppedBelow)

	q.putTasks(toPut, wasEmpty)
}

// putTasks inserts injected transfer tasks, anchors/extends the cached window, and enforces the
// size cap by dropping the OLDEST tasks (head eviction). Caller must hold q.mu.
func (q *cachedImmediateQueueReader) putTasks(tasks []persistence.Task, wasEmpty bool) {
	if len(tasks) == 0 {
		return
	}

	// Determine the min and max keys of this batch without assuming input ordering.
	minKey := tasks[0].GetTaskKey()
	maxKey := tasks[0].GetTaskKey()
	for _, t := range tasks[1:] {
		key := t.GetTaskKey()
		if key.Less(minKey) {
			minKey = key
		}
		if key.Greater(maxKey) {
			maxKey = key
		}
	}

	q.queue.PutTasks(tasks)

	// On first population, anchor the lower bound to the earliest injected key so the cache is
	// authoritative for [minKey, ...) onward. Tasks created before that (e.g. before the cache
	// was populated) have smaller keys, so reads for them correctly miss and fall through.
	if wasEmpty {
		q.updateInclusiveLowerBound(minKey)
	}

	// Extend the upper bound to just past the newest injected task.
	if newUpper := maxKey.Next(); newUpper.Greater(q.exclusiveUpperBound) {
		q.updateExclusiveUpperBound(newUpper)
	}

	// Cap guard: drop the oldest tasks until the cache fits MaxSize, advancing the lower bound.
	newLower, trimmed := q.queue.LTrimBySize(q.options.MaxSize())
	if !trimmed {
		return
	}
	if newLower.Equal(persistence.MinimumHistoryTaskKey) {
		// The trim emptied the queue (e.g. MaxSize <= 0); reset the window.
		q.updateInclusiveLowerBound(persistence.MinimumHistoryTaskKey)
		q.updateExclusiveUpperBound(persistence.MinimumHistoryTaskKey)
		return
	}
	q.updateInclusiveLowerBound(newLower)
}
