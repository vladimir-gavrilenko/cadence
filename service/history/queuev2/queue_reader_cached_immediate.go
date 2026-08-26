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
	"sync/atomic"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/service/history/shard"
)

// cachedImmediateQueueReader is the immediate (transfer) cached queue reader. It embeds the
// shared cachedQueueReaderBase and populates the cache purely from notification injection:
// there is no prefetch cycle. It still implements the CachedQueueReader lifecycle, but its
// Start/Stop only flip lifecycle state and emit a single log line each, since there is no
// background loop to run. Unlike the timer reader, its cap guard drops the OLDEST tasks
// (LTrimBySize) so healthy domains keep serving the newest tasks from cache while a stalled
// domain's tasks fall through to the DB.
type cachedImmediateQueueReader struct {
	*cachedQueueReaderBase
}

// newCachedImmediateQueueReaderWithOptions builds an immediate cached reader from an already-built
// options struct. It reads no dynamic config, so the reader and its tests exist without referencing
// any dynamic-config key; the config-reading convenience constructor is added when the cache is
// wired into the transfer queue factory.
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

// Start marks the reader started. Unlike the scheduled reader there is no prefetch loop to
// launch, so this only flips lifecycle state (an atomic compare-and-swap on the base's status)
// and logs once. There is deliberately no "starting" log: the reader is already fully operational
// the moment it is constructed.
func (q *cachedImmediateQueueReader) Start() {
	if !atomic.CompareAndSwapInt32(&q.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	q.logger.Info("Immediate Cached Queue Reader state changed", tag.LifeCycleStarted)
}

// Stop marks the reader stopped. There is no background goroutine to cancel or wait on, so this
// only flips lifecycle state (an atomic compare-and-swap on the base's status) and logs once.
// There is deliberately no "stopping" log.
func (q *cachedImmediateQueueReader) Stop() {
	if !atomic.CompareAndSwapInt32(&q.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	q.logger.Info("Immediate Cached Queue Reader state changed", tag.LifeCycleStopped)
}
