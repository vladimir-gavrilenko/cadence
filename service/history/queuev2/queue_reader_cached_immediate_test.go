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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/shard"
)

func testImmediateOptions(overrides ...func(*cachedQueueReaderOptions)) *cachedQueueReaderOptions {
	opts := &cachedQueueReaderOptions{
		Mode:                 dynamicproperties.GetStringPropertyFnFilteredByShardID("enabled"),
		MaxSize:              dynamicproperties.GetIntPropertyFn(100),
		ShadowSampleInterval: dynamicproperties.GetDurationPropertyFn(0),
	}
	for _, o := range overrides {
		if o == nil {
			continue
		}
		o(opts)
	}
	return opts
}

// setupImmediateCachedReader builds a transfer cached reader backed by a real in-memory queue
// and a mocked base reader.
func setupImmediateCachedReader(
	t *testing.T,
	ctrl *gomock.Controller,
	overrides ...func(*cachedQueueReaderOptions),
) (*cachedImmediateQueueReader, *MockQueueReader) {
	t.Helper()
	mockShard := shard.NewMockContext(ctrl)
	mockShard.EXPECT().GetRangeID().Return(int64(0)).AnyTimes()
	mockShard.EXPECT().GetConfig().Return(&config.Config{RangeSizeBits: 20}).AnyTimes()
	mockShard.EXPECT().GetShardID().Return(0).AnyTimes()

	mockBase := NewMockQueueReader(ctrl)
	r := newCachedImmediateQueueReaderWithOptions(
		mockBase,
		newInMemQueue(),
		mockShard,
		clock.NewMockedTimeSource(),
		testlogger.New(t),
		metrics.NoopScope,
		testImmediateOptions(overrides...),
	)
	return r, mockBase
}

func transferTask(taskID int64) persistence.Task {
	return &persistence.ActivityTask{
		TaskData: persistence.TaskData{TaskID: taskID},
	}
}

func immediateKey(taskID int64) persistence.HistoryTaskKey {
	return persistence.NewImmediateTaskKey(taskID)
}

func getTaskReq(inclusiveMin, exclusiveMax persistence.HistoryTaskKey) *GetTaskRequest {
	return &GetTaskRequest{
		Progress: &GetTaskProgress{
			Range: Range{
				InclusiveMinTaskKey: inclusiveMin,
				ExclusiveMaxTaskKey: exclusiveMax,
			},
			NextTaskKey: inclusiveMin,
		},
		Predicate: NewUniversalPredicate(),
		PageSize:  100,
	}
}

func TestCachedImmediateQueueReader_Inject_AnchorsAndExtends(t *testing.T) {
	ctrl := gomock.NewController(t)
	r, _ := setupImmediateCachedReader(t, ctrl)

	require.True(t, r.IsEmpty())

	r.Inject([]persistence.Task{transferTask(5), transferTask(6), transferTask(7)})

	require.False(t, r.IsEmpty())
	require.Equal(t, immediateKey(5), r.inclusiveLowerBound, "lower bound anchors to first injected key")
	require.Equal(t, immediateKey(7).Next(), r.exclusiveUpperBound, "upper bound extends past newest key")
	require.Equal(t, 3, r.queue.Len())

	// A later batch extends the upper bound but keeps the anchored lower bound.
	r.Inject([]persistence.Task{transferTask(8)})
	require.Equal(t, immediateKey(5), r.inclusiveLowerBound)
	require.Equal(t, immediateKey(8).Next(), r.exclusiveUpperBound)
	require.Equal(t, 4, r.queue.Len())
}

func TestCachedImmediateQueueReader_Inject_SkipsZeroTaskID(t *testing.T) {
	ctrl := gomock.NewController(t)
	r, _ := setupImmediateCachedReader(t, ctrl)

	r.Inject([]persistence.Task{transferTask(0), transferTask(10)})

	require.Equal(t, 1, r.queue.Len())
	require.Equal(t, immediateKey(10), r.inclusiveLowerBound)
	require.Equal(t, immediateKey(10).Next(), r.exclusiveUpperBound)
}

func TestCachedImmediateQueueReader_Inject_DropsBelowLowerBound(t *testing.T) {
	ctrl := gomock.NewController(t)
	r, _ := setupImmediateCachedReader(t, ctrl)

	r.Inject([]persistence.Task{transferTask(10), transferTask(11), transferTask(12), transferTask(13)})
	// Simulate the processor having advanced the read level to 12 (still within the window).
	r.UpdateReadLevel(immediateKey(12))
	require.Equal(t, immediateKey(12), r.inclusiveLowerBound)
	require.Equal(t, 2, r.queue.Len(), "tasks below the read level are trimmed")

	// A stale task below the lower bound is dropped, leaving the window unchanged.
	r.Inject([]persistence.Task{transferTask(11)})
	require.Equal(t, immediateKey(12), r.inclusiveLowerBound)
	require.Equal(t, 2, r.queue.Len())
}

func TestCachedImmediateQueueReader_Inject_CapGuardDropsOldest(t *testing.T) {
	ctrl := gomock.NewController(t)
	r, _ := setupImmediateCachedReader(t, ctrl, func(o *cachedQueueReaderOptions) {
		o.MaxSize = dynamicproperties.GetIntPropertyFn(3)
	})

	r.Inject([]persistence.Task{transferTask(1), transferTask(2), transferTask(3)})
	require.Equal(t, immediateKey(1), r.inclusiveLowerBound)

	// Exceeding MaxSize drops the oldest (head), advancing the lower bound.
	r.Inject([]persistence.Task{transferTask(4), transferTask(5)})
	require.Equal(t, 3, r.queue.Len())
	require.Equal(t, immediateKey(3), r.inclusiveLowerBound, "oldest tasks evicted from the head")
	require.Equal(t, immediateKey(5).Next(), r.exclusiveUpperBound)
}

func TestCachedImmediateQueueReader_GetTask_HitAndMiss(t *testing.T) {
	ctrl := gomock.NewController(t)
	r, mockBase := setupImmediateCachedReader(t, ctrl)

	r.Inject([]persistence.Task{transferTask(5), transferTask(6), transferTask(7)})

	// Covered range -> served from cache, base is never called.
	resp, err := r.GetTask(context.Background(), getTaskReq(immediateKey(5), immediateKey(8)))
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 3)

	// Range starting below the lower bound -> miss -> delegated to base.
	mockBase.EXPECT().GetTask(gomock.Any(), gomock.Any()).Return(&GetTaskResponse{}, nil).Times(1)
	_, err = r.GetTask(context.Background(), getTaskReq(immediateKey(1), immediateKey(8)))
	require.NoError(t, err)
}

func TestCachedImmediateQueueReader_Disabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	r, mockBase := setupImmediateCachedReader(t, ctrl, func(o *cachedQueueReaderOptions) {
		o.Mode = dynamicproperties.GetStringPropertyFnFilteredByShardID("disabled")
	})

	// Inject is a no-op while disabled.
	r.Inject([]persistence.Task{transferTask(5)})
	require.Equal(t, 0, r.queue.Len())

	// GetTask always delegates to base while disabled.
	mockBase.EXPECT().GetTask(gomock.Any(), gomock.Any()).Return(&GetTaskResponse{}, nil).Times(1)
	_, err := r.GetTask(context.Background(), getTaskReq(immediateKey(1), immediateKey(8)))
	require.NoError(t, err)
}

func TestCachedImmediateQueueReader_Clear(t *testing.T) {
	ctrl := gomock.NewController(t)
	r, _ := setupImmediateCachedReader(t, ctrl)

	r.Inject([]persistence.Task{transferTask(5), transferTask(6)})
	require.False(t, r.IsEmpty())

	r.Clear()
	require.True(t, r.IsEmpty())
	require.Equal(t, 0, r.queue.Len())
	require.Equal(t, persistence.MinimumHistoryTaskKey, r.inclusiveLowerBound)
	require.Equal(t, persistence.MinimumHistoryTaskKey, r.exclusiveUpperBound)
}
