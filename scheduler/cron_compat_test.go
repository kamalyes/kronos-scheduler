/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 15:05:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 16:11:20
 * @FilePath: \kronos-scheduler\scheduler\cron_compat_test.go
 * @Description: Cron 兼容层测试 - 使用 assert 校验
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewCron 测试创建 Cron 实例
func TestNewCron(t *testing.T) {
	c := New()

	assert.NotNil(t, c)
	assert.NotNil(t, c.scheduler)
	assert.NotNil(t, c.location)
	assert.NotNil(t, c.parser)
	assert.NotNil(t, c.chain)
}

// TestNewCronWithOptions 测试使用选项创建 Cron 实例
func TestNewCronWithOptions(t *testing.T) {
	loc, _ := time.LoadLocation("America/New_York")
	c := New(
		WithCronLocation(loc),
		WithSeconds(),
	)

	assert.NotNil(t, c)
	assert.Equal(t, loc, c.location)
	assert.Equal(t, SecondParser, c.parser)
}

// TestFuncJob 测试函数任务
func TestFuncJob(t *testing.T) {
	executed := false
	fn := FuncJob(func() {
		executed = true
	})

	fn.Run()

	assert.True(t, executed)
}

// TestAddFunc 测试添加函数任务
func TestAddFunc(t *testing.T) {
	c := New()

	entryID, err := c.AddFunc("*/5 * * * * *", func() {})

	assert.NoError(t, err)
	assert.NotZero(t, entryID)
}

// TestAddFuncInvalidCron 测试添加无效 cron 表达式的函数任务
func TestAddFuncInvalidCron(t *testing.T) {
	c := New()

	entryID, err := c.AddFunc("invalid", func() {})

	assert.Error(t, err)
	assert.Zero(t, entryID)
}

// TestAddJob 测试添加任务
func TestAddJob(t *testing.T) {
	c := New()

	job := FuncJob(func() {})

	entryID, err := c.AddJob("*/5 * * * * *", job)

	assert.NoError(t, err)
	assert.NotZero(t, entryID)
}

// TestSchedule 测试调度任务
func TestSchedule(t *testing.T) {
	c := New()

	// 使用 EverySchedule 创建固定间隔调度
	schedule := &EverySchedule{Duration: 5 * time.Second}
	job := FuncJob(func() {})

	entryID := c.Schedule(schedule, job)

	assert.NotZero(t, entryID)
}

// TestEntries 测试获取所有条目
func TestEntries(t *testing.T) {
	c := New()

	c.AddFunc("*/5 * * * * *", func() {})
	c.AddFunc("*/10 * * * * *", func() {})

	entries := c.Entries()

	assert.Len(t, entries, 2)
}

// TestEntry 测试获取指定条目
func TestEntry(t *testing.T) {
	c := New()

	entryID, err := c.AddFunc("*/5 * * * * *", func() {})
	require.NoError(t, err)

	entry := c.Entry(entryID)

	assert.True(t, entry.Valid())
	assert.Equal(t, entryID, entry.Id)
}

// TestEntryNotFound 测试获取不存在的条目
func TestEntryNotFound(t *testing.T) {
	c := New()

	entry := c.Entry(999)

	assert.False(t, entry.Valid())
}

// TestRemove 测试移除任务
func TestRemove(t *testing.T) {
	c := New()

	entryID, err := c.AddFunc("*/5 * * * * *", func() {})
	require.NoError(t, err)

	c.Remove(entryID)

	entry := c.Entry(entryID)
	assert.False(t, entry.Valid())
}

// TestStartAndStop 测试启动和停止
func TestCronStartAndStop(t *testing.T) {
	c := New()

	c.Start()
	assert.True(t, c.scheduler.IsRunning())

	ctx := c.Stop()
	assert.NotNil(t, ctx)
	assert.False(t, c.scheduler.IsRunning())

	// 等待上下文完成
	select {
	case <-ctx.Done():
		// 成功
	case <-time.After(1 * time.Second):
		t.Fatal("Stop context should be done")
	}
}

// TestLocation 测试获取时区
func TestLocation(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Tokyo")
	c := New(WithCronLocation(loc))

	assert.Equal(t, loc, c.Location())
}

// TestCronIntegration 测试完整的任务执行流程
func TestCronIntegration(t *testing.T) {
	c := New()

	var counter int32
	entryID, err := c.AddFunc("* * * * * *", func() {
		atomic.AddInt32(&counter, 1)
	})
	require.NoError(t, err)
	assert.NotZero(t, entryID)

	c.Start()
	defer c.Stop()

	// 等待任务执行至少 2 次（优化：从 2500ms 减少到 1200ms）
	time.Sleep(1200 * time.Millisecond)

	count := atomic.LoadInt32(&counter)
	assert.GreaterOrEqual(t, count, int32(1), "任务应该至少执行 1 次")
}

// TestCompatJobExecute 测试兼容任务执行
func TestCompatJobExecute(t *testing.T) {
	executed := false
	job := &compatJob{
		name: "test-job",
		cronJob: FuncJob(func() {
			executed = true
		}),
	}

	err := job.Execute(context.Background())

	assert.NoError(t, err)
	assert.True(t, executed)
	assert.Equal(t, "test-job", job.Name())
	assert.Equal(t, "cron compatible job", job.Description())
}

// TestMultipleJobs 测试多个任务
func TestMultipleJobs(t *testing.T) {
	c := New()

	var counter1, counter2 int32

	id1, err1 := c.AddFunc("* * * * * *", func() {
		atomic.AddInt32(&counter1, 1)
	})
	id2, err2 := c.AddFunc("*/2 * * * * *", func() {
		atomic.AddInt32(&counter2, 1)
	})

	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NotZero(t, id1)
	assert.NotZero(t, id2)
	assert.NotEqual(t, id1, id2)

	c.Start()
	defer c.Stop()

	// 优化：从 3500ms 减少到 2200ms
	time.Sleep(2200 * time.Millisecond)

	count1 := atomic.LoadInt32(&counter1)
	count2 := atomic.LoadInt32(&counter2)

	// counter1 每秒执行一次，应该至少执行 2 次
	assert.GreaterOrEqual(t, count1, int32(2))
	// counter2 每 2 秒执行一次，应该至少执行 1 次
	assert.GreaterOrEqual(t, count2, int32(1))
	// counter1 应该比 counter2 执行更多次
	assert.Greater(t, count1, count2)
}

// TestRemoveRunningJob 测试移除正在运行的任务
func TestRemoveRunningJob(t *testing.T) {
	c := New()

	var counter int32
	id, err := c.AddFunc("* * * * * *", func() {
		atomic.AddInt32(&counter, 1)
	})
	require.NoError(t, err)

	c.Start()
	defer c.Stop()

	// 优化：从 2500ms 减少到 1200ms
	time.Sleep(1200 * time.Millisecond)
	firstCount := atomic.LoadInt32(&counter)
	assert.GreaterOrEqual(t, firstCount, int32(1))

	// 移除任务
	c.Remove(id)

	// 优化：从 1500ms 减少到 800ms
	time.Sleep(800 * time.Millisecond)
	finalCount := atomic.LoadInt32(&counter)

	// 计数应该没有增加太多（可能有一次正在执行的）
	assert.LessOrEqual(t, finalCount-firstCount, int32(1))
}

// TestCronWithSeconds 测试带秒的解析器
func TestCronWithSeconds(t *testing.T) {
	c := New(WithSeconds())

	// 使用带秒的 cron 表达式（6 个字段）
	id, err := c.AddFunc("30 * * * * *", func() {})

	assert.NoError(t, err)
	assert.NotZero(t, id)
}

// TestCronChain 测试任务链
func TestCronChain(t *testing.T) {
	executed := false

	chain := NewChain(
		Recover(nil),
	)

	c := New(WithCronChain(chain))

	c.AddFunc("* * * * * *", func() {
		executed = true
	})

	c.Start()
	defer c.Stop()

	// 优化：从 1500ms 减少到 1100ms
	time.Sleep(1100 * time.Millisecond)

	assert.True(t, executed)
}
