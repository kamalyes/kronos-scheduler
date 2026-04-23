/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 15:05:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 15:10:27
 * @FilePath: \kronos-scheduler\scheduler\decorator_test.go
 * @Description: 装饰器测试 - 职责单一，使用 assert 校验
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/kamalyes/kronos-scheduler/job"
	"github.com/stretchr/testify/assert"
)

// mockJob 模拟任务
type mockJob struct {
	name        string
	description string
	executeFunc func(ctx context.Context) error
}

func (m *mockJob) Name() string                      { return m.name }
func (m *mockJob) Description() string               { return m.description }
func (m *mockJob) Execute(ctx context.Context) error { return m.executeFunc(ctx) }

// TestRecoverShouldCatchPanic 测试捕获 panic
func TestRecoverShouldCatchPanic(t *testing.T) {
	j := &mockJob{
		name:        "panic-job",
		executeFunc: func(ctx context.Context) error { panic("test panic") },
	}

	wrapped := Recover(nil)(j)
	err := wrapped.Execute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "任务 panic: test panic")
}

// TestRecoverShouldNotAffectNormalJob 测试不影响正常任务
func TestRecoverShouldNotAffectNormalJob(t *testing.T) {
	j := &mockJob{
		name:        "normal-job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	wrapped := Recover(nil)(j)
	err := wrapped.Execute(context.Background())

	assert.NoError(t, err)
}

// TestSkipIfRunningShouldSkipConcurrent 测试跳过并发执行
func TestSkipIfRunningShouldSkipConcurrent(t *testing.T) {
	executed := make(chan struct{})
	j := &mockJob{
		name: "slow-job",
		executeFunc: func(ctx context.Context) error {
			close(executed)
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	}

	wrapped := SkipIfRunning(nil)(j)

	go wrapped.Execute(context.Background())
	<-executed

	err := wrapped.Execute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "is already running")
}

// TestSkipIfRunningShouldAllowSequential 测试允许顺序执行
func TestSkipIfRunningShouldAllowSequential(t *testing.T) {
	count := 0
	j := &mockJob{
		name:        "test-job",
		executeFunc: func(ctx context.Context) error { count++; return nil },
	}

	wrapped := SkipIfRunning(nil)(j)
	err1 := wrapped.Execute(context.Background())
	err2 := wrapped.Execute(context.Background())

	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, 2, count)
}

// TestDelayIfRunningShouldWaitForPrevious 测试等待前一个任务完成
func TestDelayIfRunningShouldWaitForPrevious(t *testing.T) {
	order := []int{}
	j := &mockJob{
		name: "slow-job",
		executeFunc: func(ctx context.Context) error {
			order = append(order, 1)
			time.Sleep(50 * time.Millisecond)
			order = append(order, 2)
			return nil
		},
	}

	wrapped := DelayIfRunning(nil)(j)

	go wrapped.Execute(context.Background())
	time.Sleep(10 * time.Millisecond)
	wrapped.Execute(context.Background())

	assert.Equal(t, []int{1, 2, 1, 2}, order)
}

// TestWithTimeoutShouldTimeout 测试超时功能
func TestWithTimeoutShouldTimeout(t *testing.T) {
	j := &mockJob{
		name:        "slow-job",
		executeFunc: func(ctx context.Context) error { time.Sleep(200 * time.Millisecond); return nil },
	}

	wrapped := WithTimeout(50*time.Millisecond, nil)(j)
	err := wrapped.Execute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "超时")
}

// TestWithTimeoutShouldNotTimeoutFast 测试不影响快速任务
func TestWithTimeoutShouldNotTimeoutFast(t *testing.T) {
	j := &mockJob{
		name:        "fast-job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	wrapped := WithTimeout(100*time.Millisecond, nil)(j)
	err := wrapped.Execute(context.Background())

	assert.NoError(t, err)
}

// TestWithRetryShouldRetryAndSucceed 测试重试并最终成功
func TestWithRetryShouldRetryAndSucceed(t *testing.T) {
	attempts := 0
	j := &mockJob{
		name: "flaky-job",
		executeFunc: func(ctx context.Context) error {
			attempts++
			if attempts < 3 {
				return errors.New("temp error")
			}
			return nil
		},
	}

	wrapped := WithRetryDecorator(3, 10*time.Millisecond, 0, nil)(j)
	err := wrapped.Execute(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

// TestWithRetryShouldFailAfterMaxRetries 测试达到最大重试后失败
func TestWithRetryShouldFailAfterMaxRetries(t *testing.T) {
	attempts := 0
	j := &mockJob{
		name:        "fail-job",
		executeFunc: func(ctx context.Context) error { attempts++; return errors.New("error") },
	}

	wrapped := WithRetryDecorator(2, 10*time.Millisecond, 0, nil)(j)
	err := wrapped.Execute(context.Background())

	assert.Error(t, err)
	assert.Equal(t, 3, attempts) // 初始 + 2次重试
	assert.Contains(t, err.Error(), "重试 2 次后仍失败")
}

// TestWithRetryShouldApplyJitter 测试抖动功能
func TestWithRetryShouldApplyJitter(t *testing.T) {
	j := &mockJob{
		name:        "fail-job",
		executeFunc: func(ctx context.Context) error { return errors.New("error") },
	}

	start := time.Now()
	wrapped := WithRetryDecorator(2, 50*time.Millisecond, 0.5, nil)(j)
	wrapped.Execute(context.Background())
	duration := time.Since(start)

	// 抖动范围: (50ms±25ms) * 2 = 50ms ~ 150ms
	assert.Greater(t, duration, 40*time.Millisecond)
	assert.Less(t, duration, 200*time.Millisecond)
}

// TestWithJobLoggerShouldLogSuccess 测试记录成功执行
func TestWithJobLoggerShouldLogSuccess(t *testing.T) {
	j := &mockJob{
		name:        "test-job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	wrapped := WithJobLogger(nil)(j)
	err := wrapped.Execute(context.Background())

	assert.NoError(t, err)
}

// TestWithJobLoggerShouldLogError 测试记录执行错误
func TestWithJobLoggerShouldLogError(t *testing.T) {
	expectedErr := errors.New("job error")
	j := &mockJob{
		name:        "fail-job",
		executeFunc: func(ctx context.Context) error { return expectedErr },
	}

	wrapped := WithJobLogger(nil)(j)
	err := wrapped.Execute(context.Background())

	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

// TestWithHooksShouldExecuteBefore 测试执行前置钩子
func TestWithHooksShouldExecuteBefore(t *testing.T) {
	called := false
	before := func(ctx context.Context, name string) error {
		called = true
		assert.Equal(t, "test-job", name)
		return nil
	}

	j := &mockJob{
		name:        "test-job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	wrapped := WithHooks(before, nil)(j)
	wrapped.Execute(context.Background())

	assert.True(t, called)
}

// TestWithHooksShouldExecuteAfter 测试执行后置钩子
func TestWithHooksShouldExecuteAfter(t *testing.T) {
	called := false
	after := func(ctx context.Context, name string) error {
		called = true
		return nil
	}

	j := &mockJob{
		name:        "test-job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	wrapped := WithHooks(nil, after)(j)
	wrapped.Execute(context.Background())

	assert.True(t, called)
}

// TestWithHooksShouldFailOnBeforeError 测试前置钩子失败
func TestWithHooksShouldFailOnBeforeError(t *testing.T) {
	hookErr := errors.New("hook error")
	before := func(ctx context.Context, name string) error { return hookErr }

	jobExecuted := false
	j := &mockJob{
		name:        "test-job",
		executeFunc: func(ctx context.Context) error { jobExecuted = true; return nil },
	}

	wrapped := WithHooks(before, nil)(j)
	err := wrapped.Execute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "前置钩子失败")
	assert.False(t, jobExecuted)
}

// TestWithHooksShouldExecuteAfterEvenOnJobError 测试即使任务失败也执行后置钩子
func TestWithHooksShouldExecuteAfterEvenOnJobError(t *testing.T) {
	afterCalled := false
	after := func(ctx context.Context, name string) error {
		afterCalled = true
		return nil
	}

	j := &mockJob{
		name:        "fail-job",
		executeFunc: func(ctx context.Context) error { return errors.New("job error") },
	}

	wrapped := WithHooks(nil, after)(j)
	wrapped.Execute(context.Background())

	assert.True(t, afterCalled)
}

// TestJobChainShouldApplyMultiple 测试链式应用多个装饰器
func TestJobChainShouldApplyMultiple(t *testing.T) {
	count := 0
	j := &mockJob{
		name:        "test-job",
		executeFunc: func(ctx context.Context) error { count++; return nil },
	}

	chain := NewChain(Recover(nil), WithJobLogger(nil))
	wrapped := chain.Then(j)
	err := wrapped.Execute(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

// TestJobChainShouldApplyInOrder 测试装饰器按顺序应用
func TestJobChainShouldApplyInOrder(t *testing.T) {
	order := []string{}

	dec1 := func(j job.Job) job.Job {
		return &mockJob{
			name: j.Name(),
			executeFunc: func(ctx context.Context) error {
				order = append(order, "dec1-before")
				err := j.Execute(ctx)
				order = append(order, "dec1-after")
				return err
			},
		}
	}

	dec2 := func(j job.Job) job.Job {
		return &mockJob{
			name: j.Name(),
			executeFunc: func(ctx context.Context) error {
				order = append(order, "dec2-before")
				err := j.Execute(ctx)
				order = append(order, "dec2-after")
				return err
			},
		}
	}

	j := &mockJob{
		name:        "test-job",
		executeFunc: func(ctx context.Context) error { order = append(order, "job"); return nil },
	}

	chain := NewChain(dec1, dec2)
	chain.Then(j).Execute(context.Background())

	expected := []string{"dec1-before", "dec2-before", "job", "dec2-after", "dec1-after"}
	assert.Equal(t, expected, order)
}

// TestJobChainAppendShouldAddDecorator 测试 Append 添加装饰器
func TestJobChainAppendShouldAddDecorator(t *testing.T) {
	j := &mockJob{
		name:        "test-job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	chain := NewChain(Recover(nil))
	chain.Append(WithJobLogger(nil))
	wrapped := chain.Then(j)

	assert.NotNil(t, wrapped)
	assert.Equal(t, "test-job", wrapped.Name())
}

// TestWrappedJobShouldPreserveMetadata 测试保留任务元数据
func TestWrappedJobShouldPreserveMetadata(t *testing.T) {
	original := &mockJob{
		name:        "original-job",
		description: "original desc",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	wrapped := Recover(nil)(original)

	assert.Equal(t, "original-job", wrapped.Name())
	assert.Equal(t, "original desc", wrapped.Description())
}

// TestNilLoggerShouldWork 测试 nil logger 能正常工作
func TestNilLoggerShouldWork(t *testing.T) {
	j := &mockJob{
		name:        "test-job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	decorators := []JobWrapper{
		Recover(nil),
		SkipIfRunning(nil),
		DelayIfRunning(nil),
		WithTimeout(100*time.Millisecond, nil),
		WithRetryDecorator(1, 10*time.Millisecond, 0, nil),
		WithJobLogger(nil),
	}

	for _, dec := range decorators {
		wrapped := dec(j)
		err := wrapped.Execute(context.Background())
		assert.NoError(t, err, fmt.Sprintf("%T should handle nil logger", dec))
	}
}
