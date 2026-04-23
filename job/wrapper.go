/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-23 18:54:34
 * @FilePath: \kronos-scheduler\job\wrapper.go
 * @Description: Job包装器 - 提供超时、重试、重叠防止等功能
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kamalyes/go-toolbox/pkg/retry"
)

// WrapperConfig 包装器配置
type WrapperConfig struct {
	// 超时时间(秒)
	Timeout int
	// 最大重试次数
	MaxRetries int
	// 重试间隔(秒)
	RetryInterval int
	// 是否防止重叠执行
	OverlapPrevent bool
}

// JobWrapper Job包装器
type JobWrapper struct {
	job    Job
	config WrapperConfig
	mu     *sync.Mutex
}

// NewJobWrapper 创建Job包装器
func NewJobWrapper(job Job, config WrapperConfig) *JobWrapper {
	wrapper := &JobWrapper{
		job:    job,
		config: config,
	}
	if config.OverlapPrevent {
		wrapper.mu = &sync.Mutex{}
	}
	return wrapper
}

// Name 实现Job接口
func (w *JobWrapper) Name() string {
	return w.job.Name()
}

// Description 实现Job接口
func (w *JobWrapper) Description() string {
	return w.job.Description()
}

// Execute 执行任务(带超时、重试、重叠防止)
func (w *JobWrapper) Execute(ctx context.Context) error {
	// 防止重叠执行
	if w.config.OverlapPrevent {
		if !w.mu.TryLock() {
			return fmt.Errorf("job %s is already running", w.job.Name())
		}
		defer w.mu.Unlock()
	}

	// 超时控制
	if w.config.Timeout > 0 {
		return w.executeWithTimeout(ctx)
	}

	// 重试控制
	if w.config.MaxRetries > 0 {
		return w.executeWithRetry(ctx)
	}

	// 直接执行
	return w.job.Execute(ctx)
}

// executeWithTimeout 带超时的执行
func (w *JobWrapper) executeWithTimeout(parentCtx context.Context) error {
	ctx, cancel := context.WithTimeout(parentCtx, time.Duration(w.config.Timeout)*time.Second)
	defer cancel()

	resultChan := make(chan error, 1)
	go func() {
		if w.config.MaxRetries > 0 {
			resultChan <- w.executeWithRetry(ctx)
		} else {
			resultChan <- w.job.Execute(ctx)
		}
	}()

	select {
	case err := <-resultChan:
		return err
	case <-ctx.Done():
		return fmt.Errorf("job %s execution timeout after %d seconds", w.job.Name(), w.config.Timeout)
	}
}

// executeWithRetry 带重试的执行(使用 go-toolbox retry)
func (w *JobWrapper) executeWithRetry(ctx context.Context) error {
	var lastErr error

	r := retry.NewRetryWithCtx(ctx).
		SetAttemptCount(w.config.MaxRetries + 1). // +1 因为包含首次执行
		SetInterval(time.Duration(w.config.RetryInterval) * time.Second).
		SetConditionFunc(func(err error) bool {
			// 使用Job的ShouldRetry判断
			if retryable, ok := w.job.(RetryableJob); ok {
				return retryable.ShouldRetry(err)
			}
			return true // 默认允许重试
		}).
		SetErrCallback(func(now, remain int, err error, funcName ...string) {
			lastErr = err
		})

	err := r.Do(func() error {
		return w.job.Execute(ctx)
	})

	if err != nil {
		return fmt.Errorf("job %s failed after %d retries: %w", w.job.Name(), w.config.MaxRetries, lastErr)
	}

	return nil
}
