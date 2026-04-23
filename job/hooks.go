/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-25 15:15:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 15:15:00
 * @FilePath: \kronos-scheduler\job\hooks.go
 * @Description: 任务生命周期钩子 - 提供任务执行前后的钩子机制
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package job

import (
	"context"
	"fmt"
)

// HookFunc 钩子函数类型
type HookFunc func(ctx context.Context) error

// ResultHookFunc 带结果的钩子函数类型
type ResultHookFunc func(ctx context.Context, result interface{}) error

// ErrorHookFunc 错误钩子函数类型
type ErrorHookFunc func(ctx context.Context, err error) error

// SkipFunc 跳过判断函数类型
type SkipFunc func(ctx context.Context) (bool, string)

// JobHooks 任务钩子配置
type JobHooks struct {
	// BeforeRun 任务执行前钩子
	BeforeRun HookFunc

	// AfterRun 任务执行后钩子(无论成功或失败都会执行)
	AfterRun ErrorHookFunc

	// OnSuccess 任务成功后钩子
	OnSuccess ResultHookFunc

	// OnFailure 任务失败后钩子
	OnFailure ErrorHookFunc

	// OnTimeout 任务超时后钩子
	OnTimeout ErrorHookFunc

	// OnAbort 任务中止后钩子
	OnAbort HookFunc

	// OnRetry 任务重试前钩子
	OnRetry func(ctx context.Context, attempt int, err error) error

	// ShouldSkip 判断是否跳过执行
	ShouldSkip SkipFunc

	// OnSkip 跳过执行时的钩子
	OnSkip func(ctx context.Context, reason string) error
}

// NewJobHooks 创建新的钩子配置
func NewJobHooks() *JobHooks {
	return &JobHooks{}
}

// WithBeforeRun 设置执行前钩子
func (h *JobHooks) WithBeforeRun(fn HookFunc) *JobHooks {
	h.BeforeRun = fn
	return h
}

// WithAfterRun 设置执行后钩子
func (h *JobHooks) WithAfterRun(fn ErrorHookFunc) *JobHooks {
	h.AfterRun = fn
	return h
}

// WithOnSuccess 设置成功钩子
func (h *JobHooks) WithOnSuccess(fn ResultHookFunc) *JobHooks {
	h.OnSuccess = fn
	return h
}

// WithOnFailure 设置失败钩子
func (h *JobHooks) WithOnFailure(fn ErrorHookFunc) *JobHooks {
	h.OnFailure = fn
	return h
}

// WithOnTimeout 设置超时钩子
func (h *JobHooks) WithOnTimeout(fn ErrorHookFunc) *JobHooks {
	h.OnTimeout = fn
	return h
}

// WithOnAbort 设置中止钩子
func (h *JobHooks) WithOnAbort(fn HookFunc) *JobHooks {
	h.OnAbort = fn
	return h
}

// WithOnRetry 设置重试钩子
func (h *JobHooks) WithOnRetry(fn func(ctx context.Context, attempt int, err error) error) *JobHooks {
	h.OnRetry = fn
	return h
}

// WithShouldSkip 设置跳过判断函数
func (h *JobHooks) WithShouldSkip(fn SkipFunc) *JobHooks {
	h.ShouldSkip = fn
	return h
}

// WithOnSkip 设置跳过钩子
func (h *JobHooks) WithOnSkip(fn func(ctx context.Context, reason string) error) *JobHooks {
	h.OnSkip = fn
	return h
}

// ExecuteBeforeRun 执行前置钩子
func (h *JobHooks) ExecuteBeforeRun(ctx context.Context) error {
	if h.BeforeRun != nil {
		return h.BeforeRun(ctx)
	}
	return nil
}

// ExecuteAfterRun 执行后置钩子
func (h *JobHooks) ExecuteAfterRun(ctx context.Context, err error) error {
	if h.AfterRun != nil {
		return h.AfterRun(ctx, err)
	}
	return nil
}

// ExecuteOnSuccess 执行成功钩子
func (h *JobHooks) ExecuteOnSuccess(ctx context.Context, result interface{}) error {
	if h.OnSuccess != nil {
		return h.OnSuccess(ctx, result)
	}
	return nil
}

// ExecuteOnFailure 执行失败钩子
func (h *JobHooks) ExecuteOnFailure(ctx context.Context, err error) error {
	if h.OnFailure != nil {
		return h.OnFailure(ctx, err)
	}
	return nil
}

// ExecuteOnTimeout 执行超时钩子
func (h *JobHooks) ExecuteOnTimeout(ctx context.Context, err error) error {
	if h.OnTimeout != nil {
		return h.OnTimeout(ctx, err)
	}
	return nil
}

// ExecuteOnAbort 执行中止钩子
func (h *JobHooks) ExecuteOnAbort(ctx context.Context) error {
	if h.OnAbort != nil {
		return h.OnAbort(ctx)
	}
	return nil
}

// ExecuteOnRetry 执行重试钩子
func (h *JobHooks) ExecuteOnRetry(ctx context.Context, attempt int, err error) error {
	if h.OnRetry != nil {
		return h.OnRetry(ctx, attempt, err)
	}
	return nil
}

// CheckShouldSkip 检查是否应该跳过执行
func (h *JobHooks) CheckShouldSkip(ctx context.Context) (bool, string) {
	if h.ShouldSkip != nil {
		return h.ShouldSkip(ctx)
	}
	return false, ""
}

// ExecuteOnSkip 执行跳过钩子
func (h *JobHooks) ExecuteOnSkip(ctx context.Context, reason string) error {
	if h.OnSkip != nil {
		return h.OnSkip(ctx, reason)
	}
	return nil
}

// HookChain 钩子链
type HookChain struct {
	hooks []HookFunc
}

// NewHookChain 创建新的钩子链
func NewHookChain() *HookChain {
	return &HookChain{
		hooks: make([]HookFunc, 0),
	}
}

// Add 添加钩子
func (c *HookChain) Add(hook HookFunc) *HookChain {
	c.hooks = append(c.hooks, hook)
	return c
}

// Execute 按顺序执行所有钩子
func (c *HookChain) Execute(ctx context.Context) error {
	for i, hook := range c.hooks {
		if err := hook(ctx); err != nil {
			return fmt.Errorf("hook %d failed: %w", i, err)
		}
	}
	return nil
}

// ExecuteReverse 逆序执行所有钩子(用于清理操作)
func (c *HookChain) ExecuteReverse(ctx context.Context) error {
	for i := len(c.hooks) - 1; i >= 0; i-- {
		if err := c.hooks[i](ctx); err != nil {
			return fmt.Errorf("hook %d failed: %w", i, err)
		}
	}
	return nil
}

// CommonHooks 常用钩子函数
type CommonHooks struct{}

// LogBeforeRun 记录执行前日志的钩子
func (CommonHooks) LogBeforeRun(jobID string) HookFunc {
	return func(ctx context.Context) error {
		fmt.Printf("[%s] Job starting...\n", jobID)
		return nil
	}
}

// LogAfterRun 记录执行后日志的钩子
func (CommonHooks) LogAfterRun(jobID string) ErrorHookFunc {
	return func(ctx context.Context, err error) error {
		if err != nil {
			fmt.Printf("[%s] Job completed with error: %v\n", jobID, err)
		} else {
			fmt.Printf("[%s] Job completed successfully\n", jobID)
		}
		return nil
	}
}

// NotifyOnFailure 失败时发送通知的钩子
func (CommonHooks) NotifyOnFailure(jobID string, notifyFunc func(string, error)) ErrorHookFunc {
	return func(ctx context.Context, err error) error {
		if notifyFunc != nil {
			notifyFunc(jobID, err)
		}
		return nil
	}
}

// RetryWithDelay 带延迟的重试钩子
func (CommonHooks) RetryWithDelay(jobID string, delayFunc func(int) error) func(context.Context, int, error) error {
	return func(ctx context.Context, attempt int, err error) error {
		fmt.Printf("[%s] Retry attempt %d after error: %v\n", jobID, attempt, err)
		if delayFunc != nil {
			return delayFunc(attempt)
		}
		return nil
	}
}

// SkipIfBusy 系统繁忙时跳过的钩子
func (CommonHooks) SkipIfBusy(checkFunc func() bool) SkipFunc {
	return func(ctx context.Context) (bool, string) {
		if checkFunc != nil && checkFunc() {
			return true, "system is busy"
		}
		return false, ""
	}
}

// SkipIfMaintenanceMode 维护模式时跳过的钩子
func (CommonHooks) SkipIfMaintenanceMode(isMaintenanceFunc func() bool) SkipFunc {
	return func(ctx context.Context) (bool, string) {
		if isMaintenanceFunc != nil && isMaintenanceFunc() {
			return true, "system is in maintenance mode"
		}
		return false, ""
	}
}
