/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 21:30:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 19:32:30
 * @FilePath: \kronos-scheduler\scheduler\decorator.go
 * @Description: 任务装饰器 - 提供链式调用、日志、恢复、延迟等能力
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/kamalyes/kronos-scheduler/job"
	"github.com/kamalyes/kronos-scheduler/logger"
	"github.com/kamalyes/go-toolbox/pkg/breaker"
	"github.com/kamalyes/go-toolbox/pkg/mathx"
)

// JobWrapper 任务包装器 - 用于装饰任务，添加额外行为
type JobWrapper func(job.Job) job.Job

// JobChain 任务链 - 链式组合多个装饰器
type JobChain struct {
	wrappers []JobWrapper
}

// NewChain 创建任务链
func NewChain(wrappers ...JobWrapper) *JobChain {
	return &JobChain{wrappers: wrappers}
}

// Then 应用链中的所有装饰器到任务(从内到外)
//
// 示例：
//
//	chain := NewChain(Recover(), SkipIfRunning(), Log())
//	wrappedJob := chain.Then(myJob)
//
// 等价于：Log(SkipIfRunning(Recover(myJob)))
func (c *JobChain) Then(j job.Job) job.Job {
	for i := len(c.wrappers) - 1; i >= 0; i-- {
		j = c.wrappers[i](j)
	}
	return j
}

// Append 追加装饰器到链末尾(链式调用)
func (c *JobChain) Append(wrapper JobWrapper) *JobChain {
	c.wrappers = append(c.wrappers, wrapper)
	return c
}

// wrappedJob 包装后的任务
type wrappedJob struct {
	name        string
	description string
	execute     func(ctx context.Context) error
}

func (w *wrappedJob) Name() string                      { return w.name }
func (w *wrappedJob) Description() string               { return w.description }
func (w *wrappedJob) Execute(ctx context.Context) error { return w.execute(ctx) }

// Recover 恢复装饰器 - 捕获 panic 并记录
func Recover(log Logger) JobWrapper {
	log = mathx.IF(log == nil, logger.NewNoOpLogger(), log)
	return func(j job.Job) job.Job {
		return &wrappedJob{
			name:        j.Name(),
			description: j.Description(),
			execute: func(ctx context.Context) (err error) {
				defer func() {
					if r := recover(); r != nil {
						stack := debug.Stack()
						err = fmt.Errorf("%w: %v\n堆栈:\n%s", ErrTaskPanic, r, string(stack))
						log.Errorf("任务执行 panic: %v, job=%s", r, j.Name())
					}
				}()
				return j.Execute(ctx)
			},
		}
	}
}

// SkipIfRunning 跳过装饰器 - 如果任务正在执行则跳过本次调度
func SkipIfRunning(log Logger) JobWrapper {
	log = mathx.IF(log == nil, logger.NewNoOpLogger(), log)
	var ch = make(chan struct{}, 1)
	ch <- struct{}{}
	return func(j job.Job) job.Job {
		return &wrappedJob{
			name:        j.Name(),
			description: j.Description(),
			execute: func(ctx context.Context) error {
				select {
				case v := <-ch:
					defer func() { ch <- v }()
					return j.Execute(ctx)
				default:
					log.InfoContext(ctx, "跳过正在运行的任务: %s", j.Name())
					return fmt.Errorf("%w: %s", ErrJobAlreadyRunning, j.Name())
				}
			},
		}
	}
}

// DelayIfRunning 延迟装饰器 - 如果任务正在执行则等待前一个执行完成
func DelayIfRunning(log Logger) JobWrapper {
	log = mathx.IF(log == nil, logger.NewNoOpLogger(), log)
	var mu sync.Mutex
	return func(j job.Job) job.Job {
		return &wrappedJob{
			name:        j.Name(),
			description: j.Description(),
			execute: func(ctx context.Context) error {
				start := time.Now()
				mu.Lock()
				defer mu.Unlock()

				if delay := time.Since(start); delay > time.Minute {
					log.InfoContext(ctx, "任务延迟执行: %s, delay=%v", j.Name(), delay)
				}
				return j.Execute(ctx)
			},
		}
	}
}

// WithTimeout 超时装饰器 - 为任务执行添加超时限制
func WithTimeout(timeout time.Duration, log Logger) JobWrapper {
	log = mathx.IF(log == nil, logger.NewNoOpLogger(), log)
	return func(j job.Job) job.Job {
		return &wrappedJob{
			name:        j.Name(),
			description: j.Description(),
			execute: func(ctx context.Context) error {
				ctx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()

				done := make(chan error, 1)
				go func() {
					done <- j.Execute(ctx)
				}()

				select {
				case err := <-done:
					return err
				case <-ctx.Done():
					err := fmt.Errorf("任务执行超时 (限制: %v)", timeout)
					log.Errorf("任务超时: %s, timeout=%v", j.Name(), timeout)
					return err
				}
			},
		}
	}
}

// WithRetryDecorator 重试装饰器 - 失败时自动重试(支持抖动)
func WithRetryDecorator(maxRetries int, retryDelay time.Duration, jitter float64, log Logger) JobWrapper {
	log = mathx.IF(log == nil, logger.NewNoOpLogger(), log)
	return func(j job.Job) job.Job {
		return &wrappedJob{
			name:        j.Name(),
			description: j.Description(),
			execute: func(ctx context.Context) error {
				var lastErr error
				for i := 0; i <= maxRetries; i++ {
					if i > 0 {
						log.InfoContext(ctx, "任务重试: %s, attempt=%d, maxRetries=%d", j.Name(), i, maxRetries)

						// 应用抖动：retryDelay ± (retryDelay * jitter)
						actualDelay := retryDelay
						if jitter > 0 && jitter <= 1 {
							jitterRange := float64(retryDelay) * jitter
							jitterOffset := (2*float64(i%2) - 1) * jitterRange // 交替正负抖动
							actualDelay = time.Duration(float64(retryDelay) + jitterOffset)
						}

						time.Sleep(actualDelay)
					}

					lastErr = j.Execute(ctx)
					if lastErr == nil {
						return nil
					}

					// 检查是否支持重试判断
					if retryable, ok := j.(job.RetryableJob); ok {
						if !retryable.ShouldRetry(lastErr) {
							return lastErr
						}
					}
				}

				log.Errorf("任务重试失败: %s, maxRetries=%d, error=%v", j.Name(), maxRetries, lastErr)
				return fmt.Errorf("%w: %s 重试 %d 次后仍失败: %v", ErrRetryExhausted, j.Name(), maxRetries, lastErr)
			},
		}
	}
}

// WithJobLogger 日志装饰器 - 记录任务执行日志
func WithJobLogger(log Logger) JobWrapper {
	log = mathx.IF(log == nil, logger.NewNoOpLogger(), log)
	return func(j job.Job) job.Job {
		return &wrappedJob{
			name:        j.Name(),
			description: j.Description(),
			execute: func(ctx context.Context) error {
				start := time.Now()
				log.InfoContext(ctx, "任务开始执行: %s, time=%v", j.Name(), start)

				err := j.Execute(ctx)
				duration := time.Since(start)

				if err != nil {
					log.Errorf("任务执行失败: %s, duration=%v, error=%v", j.Name(), duration, err)
				} else {
					log.InfoContext(ctx, "任务执行成功: %s, duration=%v", j.Name(), duration)
				}

				return err
			},
		}
	}
}

// WithHooks 钩子装饰器 - 添加前置和后置回调
func WithHooks(before, after func(ctx context.Context, jobName string) error) JobWrapper {
	return func(j job.Job) job.Job {
		return &wrappedJob{
			name:        j.Name(),
			description: j.Description(),
			execute: func(ctx context.Context) error {
				// 执行前置钩子
				if before != nil {
					if err := before(ctx, j.Name()); err != nil {
						return fmt.Errorf("前置钩子失败: %w", err)
					}
				}

				// 执行任务
				err := j.Execute(ctx)

				// 执行后置钩子
				if after != nil {
					if afterErr := after(ctx, j.Name()); afterErr != nil {
						if err != nil {
							return fmt.Errorf("%w: 任务失败: %v; 后置钩子: %w", ErrTaskAndPostHookFailed, err, afterErr)
						}
						return fmt.Errorf("%w: %w", ErrPostHookExecutionFailed, afterErr)
					}
				}

				return err
			},
		}
	}
}

// WithMetrics 指标装饰器 - 收集任务执行指标
func WithMetrics(metrics *breaker.MetricsCollector) JobWrapper {
	return func(j job.Job) job.Job {
		return &wrappedJob{
			name:        j.Name(),
			description: j.Description(),
			execute: func(ctx context.Context) error {
				start := time.Now()
				metrics.RecordStart(j.Name())

				err := j.Execute(ctx)

				duration := time.Since(start)
				if err != nil {
					metrics.RecordFailure(j.Name(), duration)
				} else {
					metrics.RecordSuccess(j.Name(), duration)
				}

				return err
			},
		}
	}
}

type Logger = logger.Logger
