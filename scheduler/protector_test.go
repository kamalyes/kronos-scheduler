/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 15:05:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 15:15:27
 * @FilePath: \kronos-scheduler\scheduler\protector_test.go
 * @Description: 任务保护器测试
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kamalyes/go-toolbox/pkg/breaker"
	"github.com/stretchr/testify/assert"
)

func TestNewJobProtector(t *testing.T) {
	protector := NewJobProtector("test-job").
		WithCircuitBreaker(breaker.Config{
			MaxFailures:  3,
			ResetTimeout: time.Second,
		})

	assert.NotNil(t, protector)
	assert.NotNil(t, protector.GetCircuit())
}

func TestNewJobProtectorWithMetrics(t *testing.T) {
	metrics := breaker.NewMetricsCollector()
	protector := NewJobProtector("test-job").
		WithCircuitBreaker(breaker.Config{
			MaxFailures: 3,
		}).
		WithMetrics(metrics)

	assert.NotNil(t, protector)
	assert.Equal(t, metrics, protector.GetMetrics())
}

func TestJobProtectorExecuteSuccess(t *testing.T) {
	protector := NewJobProtector("test-job").
		WithCircuitBreaker(breaker.Config{
			MaxFailures: 3,
		})

	ctx := context.Background()
	executed := false

	err := protector.Execute(ctx, "test-job", func() error {
		executed = true
		return nil
	})

	assert.NoError(t, err)
	assert.True(t, executed)
	assert.Equal(t, breaker.StateClosed, protector.GetCircuit().GetState())
}

func TestJobProtectorExecuteFailure(t *testing.T) {
	protector := NewJobProtector("test-job").
		WithCircuitBreaker(breaker.Config{
			MaxFailures: 3,
		})

	ctx := context.Background()
	testErr := errors.New("test error")

	err := protector.Execute(ctx, "test-job", func() error {
		return testErr
	})

	assert.Error(t, err)
	assert.Equal(t, testErr, err)
}

func TestJobProtectorCircuitBreaker(t *testing.T) {
	protector := NewJobProtector("test-job").
		WithCircuitBreaker(breaker.Config{
			MaxFailures:  2,
			ResetTimeout: time.Second,
		})

	ctx := context.Background()
	testErr := errors.New("test error")

	// 触发熔断
	protector.Execute(ctx, "test-job", func() error { return testErr })
	protector.Execute(ctx, "test-job", func() error { return testErr })

	// 熔断器应该打开
	assert.Equal(t, breaker.StateOpen, protector.GetCircuit().GetState())

	// 应该返回熔断错误
	err := protector.Execute(ctx, "test-job", func() error {
		return nil
	})

	assert.Error(t, err)
	assert.Equal(t, breaker.ErrOpen, err)
}

func TestJobProtectorWithMetrics(t *testing.T) {
	metrics := breaker.NewMetricsCollector()
	protector := NewJobProtector("test-job").
		WithCircuitBreaker(breaker.Config{
			MaxFailures: 10,
		}).
		WithMetrics(metrics)

	ctx := context.Background()

	// 成功执行
	protector.Execute(ctx, "test-job", func() error { return nil })

	// 失败执行
	protector.Execute(ctx, "test-job", func() error { return errors.New("error") })

	global := metrics.GetGlobalMetrics()
	assert.Equal(t, int64(2), global.TotalExecutions)
	assert.Equal(t, int64(1), global.TotalSuccess)
	assert.Equal(t, int64(1), global.TotalFailure)
}

func TestJobProtectorGetStats(t *testing.T) {
	metrics := breaker.NewMetricsCollector()
	protector := NewJobProtector("test-job").
		WithCircuitBreaker(breaker.Config{
			MaxFailures: 3,
		}).
		WithMetrics(metrics)

	ctx := context.Background()
	protector.Execute(ctx, "test-job", func() error { return nil })

	stats := protector.GetStats()

	assert.NotNil(t, stats["circuit"])
	assert.NotNil(t, stats["metrics"])

	circuitStats := stats["circuit"].(map[string]interface{})
	assert.Equal(t, "test-job", circuitStats["name"])
	assert.Equal(t, "closed", circuitStats["state"])
}

func TestJobProtectorGetStatsWithoutMetrics(t *testing.T) {
	protector := NewJobProtector("test-job").
		WithCircuitBreaker(breaker.Config{
			MaxFailures: 3,
		})

	stats := protector.GetStats()

	assert.NotNil(t, stats["circuit"])
	assert.Nil(t, stats["metrics"])
}

func TestJobProtectorString(t *testing.T) {
	protector := NewJobProtector("test-job").
		WithCircuitBreaker(breaker.Config{
			MaxFailures: 3,
		})

	str := protector.String()

	assert.Contains(t, str, "JobProtector")
	assert.Contains(t, str, "circuit")
}

func TestJobProtectorConcurrentExecution(t *testing.T) {
	protector := NewJobProtector("test-job").
		WithCircuitBreaker(breaker.Config{
			MaxFailures: 100,
		})

	ctx := context.Background()
	done := make(chan bool)
	workers := 10

	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				protector.Execute(ctx, "test-job", func() error {
					time.Sleep(time.Millisecond)
					return nil
				})
			}
			done <- true
		}()
	}

	for i := 0; i < workers; i++ {
		<-done
	}

	assert.Equal(t, breaker.StateClosed, protector.GetCircuit().GetState())
}

func TestJobProtectorCircuitBreakerWithMetrics(t *testing.T) {
	metrics := breaker.NewMetricsCollector()
	protector := NewJobProtector("test-job").
		WithCircuitBreaker(breaker.Config{
			MaxFailures: 2,
		}).
		WithMetrics(metrics)

	ctx := context.Background()
	testErr := errors.New("test error")

	// 触发熔断
	protector.Execute(ctx, "test-job", func() error { return testErr })
	protector.Execute(ctx, "test-job", func() error { return testErr })

	// 熔断后的请求
	// 熔断后的请求
	err := protector.Execute(ctx, "test-job", func() error { return nil })
	assert.Equal(t, breaker.ErrOpen, err)

	// 检查指标
	global := metrics.GetGlobalMetrics()
	assert.Equal(t, int64(2), global.TotalExecutions)
	assert.Equal(t, int64(0), global.TotalSuccess)
	assert.Equal(t, int64(2), global.TotalFailure)
}

func TestJobProtectorGetCircuit(t *testing.T) {
	protector := NewJobProtector("test-job").
		WithCircuitBreaker(breaker.Config{
			MaxFailures: 3,
		})

	circuit := protector.GetCircuit()

	assert.NotNil(t, circuit)
	assert.Equal(t, breaker.StateClosed, circuit.GetState())
}

func TestJobProtectorGetMetrics(t *testing.T) {
	metrics := breaker.NewMetricsCollector()
	protector := NewJobProtector("test-job").
		WithCircuitBreaker(breaker.Config{
			MaxFailures: 3,
		}).
		WithMetrics(metrics)

	gotMetrics := protector.GetMetrics()

	assert.NotNil(t, gotMetrics)
	assert.Equal(t, metrics, gotMetrics)
}
