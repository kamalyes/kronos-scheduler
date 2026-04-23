/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 15:05:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 15:15:27
 * @FilePath: \kronos-scheduler\scheduler\protector.go
 * @Description: 任务保护器(熔断器保护)
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/kamalyes/go-toolbox/pkg/breaker"
)

// JobProtector 任务保护器(仅熔断器)
type JobProtector struct {
	name    string
	circuit *breaker.Circuit
	metrics *breaker.MetricsCollector
}

// NewJobProtector 创建任务保护器(使用默认配置)
func NewJobProtector(name string) *JobProtector {
	return &JobProtector{
		name: name,
		circuit: breaker.New(name, breaker.Config{
			MaxFailures:       5,
			ResetTimeout:      30 * time.Second,
			HalfOpenSuccesses: 2,
		}),
	}
}

// WithCircuitBreaker 设置熔断器配置
func (p *JobProtector) WithCircuitBreaker(config breaker.Config) *JobProtector {
	p.circuit = breaker.New(p.name, config)
	return p
}

// WithMetrics 设置指标收集器
func (p *JobProtector) WithMetrics(metrics *breaker.MetricsCollector) *JobProtector {
	p.metrics = metrics
	return p
}

// Execute 执行带保护的操作
func (p *JobProtector) Execute(ctx context.Context, jobName string, fn func() error) error {
	// 检查熔断器是否允许请求
	if !p.circuit.AllowRequest() {
		return breaker.ErrOpen
	}

	// 记录任务开始
	if p.metrics != nil {
		p.metrics.RecordStart(jobName)
	}

	// 记录开始时间
	start := time.Now()

	// 执行任务
	err := fn()

	// 记录执行时间
	duration := time.Since(start)

	// 更新熔断器状态
	if err != nil {
		p.circuit.RecordFailure()
	} else {
		p.circuit.RecordSuccess()
	}

	// 记录指标
	if p.metrics != nil {
		if err != nil {
			p.metrics.RecordFailure(jobName, duration)
		} else {
			p.metrics.RecordSuccess(jobName, duration)
		}
	}

	return err
}

// GetStats 获取统计信息
func (p *JobProtector) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	// 熔断器统计
	circuitStats := p.circuit.Stats()
	stats["circuit"] = map[string]interface{}{
		"name":     circuitStats.Name,
		"state":    circuitStats.State,
		"failures": circuitStats.Failures,
	}

	// 指标统计
	if p.metrics != nil {
		global := p.metrics.GetGlobalMetrics()
		stats["metrics"] = map[string]interface{}{
			"totalExecutions": global.TotalExecutions,
			"totalSuccess":    global.TotalSuccess,
			"totalFailure":    global.TotalFailure,
		}
	}

	return stats
}

// GetCircuit 获取熔断器
func (p *JobProtector) GetCircuit() *breaker.Circuit {
	return p.circuit
}

// GetMetrics 获取指标收集器
func (p *JobProtector) GetMetrics() *breaker.MetricsCollector {
	return p.metrics
}

// String 字符串表示
func (p *JobProtector) String() string {
	stats := p.GetStats()
	return fmt.Sprintf("JobProtector{circuit: %v}", stats["circuit"])
}
