/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 13:22:19
 * @FilePath: \kronos-scheduler\scheduler\interface.go
 * @Description: 调度器接口定义
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"context"
	"time"

	"github.com/kamalyes/go-config/pkg/jobs"
	"github.com/kamalyes/go-toolbox/pkg/breaker"
	"github.com/kamalyes/kronos-scheduler/job"
	"github.com/kamalyes/kronos-scheduler/models"
)

// Scheduler 调度器接口
type Scheduler interface {
	// LoadFromConfig 从 go-config 配置加载所有任务
	LoadFromConfig(cfg *jobs.Jobs) error

	// RegisterJob 注册单个Job（使用 TaskCfg）
	RegisterJob(job job.Job, config jobs.TaskCfg) error

	// UnregisterJob 注销Job
	UnregisterJob(jobName string) error

	// Start 启动调度器
	Start() error

	// Stop 停止调度器
	Stop() error

	// IsRunning 是否正在运行
	IsRunning() bool

	// TriggerManual 手动触发执行
	TriggerManual(ctx context.Context, jobName string) error

	// TriggerScheduled 定时触发(XXL-JOB风格)
	TriggerScheduled(ctx context.Context, jobName string, executeTime time.Time, repeatCount int) error

	// UpdateCronSpec 动态更新Cron表达式
	UpdateCronSpec(ctx context.Context, jobName, cronSpec string) error

	// GetJobStatus 获取Job状态
	GetJobStatus(jobName string) (*models.JobStatus, error)

	// ListJobs 列出所有已注册的Job
	ListJobs() []string

	// HealthCheck 健康检查
	HealthCheck() bool

	// GetMetrics 获取指标统计
	GetMetrics() *breaker.MetricsCollector

	// GetJobMetrics 获取指定任务的指标
	GetJobMetrics(jobName string) map[string]interface{}
}
