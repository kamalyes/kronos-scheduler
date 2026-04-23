/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-24 10:19:07
 * @FilePath: \kronos-scheduler\scheduler\job_registry.go
 * @Description: Job注册器 - 支持从go-config批量注册Job
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"context"
	"fmt"

	"github.com/kamalyes/go-config/pkg/jobs"
	"github.com/kamalyes/kronos-scheduler/job"
	"github.com/kamalyes/kronos-scheduler/models"
)

// JobRegistry Job注册器 - 基于配置批量注册Job
type JobRegistry struct {
	scheduler Scheduler
	config    *jobs.Jobs
}

// NewJobRegistry 创建Job注册器
func NewJobRegistry(scheduler Scheduler, config *jobs.Jobs) *JobRegistry {
	registry := &JobRegistry{
		scheduler: scheduler,
		config:    config,
	}

	// 自动加载全局配置到调度器
	if config != nil {
		if err := scheduler.LoadFromConfig(config); err != nil {
			// 记录错误但不中断创建
			if cronScheduler, ok := scheduler.(*CronScheduler); ok && cronScheduler.logger != nil {
				cronScheduler.logger.Errorf("加载全局配置失败: %v", err)
			}
		}
	}

	return registry
}

// RegisterJobsFromConfig 从go-config批量注册Job
// jobMap: key为任务名称，value为Job实现
func (r *JobRegistry) RegisterJobsFromConfig(jobMap map[string]job.Job) error {
	if r.config == nil || !r.config.Enabled {
		return ErrJobManagerDisabled
	}

	// 遍历配置中的任务
	registeredCount := 0
	for taskName, taskCfg := range r.config.Tasks {
		// 检查Job实现是否存在
		j, exists := jobMap[taskName]
		if !exists {
			if cronScheduler, ok := r.scheduler.(*CronScheduler); ok && cronScheduler.logger != nil {
				cronScheduler.logger.Warn("⚠️  任务 [%s] 没有对应的Job实现,跳过注册", taskName)
			}
			continue
		}

		// 直接传递 TaskCfg，scheduler 会自动合并全局配置
		if err := r.scheduler.RegisterJob(j, taskCfg); err != nil {
			return fmt.Errorf("注册任务 [%s] 失败: %w", taskName, err)
		}

		registeredCount++
	}

	if registeredCount == 0 {
		return ErrNoJobsRegistered
	}

	return nil
}

// RegisterJob 注册单个Job
func (r *JobRegistry) RegisterJob(taskName string, j job.Job) error {
	taskCfg, exists := r.config.GetTaskConfig(taskName)
	if !exists {
		return fmt.Errorf("%w: %s", ErrTaskNotInConfig, taskName)
	}

	if !taskCfg.Enabled {
		return fmt.Errorf("%w: %s", ErrTaskNotEnabled, taskName)
	}

	// 直接传递 TaskCfg，scheduler 会自动合并全局配置
	return r.scheduler.RegisterJob(j, taskCfg)
}

// EnsureConfigInDatabase 确保配置同步到数据库
func (r *JobRegistry) EnsureConfigInDatabase(ctx context.Context) error {
	cronScheduler, ok := r.scheduler.(*CronScheduler)
	if !ok {
		return ErrNotCronScheduler
	}

	if cronScheduler.jobRepo == nil {
		return ErrJobRepoNotConfigured
	}

	for taskName, taskCfg := range r.config.Tasks {
		// 直接使用 TaskCfg 创建数据库模型
		dbConfig := models.FromTaskCfg(taskName, &taskCfg)

		// 确保数据库配置存在
		_, err := cronScheduler.jobRepo.EnsureConfigExists(ctx, dbConfig)
		if err != nil {
			return fmt.Errorf("同步任务 [%s] 配置到数据库失败: %w", taskName, err)
		}
	}

	return nil
}

// GetConfig 获取配置
func (r *JobRegistry) GetConfig() *jobs.Jobs {
	return r.config
}

// IsEnabled 检查Job管理器是否启用
func (r *JobRegistry) IsEnabled() bool {
	return r.config != nil && r.config.Enabled
}

// GetEnabledTasks 获取所有启用的任务名称
func (r *JobRegistry) GetEnabledTasks() []string {
	if r.config == nil {
		return nil
	}

	tasks := make([]string, 0, len(r.config.Tasks))
	for name, cfg := range r.config.Tasks {
		if cfg.Enabled {
			tasks = append(tasks, name)
		}
	}
	return tasks
}
