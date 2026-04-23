/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 19:19:16
 * @FilePath: \kronos-scheduler\repository\interface.go
 * @Description: 仓储接口定义
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package repository

import (
	"context"
	"time"

	"github.com/kamalyes/kronos-scheduler/models"
)

// Error messages
const (
	// ErrJobConfigNotFound Job配置未找到错误消息
	ErrJobConfigNotFound = "job config not found: %s"
)

// JobConfigRepository Job配置仓储接口
type JobConfigRepository interface {
	// GetByJobName 根据Job名称获取配置
	GetByJobName(ctx context.Context, jobName string) (*models.JobConfigModel, error)

	// Create 创建配置
	Create(ctx context.Context, config *models.JobConfigModel) error

	// Update 更新配置
	Update(ctx context.Context, config *models.JobConfigModel) error

	// EnsureConfigExists 确保配置存在(不存在则创建)
	EnsureConfigExists(ctx context.Context, config *models.JobConfigModel) (*models.JobConfigModel, error)

	// UpdateCronSpec 更新Cron表达式
	UpdateCronSpec(ctx context.Context, jobName, cronSpec string) error

	// UpdateJobExecutionTime 更新执行时间
	UpdateJobExecutionTime(ctx context.Context, jobName string, lastExec, nextExec time.Time) error

	// ListAll 列出所有配置
	ListAll(ctx context.Context) ([]*models.JobConfigModel, error)

	// Delete 删除配置
	Delete(ctx context.Context, jobName string) error
}

// CacheRepository 缓存仓储接口
type CacheRepository interface {
	// Get 获取缓存
	Get(ctx context.Context, key string) (interface{}, error)

	// Set 设置缓存
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error

	// Delete 删除缓存
	Delete(ctx context.Context, keys ...string) error

	// InvalidateJobConfig 使Job配置缓存失效
	InvalidateJobConfig(ctx context.Context, jobName string) error
}

// IExecutionSnapshotRepository 快照仓库接口
type IExecutionSnapshotRepository interface {
	Save(ctx context.Context, snapshot *models.ExecutionSnapshotModel) error
	Get(ctx context.Context, traceID string) (*models.ExecutionSnapshotModel, error)
	List(ctx context.Context, filter SnapshotFilter) ([]*models.ExecutionSnapshotModel, error)
	Delete(ctx context.Context, traceID string) error
	DeleteExpired(ctx context.Context, before time.Time) (int64, error)
	GetStatistics(ctx context.Context, jobID string, from, to time.Time) (*SnapshotStatistics, error)
	Count(ctx context.Context) int64
	Clear(ctx context.Context) error
}
