/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-23 18:36:20
 * @FilePath: \kronos-scheduler\job\interface.go
 * @Description: Job接口定义 - 业务方需要实现此接口
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package job

import "context"

// Job 任务接口 - 业务方需要实现此接口
type Job interface {
	// Name 返回任务名称(唯一标识)
	Name() string

	// Execute 执行任务逻辑
	Execute(ctx context.Context) error

	// Description 返回任务描述(可选)
	Description() string
}

// ConfigurableJob 可配置的任务(支持动态配置加载)
type ConfigurableJob interface {
	Job

	// LoadConfig 加载配置
	LoadConfig(ctx context.Context, config interface{}) error
}

// RetryableJob 支持重试的任务
type RetryableJob interface {
	Job

	// ShouldRetry 判断是否应该重试
	ShouldRetry(err error) bool
}
