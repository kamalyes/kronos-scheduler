/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-23 18:00:00
 * @FilePath: \kronos-scheduler\pubsub\interface.go
 * @Description: PubSub接口定义
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package pubsub

import (
	"context"
	"time"
)

// PubSub 发布订阅接口
type PubSub interface {
	// PublishConfigUpdate 发布配置更新事件
	PublishConfigUpdate(ctx context.Context, jobName string) error

	// PublishCronSpecUpdate 发布CronSpec更新事件
	PublishCronSpecUpdate(ctx context.Context, jobName, newCronSpec string) error

	// PublishManualExecute 发布手动执行事件
	PublishManualExecute(ctx context.Context, jobName string) error

	// PublishScheduledExecute 发布定时执行事件
	PublishScheduledExecute(ctx context.Context, jobName string, executeTime time.Time, repeatCount int) error

	// PublishJobDeleted 发布任务删除事件
	PublishJobDeleted(ctx context.Context, jobName string) error

	// SubscribeConfigUpdate 订阅配置更新事件
	SubscribeConfigUpdate(ctx context.Context, handler func(jobName string)) error

	// SubscribeCronSpecUpdate 订阅CronSpec更新事件
	SubscribeCronSpecUpdate(ctx context.Context, handler func(jobName, cronSpec string)) error

	// SubscribeManualExecute 订阅手动执行事件
	SubscribeManualExecute(ctx context.Context, handler func(jobName string)) error

	// SubscribeScheduledExecute 订阅定时执行事件
	SubscribeScheduledExecute(ctx context.Context, handler func(jobName string, executeTime time.Time, repeatCount int)) error

	// SubscribeJobDeleted 订阅任务删除事件
	SubscribeJobDeleted(ctx context.Context, handler func(jobName string)) error

	// Close 关闭PubSub
	Close() error
}
