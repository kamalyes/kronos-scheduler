/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 21:30:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 11:15:26
 * @FilePath: \kronos-scheduler\scheduler\cron_schedule.go
 * @Description: Cron调度规范 - 使用 go-toolbox/pkg/cron 的实现
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"time"

	"github.com/kamalyes/go-toolbox/pkg/cron"
)

// Schedule 调度规范接口(兼容 cron.CronSchedule)
type Schedule interface {
	// Next 返回下次激活时间，晚于给定时间(支持纳秒精度)
	Next(t time.Time) time.Time
}

// SpecSchedule Cron 规范调度(基于位集合存储，高性能)
// 类型别名，直接使用 go-toolbox 的实现
type SpecSchedule = cron.CronSpecSchedule

// EverySchedule 固定延迟调度(间隔执行)
type EverySchedule = cron.CronEverySchedule

// Every 创建一个固定间隔的调度(支持纳秒精度)
func Every(duration time.Duration) *EverySchedule {
	if duration < time.Nanosecond {
		duration = time.Nanosecond
	}
	return &EverySchedule{
		Duration: duration,
	}
}
