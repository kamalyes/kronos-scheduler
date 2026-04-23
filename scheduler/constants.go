/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 17:00:16
 * @FilePath: \kronos-scheduler\scheduler\constants.go
 * @Description: Scheduler 常量定义 - 包含配置和预定义 Cron 表达式
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"time"
)

const (
	// Redis Key 前缀
	RedisKeySchedulerNodes     = "scheduler:nodes"        // 节点 Hash 键
	RedisKeyNodeExpirePrefix   = "scheduler:node:expire:" // 节点过期标记键前缀
	RedisKeyJobLockPrefix      = "scheduler:lock:"        // 任务锁键前缀
	RedisKeyJobExecutionPrefix = "scheduler:execution:"   // 任务执行记录键前缀

	// Redis 键模板（配合 fmt.Sprintf 使用）
	RedisKeyTemplateExecLock  = "exec:%s:%d" // 执行锁键模板：exec:{jobName}:{timestamp}
	RedisKeyTemplateManualJob = "manual-%d"  // 手动任务ID模板：manual-{timestamp}

	// Redis 执行通道
	RedisChannelExecuteGlobal = "scheduler:execute:global"  // 全局执行通道
	RedisChannelExecuteNode   = "scheduler:execute:node:%s" // 节点执行通道模板
	RedisChannelResponse      = "scheduler:response"        // 执行响应通道

	// Redis 配置通道
	RedisChannelConfigUpdate   = "scheduler:config:update"   // 配置更新通道
	RedisChannelConfigResponse = "scheduler:config:response" // 配置响应通道

	// Redis 节点通道
	RedisChannelNodeStatus = "scheduler:node:status" // 节点状态通道
)

// 默认配置常量
const (
	DefaultWorkerPoolSize   = 10               // 默认工作池大小
	DefaultTaskQueueSize    = 100              // 默认任务队列大小
	DefaultMaxRetries       = 3                // 默认最大重试次数
	DefaultRetryDelay       = 5 * time.Second  // 默认重试延迟
	DefaultExecutionTimeout = 30 * time.Minute // 默认执行超时
	DefaultCooldownPeriod   = 1 * time.Second  // 默认冷却期
	DefaultMaxConcurrent    = 5                // 默认最大并发数
)

// 时间常量
const (
	MinIntervalDuration = 1 * time.Second      // 最小间隔时间
	MaxIntervalDuration = 365 * 24 * time.Hour // 最大间隔时间(1年)
)

// 任务状态相关常量
const (
	MaxLogRecordsPerJob = 1000               // 每个任务最大日志记录数
	SnapshotRetention   = 7 * 24 * time.Hour // 快照保留时长(7天)
)

// 调度器相关常量
const (
	DefaultMaxConcurrency = 10          // 默认最大并发数
	DefaultWaitDuration   = time.Minute // 默认等待时间
	DefaultHeapCapacity   = 16          // 默认堆容量

	// 节点心跳相关
	DefaultHeartbeatInterval = 10 * time.Second // 默认心跳间隔
	DefaultNodeTTL           = 30 * time.Second // 默认节点存活时间

	// 任务执行相关
	DefaultJobTimeout      = 5 * time.Minute  // 默认任务超时时间
	DefaultShutdownTimeout = 30 * time.Second // 默认关闭超时时间
	DefaultAcquireLockTTL  = 30 * time.Second // 默认获取锁的TTL

	// 缓存相关
	DefaultCacheExpiration = 5 * time.Minute  // 默认缓存过期时间
	DefaultCachePurgeTime  = 10 * time.Minute // 默认缓存清理时间

	// 队列相关
	DefaultQueueCapacity = 1000 // 默认队列容量
	DefaultBufferSize    = 1    // 默认缓冲区大小
)
