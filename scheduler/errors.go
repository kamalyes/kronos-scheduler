/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 17:54:55
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 10:33:53
 * @FilePath: \kronos-scheduler\scheduler\errors.go
 * @Description:
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */
package scheduler

import "errors"

// 基础错误
var (
	// ErrJobNotFound Job不存在
	ErrJobNotFound = errors.New("job does not exist")
	// ErrSchedulerNotRunning 调度器未运行
	ErrSchedulerNotRunning = errors.New("scheduler is not running")
	// ErrSchedulerAlreadyRunning 调度器已在运行中
	ErrSchedulerAlreadyRunning = errors.New("scheduler is already running")
	// ErrInvalidCronSpec 无效的Cron表达式
	ErrInvalidCronSpec = errors.New("invalid Cron expression")
)

// 任务相关错误
var (
	// ErrJobNil 任务不能为nil
	ErrJobNil = errors.New("job cannot be nil")
	// ErrJobNameEmpty 任务名称不能为空
	ErrJobNameEmpty = errors.New("job name cannot be empty")
	// ErrJobAlreadyRunning 任务正在运行
	ErrJobAlreadyRunning = errors.New("job is already running, skipping this execution")
	// ErrJobAddFailed 任务添加失败
	ErrJobAddFailed = errors.New("failed to add job")
	// ErrDependencyNotFound 依赖任务不存在
	ErrDependencyNotFound = errors.New("dependency job not found")
)

// 注册相关错误
var (
	// ErrJobManagerDisabled Job管理器被禁用
	ErrJobManagerDisabled = errors.New("job manager is disabled in config")
	// ErrNoJobsRegistered 没有注册任何任务
	ErrNoJobsRegistered = errors.New("no jobs registered")
	// ErrTaskNotInConfig 任务在配置中不存在
	ErrTaskNotInConfig = errors.New("task does not exist in the configuration")
	// ErrTaskNotEnabled 任务未启用
	ErrTaskNotEnabled = errors.New("task is not enabled")
	// ErrNotCronScheduler scheduler不是CronScheduler实例
	ErrNotCronScheduler = errors.New("scheduler is not a CronScheduler instance")
	// ErrJobRepoNotConfigured jobRepo未配置
	ErrJobRepoNotConfigured = errors.New("jobRepo is not configured")
)

// 解析器相关错误
var (
	// ErrCronSpecEmpty cron表达式不能为空
	ErrCronSpecEmpty = errors.New("cron expression cannot be empty")
	// ErrTimezoneFormat 时区格式错误
	ErrTimezoneFormat = errors.New("timezone format error")
	// ErrInvalidTimezone 无效的时区
	ErrInvalidTimezone = errors.New("invalid timezone")
	// ErrDescriptorNotSupported 解析器不支持描述符
	ErrDescriptorNotSupported = errors.New("descriptor not supported by the parser")
	// ErrMultipleOptionalFields 不能配置多个可选字段
	ErrMultipleOptionalFields = errors.New("cannot configure multiple optional fields")
	// ErrUnknownOptionalField 未知的可选字段
	ErrUnknownOptionalField = errors.New("unknown optional field")
	// ErrTooManyHyphens 连字符过多
	ErrTooManyHyphens = errors.New("too many hyphens")
	// ErrTooManySlashes 斜杠过多
	ErrTooManySlashes = errors.New("too many slashes")
	// ErrStepZero 步长不能为0
	ErrStepZero = errors.New("step cannot be zero")
	// ErrNegativeNumber 负数无效
	ErrNegativeNumber = errors.New("negative number is invalid")
	// ErrIntervalTooShort 间隔时间过短
	ErrIntervalTooShort = errors.New("interval time is too short")
	// ErrUnrecognizedDescriptor 无法识别的描述符
	ErrUnrecognizedDescriptor = errors.New("unrecognized descriptor")
	// ErrParseInterval 无法解析间隔时间
	ErrParseInterval = errors.New("unable to parse interval")
)

// 工作池相关错误
var (
	// ErrWorkerPoolAlreadyRunning 工作池已在运行中
	ErrWorkerPoolAlreadyRunning = errors.New("worker pool is already running")
	// ErrWorkerPoolNotRunning 工作池未运行
	ErrWorkerPoolNotRunning = errors.New("worker pool is not running")
	// ErrWorkerPoolPaused 工作池已暂停
	ErrWorkerPoolPaused = errors.New("worker pool is paused")
	// ErrWorkerPoolShuttingDown 工作池正在关闭
	ErrWorkerPoolShuttingDown = errors.New("worker pool is shutting down")
	// ErrWorkerPoolStopTimeout 工作池停止超时
	ErrWorkerPoolStopTimeout = errors.New("worker pool stop timeout")
	// ErrTaskQueueFull 任务队列已满
	ErrTaskQueueFull = errors.New("task queue is full")
	// ErrDecreaseWorkerNeedRestart 减少工作者数量需要重启工作池
	ErrDecreaseWorkerNeedRestart = errors.New("decreasing worker count requires restarting the worker pool")
)

// 节点注册相关错误
var (
	// ErrNodeRegisterFailed 节点注册失败
	ErrNodeRegisterFailed = errors.New("node registration failed")
	// ErrNodeUnregisterFailed 注销节点失败
	ErrNodeUnregisterFailed = errors.New("node unregistration failed")
	// ErrNodeInfoSerializeFailed 序列化节点信息失败
	ErrNodeInfoSerializeFailed = errors.New("failed to serialize node info")
	// ErrNodeInfoUpdateFailed 更新节点信息失败
	ErrNodeInfoUpdateFailed = errors.New("failed to update node info")
	// ErrGetNodeListFailed 获取节点列表失败
	ErrGetNodeListFailed = errors.New("failed to get node list")
)

// 任务配置相关错误
var (
	// ErrSyncConfigToDBFailed 同步配置到数据库失败
	ErrSyncConfigToDBFailed = errors.New("failed to sync config to database")
	// ErrUpdateDBConfigFailed 更新数据库配置失败
	ErrUpdateDBConfigFailed = errors.New("failed to update database config")
	// ErrRemoveOldJobFailed 移除旧任务失败
	ErrRemoveOldJobFailed = errors.New("failed to remove old job")
	// ErrRegisterNewJobFailed 注册新任务失败
	ErrRegisterNewJobFailed = errors.New("failed to register new job")
	// ErrSetDependencyFailed 设置任务依赖失败
	ErrSetDependencyFailed = errors.New("failed to set job dependency")
	// ErrParseCronFailed 解析cron表达式失败
	ErrParseCronFailed = errors.New("failed to parse cron expression")
	// ErrJobsConfigNil 任务配置不能为nil
	ErrJobsConfigNil = errors.New("jobs config cannot be nil")
	// ErrInvalidJobsConfig 无效的任务配置
	ErrInvalidJobsConfig = errors.New("invalid jobs config")
	// ErrProtectorNil 保护器不能为空
	ErrProtectorNil = errors.New("protector cannot be nil")
	// ErrLoadConfigFromDB 从数据库加载配置失败
	ErrLoadConfigFromDB = errors.New("failed to load config from database")
	// ErrConvertConfig 转换配置失败
	ErrConvertConfig = errors.New("failed to convert config")
	// ErrInvalidCronExpression 无效的Cron表达式
	ErrInvalidCronExpression = errors.New("invalid cron expression")
	// ErrUpdateDatabase 更新数据库失败
	ErrUpdateDatabase = errors.New("failed to update database")
	// ErrReregisterJob 重新注册任务失败
	ErrReregisterJob = errors.New("failed to reregister job")
	// ErrUnsupportedLoadStrategy 不支持的配置加载策略
	ErrUnsupportedLoadStrategy = errors.New("unsupported load strategy")
	// ErrRegisterJobFailed 注册任务失败
	ErrRegisterJobFailed = errors.New("failed to register job")
	// ErrSyncTaskConfigFailed 同步任务配置到数据库失败
	ErrSyncTaskConfigFailed = errors.New("failed to sync task config to database")
)

// 装饰器相关错误
var (
	// ErrPreHookFailed 前置钩子失败
	ErrPreHookFailed = errors.New("pre-hook failed")
	// ErrPostHookFailed 后置钩子失败
	ErrPostHookFailed = errors.New("post-hook failed")
	// ErrRetryExhausted 重试次数耗尽
	ErrRetryExhausted = errors.New("retry exhausted")
)

// 分布式相关错误
var (
	// ErrLockAcquireFailed 获取锁失败
	ErrLockAcquireFailed = errors.New("failed to acquire lock")
	// ErrLockReleaseFailed 释放锁失败
	ErrLockReleaseFailed = errors.New("failed to release lock")
	// ErrLockTimeout 锁超时
	ErrLockTimeout = errors.New("lock timeout")
)

// 执行相关错误
var (
	// ErrExecutionTimeout 执行超时
	ErrExecutionTimeout = errors.New("execution timeout")
	// ErrExecutionCancelled 执行取消
	ErrExecutionCancelled = errors.New("execution cancelled")
	// ErrExecutionPanic 执行发生崩溃
	ErrExecutionPanic = errors.New("execution panicked")
	// ErrTaskPanic 任务panic
	ErrTaskPanic = errors.New("task panicked")
	// ErrTaskExecutionTimeout 任务执行超时
	ErrTaskExecutionTimeout = errors.New("task execution timeout")
	// ErrPreHookExecutionFailed 前置钩子执行失败
	ErrPreHookExecutionFailed = errors.New("pre-hook execution failed")
	// ErrPostHookExecutionFailed 后置钩子执行失败
	ErrPostHookExecutionFailed = errors.New("post-hook execution failed")
	// ErrTaskAndPostHookFailed 任务和后置钩子都失败
	ErrTaskAndPostHookFailed = errors.New("task failed and post-hook failed")
)

// 缓存相关错误
var (
	// ErrCacheNotFound 缓存未找到
	ErrCacheNotFound = errors.New("cache not found")
	// ErrCacheExpired 缓存已过期
	ErrCacheExpired = errors.New("cache expired")
	// ErrCacheSerializeFailed 缓存序列化失败
	ErrCacheSerializeFailed = errors.New("cache serialization failed")
)
