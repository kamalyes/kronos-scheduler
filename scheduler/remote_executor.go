/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-25 17:20:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 17:17:55
 * @FilePath: \kronos-scheduler\scheduler\remote_executor.go
 * @Description: 远程执行器 - 订阅 Redis 消息执行任务
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kamalyes/go-config/pkg/jobs"
	"github.com/kamalyes/kronos-scheduler/models"
	"github.com/redis/go-redis/v9"
)

// RemoteExecutor 远程执行器（CronScheduler 的一部分）
type RemoteExecutor struct {
	scheduler *CronScheduler
	redis     redis.UniversalClient
	pubsub    *redis.PubSub
	stopChan  chan struct{}
}

// newRemoteExecutor 创建远程执行器
func newRemoteExecutor(scheduler *CronScheduler, redisClient redis.UniversalClient) *RemoteExecutor {
	return &RemoteExecutor{
		scheduler: scheduler,
		redis:     redisClient,
		stopChan:  make(chan struct{}),
	}
}

// Start 启动远程执行监听
func (e *RemoteExecutor) Start(ctx context.Context) error {
	if e.redis == nil {
		e.scheduler.logger.Warn("⚠️ Redis 客户端未配置，跳过远程执行功能")
		return nil
	}

	// 订阅全局执行通道
	globalChannel := RedisChannelExecuteGlobal
	// 订阅节点专属通道
	nodeChannel := fmt.Sprintf(RedisChannelExecuteNode, e.scheduler.nodeID)
	// 订阅配置更新通道
	configChannel := RedisChannelConfigUpdate

	e.pubsub = e.redis.Subscribe(ctx, globalChannel, nodeChannel, configChannel)

	e.scheduler.logger.Infof("🚀 远程执行器启动: NodeID=%s", e.scheduler.nodeID)
	e.scheduler.logger.Infof("📡 订阅通道: %s, %s, %s", globalChannel, nodeChannel, configChannel)

	// 启动消息监听
	go e.listenMessages(ctx)

	return nil
}

// Stop 停止远程执行监听
func (e *RemoteExecutor) Stop() error {
	if e.pubsub != nil {
		e.pubsub.Close()
	}
	close(e.stopChan)
	return nil
}

// listenMessages 监听消息
func (e *RemoteExecutor) listenMessages(ctx context.Context) {
	if e.pubsub == nil {
		return
	}

	ch := e.pubsub.Channel()

	for {
		select {
		case msg := <-ch:
			e.handleMessage(ctx, msg)
		case <-e.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

// handleMessage 处理消息
func (e *RemoteExecutor) handleMessage(ctx context.Context, msg *redis.Message) {
	e.scheduler.logger.DebugContext(ctx, "📨 收到消息: Channel=%s", msg.Channel)

	switch msg.Channel {
	case RedisChannelConfigUpdate:
		e.handleConfigUpdate(ctx, msg.Payload)
	default:
		e.handleExecuteRequest(ctx, msg.Payload)
	}
}

// handleExecuteRequest 处理执行请求
func (e *RemoteExecutor) handleExecuteRequest(ctx context.Context, payload string) {
	var req ExecuteRequest
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		e.scheduler.logger.Errorf("❌ 解析执行请求失败: %v", err)
		return
	}

	// 检查是否需要本节点执行
	if !e.shouldExecute(&req) {
		return
	}

	e.scheduler.logger.Infof("🎯 执行远程任务: Job=%s, MessageID=%s, Strategy=%s",
		req.JobName, req.MessageID, req.RouteStrategy)

	// 根据消息类型处理
	switch req.MessageType {
	case MessageTypeExecute:
		e.executeJob(ctx, &req)
	case MessageTypeScheduledExecute:
		e.executeScheduledJob(ctx, &req)
	case MessageTypeCancelExecute:
		e.scheduler.logger.Infof("🚫 取消任务: Job=%s", req.JobName)
	}
}

// shouldExecute 判断是否应该执行
func (e *RemoteExecutor) shouldExecute(req *ExecuteRequest) bool {
	switch req.RouteStrategy {
	case RouteStrategyBroadcast:
		// 广播：所有节点执行
		return true
	case RouteStrategySpecific:
		// 指定节点
		for _, nodeID := range req.TargetNodes {
			if nodeID == e.scheduler.nodeID {
				return true
			}
		}
		return false
	default:
		// 其他策略由调度中心选择节点，发送到节点专属通道
		return true
	}
}

// executeJob 执行任务
func (e *RemoteExecutor) executeJob(ctx context.Context, req *ExecuteRequest) {
	startTime := time.Now()

	// 获取任务
	entry, ok := e.scheduler.GetEntry(req.JobName)
	if !ok {
		e.sendResponse(ctx, req, false, nil, fmt.Sprintf("任务不存在: %s", req.JobName), startTime)
		return
	}

	// 设置超时
	execCtx := ctx
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, time.Duration(req.Timeout)*time.Second)
		defer cancel()
	}

	// 执行任务
	err := e.scheduler.executeWithProtector(execCtx, entry)

	success := err == nil
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}

	// 发送响应
	e.sendResponse(ctx, req, success, nil, errMsg, startTime)
}

// executeScheduledJob 执行定时任务
func (e *RemoteExecutor) executeScheduledJob(ctx context.Context, req *ExecuteRequest) {
	// 等待到执行时间
	if req.ExecuteTime != nil && req.ExecuteTime.After(time.Now()) {
		waitDuration := time.Until(*req.ExecuteTime)
		e.scheduler.logger.Infof("⏰ 任务 %s 将在 %v 后执行", req.JobName, waitDuration)

		timer := time.NewTimer(waitDuration)
		select {
		case <-timer.C:
			// 时间到，执行任务
		case <-ctx.Done():
			timer.Stop()
			return
		case <-e.stopChan:
			timer.Stop()
			return
		}
	}

	// 执行指定次数
	repeatCount := req.RepeatCount
	if repeatCount <= 0 {
		repeatCount = 1
	}

	for i := 0; i < repeatCount; i++ {
		e.executeJob(ctx, req)
		if i < repeatCount-1 {
			time.Sleep(1 * time.Second)
		}
	}
}

// sendResponse 发送响应
func (e *RemoteExecutor) sendResponse(ctx context.Context, req *ExecuteRequest, success bool, result interface{}, errMsg string, startTime time.Time) {
	resp := ExecuteResponse{
		MessageID:    req.MessageID,
		JobName:      req.JobName,
		NodeID:       e.scheduler.nodeID,
		Success:      success,
		Result:       result,
		Error:        errMsg,
		StartTime:    startTime,
		EndTime:      time.Now(),
		Duration:     time.Since(startTime).Milliseconds(),
		TraceID:      req.TraceID,
		ResponseTime: time.Now(),
	}

	data, _ := json.Marshal(resp)

	if err := e.redis.Publish(ctx, RedisChannelResponse, string(data)).Err(); err != nil {
		e.scheduler.logger.Errorf("❌ 发送响应失败: %v", err)
	} else {
		status := "✅"
		if !success {
			status = "❌"
		}
		e.scheduler.logger.Infof("%s 任务完成: Job=%s, Duration=%dms", status, req.JobName, resp.Duration)
	}
}

// handleConfigUpdate 处理配置更新
func (e *RemoteExecutor) handleConfigUpdate(ctx context.Context, payload string) {
	var req ConfigUpdateRequest
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		e.scheduler.logger.Errorf("❌ 解析配置更新请求失败: %v", err)
		return
	}

	// 检查是否目标节点
	if len(req.TargetNodes) > 0 {
		isTarget := false
		for _, nodeID := range req.TargetNodes {
			if nodeID == e.scheduler.nodeID {
				isTarget = true
				break
			}
		}
		if !isTarget {
			return
		}
	}

	e.scheduler.logger.Infof("🔄 收到配置更新: Job=%s, Operation=%s", req.JobName, req.Operation)

	var err error
	switch req.Operation {
	case OperationAdd, OperationUpdate:
		// 如果只修改 CronSpec，使用专门的方法
		if req.CronSpec != "" && req.Config.CronSpec == "" {
			err = e.handleCronSpecUpdate(ctx, req.JobName, req.CronSpec)
		} else {
			err = e.handleConfigAddOrUpdate(ctx, req.JobName, req.Config)
		}
	case OperationDelete:
		err = e.handleConfigDelete(ctx, req.JobName)
	case OperationReload:
		err = e.handleConfigReload(ctx, req.JobName)
	}

	// 发送响应
	success := err == nil
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}

	resp := ConfigUpdateResponse{
		MessageID:    req.MessageID,
		JobName:      req.JobName,
		NodeID:       e.scheduler.nodeID,
		Success:      success,
		Error:        errMsg,
		ResponseTime: time.Now(),
	}

	data, _ := json.Marshal(resp)
	e.redis.Publish(ctx, RedisChannelConfigResponse, string(data))
}

// handleConfigAddOrUpdate 处理配置添加或更新
func (e *RemoteExecutor) handleConfigAddOrUpdate(ctx context.Context, jobName string, config jobs.TaskCfg) error {
	// 1. 保存配置到数据库
	if e.scheduler.jobRepo != nil {
		dbConfig := models.FromTaskCfg(jobName, &config)
		if _, err := e.scheduler.jobRepo.EnsureConfigExists(ctx, dbConfig); err != nil {
			e.scheduler.logger.Errorf("❌ 保存配置到数据库失败: %v", err)
			return err
		}
	}

	// 2. 更新缓存
	if e.scheduler.schedulerCache != nil {
		if err := e.scheduler.schedulerCache.SetTaskConfig(ctx, jobName, config); err != nil {
			e.scheduler.logger.Warnf("⚠️ 更新缓存失败: %v", err)
		}
	}

	// 3. 重新注册任务（如果已存在 Job 实例）
	entry, exists := e.scheduler.GetEntry(jobName)
	if exists {
		// 注销旧任务
		if err := e.scheduler.UnregisterJob(jobName); err != nil {
			e.scheduler.logger.Warnf("⚠️ 注销旧任务失败: %v", err)
		}

		// 重新注册
		if err := e.scheduler.RegisterJob(entry.Job, config); err != nil {
			e.scheduler.logger.Errorf("❌ 重新注册任务失败: %v", err)
			return err
		}

		e.scheduler.logger.Infof("✅ 任务配置已更新: %s", jobName)
	}

	return nil
}

// handleConfigDelete 处理配置删除
func (e *RemoteExecutor) handleConfigDelete(ctx context.Context, jobName string) error {
	// 1. 注销任务
	if err := e.scheduler.UnregisterJob(jobName); err != nil {
		e.scheduler.logger.Warnf("⚠️ 注销任务失败: %v", err)
	}

	// 2. 删除数据库配置
	if e.scheduler.jobRepo != nil {
		if err := e.scheduler.jobRepo.Delete(ctx, jobName); err != nil {
			e.scheduler.logger.Errorf("❌ 删除数据库配置失败: %v", err)
			return err
		}
	}

	// 3. 删除缓存
	if e.scheduler.schedulerCache != nil {
		if err := e.scheduler.schedulerCache.InvalidateTaskConfig(ctx, jobName); err != nil {
			e.scheduler.logger.Warnf("⚠️ 删除缓存失败: %v", err)
		}
	}

	e.scheduler.logger.Infof("✅ 任务已删除: %s", jobName)
	return nil
}

// handleConfigReload 处理配置重载
func (e *RemoteExecutor) handleConfigReload(ctx context.Context, jobName string) error {
	// 1. 从数据库加载最新配置
	if e.scheduler.jobRepo == nil {
		return ErrJobRepoNotConfigured
	}

	dbConfig, err := e.scheduler.jobRepo.GetByJobName(ctx, jobName)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrLoadConfigFromDB, err)
	}

	taskCfg, err := dbConfig.ToTaskCfg()
	if err != nil || taskCfg == nil {
		return fmt.Errorf("%w: %w", ErrConvertConfig, err)
	}

	// 2. 应用配置
	return e.handleConfigAddOrUpdate(ctx, jobName, *taskCfg)
}

// handleCronSpecUpdate 处理单独的 Cron 表达式更新
func (e *RemoteExecutor) handleCronSpecUpdate(ctx context.Context, jobName, cronSpec string) error {
	// 1. 验证新的 Cron 表达式
	if _, err := e.scheduler.parser.Parse(cronSpec); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCronExpression, err)
	}

	// 2. 获取现有配置
	var config jobs.TaskCfg
	if e.scheduler.jobRepo != nil {
		dbConfig, err := e.scheduler.jobRepo.GetByJobName(ctx, jobName)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrLoadConfigFromDB, err)
		}

		taskCfg, err := dbConfig.ToTaskCfg()
		if err != nil || taskCfg == nil {
			return fmt.Errorf("%w: %w", ErrConvertConfig, err)
		}
		config = *taskCfg
	} else {
		// 从内存获取
		entry, ok := e.scheduler.GetEntry(jobName)
		if !ok {
			return fmt.Errorf("%w: %s", ErrJobNotFound, jobName)
		}
		config = entry.Config
	}

	// 3. 更新 CronSpec
	config.CronSpec = cronSpec

	// 4. 保存到数据库
	if e.scheduler.jobRepo != nil {
		if err := e.scheduler.jobRepo.UpdateCronSpec(ctx, jobName, cronSpec); err != nil {
			return fmt.Errorf("%w: %w", ErrUpdateDatabase, err)
		}
	}

	// 5. 更新缓存
	if e.scheduler.schedulerCache != nil {
		if err := e.scheduler.schedulerCache.SetTaskConfig(ctx, jobName, config); err != nil {
			e.scheduler.logger.Warnf("⚠️ 更新缓存失败: %v", err)
		}
	}

	// 6. 重新注册任务
	entry, exists := e.scheduler.GetEntry(jobName)
	if exists {
		// 注销旧任务
		if err := e.scheduler.UnregisterJob(jobName); err != nil {
			e.scheduler.logger.Warnf("⚠️ 注销旧任务失败: %v", err)
		}

		// 重新注册
		if err := e.scheduler.RegisterJob(entry.Job, config); err != nil {
			return fmt.Errorf("%w: %w", ErrReregisterJob, err)
		}

		e.scheduler.logger.Infof("✅ Cron表达式已更新: %s -> %s", jobName, cronSpec)
	}

	return nil
}
