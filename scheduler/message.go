/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-25 17:30:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 17:09:06
 * @FilePath: \kronos-scheduler\scheduler\message.go
 * @Description: 远程执行和配置管理消息结构
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"time"

	"github.com/kamalyes/go-config/pkg/jobs"
)

// MessageType 消息类型
type MessageType string

const (
	MessageTypeExecute          MessageType = "execute"           // 立即执行
	MessageTypeScheduledExecute MessageType = "scheduled_execute" // 定时执行
	MessageTypeCancelExecute    MessageType = "cancel_execute"    // 取消执行
)

// String 返回字符串表示
func (m MessageType) String() string {
	return string(m)
}

// IsValid 验证消息类型是否有效
func (m MessageType) IsValid() bool {
	switch m {
	case MessageTypeExecute, MessageTypeScheduledExecute, MessageTypeCancelExecute:
		return true
	}
	return false
}

// RouteStrategy 路由策略
type RouteStrategy string

const (
	RouteStrategyFirst      RouteStrategy = "first"       // 第一个节点
	RouteStrategyLast       RouteStrategy = "last"        // 最后一个节点
	RouteStrategyRandom     RouteStrategy = "random"      // 随机节点
	RouteStrategyRoundRobin RouteStrategy = "round_robin" // 轮询
	RouteStrategyBroadcast  RouteStrategy = "broadcast"   // 广播（所有节点）
	RouteStrategySharding   RouteStrategy = "sharding"    // 分片（根据任务ID哈希）
	RouteStrategySpecific   RouteStrategy = "specific"    // 指定节点
)

// String 返回字符串表示
func (r RouteStrategy) String() string {
	return string(r)
}

// IsValid 验证路由策略是否有效
func (r RouteStrategy) IsValid() bool {
	switch r {
	case RouteStrategyFirst, RouteStrategyLast, RouteStrategyRandom,
		RouteStrategyRoundRobin, RouteStrategyBroadcast, RouteStrategySharding, RouteStrategySpecific:
		return true
	}
	return false
}

// ConfigOperation 配置操作类型
type ConfigOperation string

const (
	OperationAdd    ConfigOperation = "add"    // 添加配置
	OperationUpdate ConfigOperation = "update" // 更新配置
	OperationDelete ConfigOperation = "delete" // 删除配置
	OperationReload ConfigOperation = "reload" // 重载配置
)

// String 返回字符串表示
func (o ConfigOperation) String() string {
	return string(o)
}

// IsValid 验证操作是否有效
func (o ConfigOperation) IsValid() bool {
	switch o {
	case OperationAdd, OperationUpdate, OperationDelete, OperationReload:
		return true
	}
	return false
}

// ExecuteRequest 执行请求
type ExecuteRequest struct {
	MessageID     string        `json:"message_id"`     // 消息ID
	MessageType   MessageType   `json:"message_type"`   // 消息类型
	JobName       string        `json:"job_name"`       // 任务名称
	RouteStrategy RouteStrategy `json:"route_strategy"` // 路由策略
	TargetNodes   []string      `json:"target_nodes"`   // 目标节点列表（specific策略时使用）
	ExecuteTime   *time.Time    `json:"execute_time"`   // 执行时间（scheduled_execute时使用）
	RepeatCount   int           `json:"repeat_count"`   // 重复次数（scheduled_execute时使用）
	Timeout       int           `json:"timeout"`        // 超时时间（秒）
	TraceID       string        `json:"trace_id"`       // 链路追踪ID
	RequestTime   time.Time     `json:"request_time"`   // 请求时间
}

// ExecuteResponse 执行响应
type ExecuteResponse struct {
	MessageID    string      `json:"message_id"`    // 消息ID
	JobName      string      `json:"job_name"`      // 任务名称
	NodeID       string      `json:"node_id"`       // 执行节点ID
	Success      bool        `json:"success"`       // 是否成功
	Result       interface{} `json:"result"`        // 执行结果
	Error        string      `json:"error"`         // 错误信息
	StartTime    time.Time   `json:"start_time"`    // 开始时间
	EndTime      time.Time   `json:"end_time"`      // 结束时间
	Duration     int64       `json:"duration"`      // 执行时长（毫秒）
	TraceID      string      `json:"trace_id"`      // 链路追踪ID
	ResponseTime time.Time   `json:"response_time"` // 响应时间
}

// ConfigUpdateRequest 配置更新请求
type ConfigUpdateRequest struct {
	MessageID   string          `json:"message_id"`   // 消息ID
	JobName     string          `json:"job_name"`     // 任务名称
	Operation   ConfigOperation `json:"operation"`    // 操作类型
	Config      jobs.TaskCfg    `json:"config"`       // 任务配置
	CronSpec    string          `json:"cron_spec"`    // Cron表达式（单独修改时使用）
	TargetNodes []string        `json:"target_nodes"` // 目标节点列表（为空则广播）
	TraceID     string          `json:"trace_id"`     // 链路追踪ID
	RequestTime time.Time       `json:"request_time"` // 请求时间
}

// ConfigUpdateResponse 配置更新响应
type ConfigUpdateResponse struct {
	MessageID    string    `json:"message_id"`    // 消息ID
	JobName      string    `json:"job_name"`      // 任务名称
	NodeID       string    `json:"node_id"`       // 节点ID
	Success      bool      `json:"success"`       // 是否成功
	Error        string    `json:"error"`         // 错误信息
	ResponseTime time.Time `json:"response_time"` // 响应时间
}
