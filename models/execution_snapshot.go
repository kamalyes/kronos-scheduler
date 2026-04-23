/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-25 15:10:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-04-23 09:29:53
 * @FilePath: \kronos-scheduler\models\execution_snapshot.go
 * @Description: 任务执行快照 - 记录任务执行的详细信息和指标
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package models

import (
	"fmt"
	"github.com/kamalyes/go-toolbox/pkg/mathx"
	"time"
)

// JobType 任务类型
type JobType int

const (
	// JobTypeCron Cron定时任务
	JobTypeCron JobType = iota
	// JobTypePeriodic 周期任务
	JobTypePeriodic
	// JobTypeManual 手动触发任务
	JobTypeManual
	// JobTypeSyncx Syncx任务
	JobTypeSyncx
)

// String 返回任务类型的字符串表示
func (t JobType) String() string {
	return [...]string{
		"Cron",
		"Periodic",
		"Manual",
		"Syncx",
	}[t]
}

// ExecutionStatus 执行状态
type ExecutionStatus int

const (
	// StatusPending 等待执行
	StatusPending ExecutionStatus = iota
	// StatusRunning 执行中
	StatusRunning
	// StatusSuccess 执行成功
	StatusSuccess
	// StatusFailure 执行失败
	StatusFailure
	// StatusTimeout 执行超时
	StatusTimeout
	// StatusRetrying 重试中
	StatusRetrying
	// StatusAborted 用户中止
	StatusAborted
	// StatusSystemAborted 系统中止
	StatusSystemAborted
	// StatusSkipped 跳过执行
	StatusSkipped
)

// String 返回状态的字符串表示
func (s ExecutionStatus) String() string {
	return [...]string{
		"Pending",
		"Running",
		"Success",
		"Failure",
		"Timeout",
		"Retrying",
		"Aborted",
		"SystemAborted",
		"Skipped",
	}[s]
}

// IsTerminal 判断是否为终止状态(不会再变化)
func (s ExecutionStatus) IsTerminal() bool {
	return s == StatusSuccess || s == StatusFailure || s == StatusTimeout ||
		s == StatusAborted || s == StatusSystemAborted || s == StatusSkipped
}

// ExecutionSnapshotModel 执行快照数据库模型
type ExecutionSnapshotModel struct {
	ID               uint32          `gorm:"primaryKey;autoIncrement;comment:主键ID" json:"id"`
	TraceID          string          `gorm:"type:varchar(100);uniqueIndex;not null;comment:跟踪ID" json:"trace_id"`
	JobID            string          `gorm:"type:varchar(100);index;not null;comment:任务ID" json:"job_id"`
	JobName          string          `gorm:"type:varchar(200);index;comment:任务名称" json:"job_name"`
	JobType          JobType         `gorm:"type:tinyint;comment:任务类型" json:"job_type"`
	NodeID           string          `gorm:"type:varchar(100);index;comment:执行节点ID" json:"node_id"`
	TriggerAt        time.Time       `gorm:"index;comment:触发时间" json:"trigger_at"`
	StartTime        time.Time       `gorm:"index;comment:开始执行时间" json:"start_time"`
	EndTime          *time.Time      `gorm:"comment:结束执行时间" json:"end_time"`
	Status           ExecutionStatus `gorm:"type:tinyint;index;comment:当前状态" json:"status"`
	PreviousStatus   ExecutionStatus `gorm:"type:tinyint;comment:上一个状态" json:"previous_status"`
	StatusTransition time.Time       `gorm:"comment:状态变更时间" json:"status_transition"`
	ExecFrequency    int             `gorm:"default:0;comment:累计执行次数" json:"exec_frequency"`
	FailureFrequency int             `gorm:"default:0;comment:累计失败次数" json:"failure_frequency"`
	SuccessFrequency int             `gorm:"default:0;comment:累计成功次数" json:"success_frequency"`
	Error            string          `gorm:"type:text;comment:错误信息" json:"error,omitempty"`
	Result           string          `gorm:"type:text;comment:执行结果" json:"result,omitempty"`
	StackTrace       string          `gorm:"type:text;comment:错误堆栈" json:"stack_trace,omitempty"`
	LogsJSON         string          `gorm:"type:text;comment:执行日志JSON" json:"logs_json,omitempty"`
	MetricsJSON      string          `gorm:"type:text;comment:性能指标JSON" json:"metrics_json,omitempty"`
	LabelsJSON       string          `gorm:"type:text;comment:标签JSON" json:"labels_json,omitempty"`
	CreatedAt        time.Time       `gorm:"index;comment:创建时间" json:"created_at"`
	UpdatedAt        time.Time       `gorm:"comment:更新时间" json:"updated_at"`
}

// TableName 指定表名
func (ExecutionSnapshotModel) TableName() string {
	return "job_execution_snapshots"
}

// TableComment 表注释
func (ExecutionSnapshotModel) TableComment() string {
	return "任务执行快照表"
}

// NewExecutionSnapshot 创建新的执行快照
func NewExecutionSnapshot(traceID, jobID, jobName string, jobType JobType, nodeID string) *ExecutionSnapshotModel {
	now := time.Now()
	return &ExecutionSnapshotModel{
		TraceID:          traceID,
		JobID:            jobID,
		JobName:          jobName,
		JobType:          jobType,
		NodeID:           nodeID,
		TriggerAt:        now,
		StartTime:        now,
		Status:           StatusRunning,
		PreviousStatus:   StatusPending,
		StatusTransition: now,
		ExecFrequency:    0,
		FailureFrequency: 0,
		SuccessFrequency: 0,
	}
}

// updateTerminalState 统一处理终态变更
func (s *ExecutionSnapshotModel) updateTerminalState(status ExecutionStatus) {
	now := time.Now()
	s.EndTime = &now
	s.PreviousStatus = s.Status
	s.Status = status
	s.StatusTransition = now
	s.ExecFrequency++
}

// MarkSuccess 标记执行成功
func (s *ExecutionSnapshotModel) MarkSuccess(data interface{}) {
	s.updateTerminalState(StatusSuccess)
	s.SuccessFrequency++
	if data != nil {
		s.Result = fmt.Sprintf("%v", data)
	}
}

// MarkFailure 标记执行失败
func (s *ExecutionSnapshotModel) MarkFailure(err error) {
	s.updateTerminalState(StatusFailure)
	s.FailureFrequency++
	s.Error = mathx.IfDo(err != nil, func() string { return err.Error() }, "")
}

// Complete 完成执行（根据错误自动判断成功或失败）
func (s *ExecutionSnapshotModel) Complete(err error, data interface{}) {
	if err != nil {
		s.MarkFailure(err)
	} else {
		s.MarkSuccess(data)
	}
}
