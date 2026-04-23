/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-04-23 09:29:37
 * @FilePath: \kronos-scheduler\models\job_config.go
 * @Description: Job配置数据模型
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package models

import (
	"fmt"
	"github.com/kamalyes/go-config/pkg/jobs"
	"github.com/kamalyes/go-toolbox/pkg/serializer"
	"time"
)

// JobConfigModel Job配置数据库模型
type JobConfigModel struct {
	Id              uint32     `gorm:"primaryKey;autoIncrement;comment:主键Id" json:"id"`
	JobName         string     `gorm:"type:varchar(100);uniqueIndex;not null;comment:任务名称" json:"job_name"`
	TaskConfig      string     `gorm:"type:text;not null;comment:任务配置(TaskCfg JSON)" json:"task_config"`
	ExtraData       string     `gorm:"type:text;comment:额外配置变量(JSON)" json:"extra_data"`
	LastExecutedAt  *time.Time `gorm:"comment:上次执行时间" json:"last_executed_at"`
	NextExecutionAt *time.Time `gorm:"comment:下次执行时间" json:"next_execution_at"`
	CreatedAt       time.Time  `gorm:"comment:创建时间" json:"created_at"`
	UpdatedAt       time.Time  `gorm:"comment:更新时间" json:"updated_at"`
}

// TableName 指定表名
func (JobConfigModel) TableName() string {
	return "job_scheduler_configs"
}

// TableComment 表注释
func (JobConfigModel) TableComment() string {
	return "任务调度配置表"
}

// ToTaskCfg 转换为 TaskCfg
func (m *JobConfigModel) ToTaskCfg() (*jobs.TaskCfg, error) {
	if m == nil || m.TaskConfig == "" {
		return nil, nil
	}
	return serializer.FromJSON[*jobs.TaskCfg](m.TaskConfig), nil
}

// FromTaskCfg 从 TaskCfg 创建模型
func FromTaskCfg(jobName string, cfg *jobs.TaskCfg) *JobConfigModel {
	if cfg == nil {
		return nil
	}
	return &JobConfigModel{
		JobName:    jobName,
		TaskConfig: serializer.ToJSON(cfg),
	}
}

// UpdateFromTaskCfg 更新配置
func (m *JobConfigModel) UpdateFromTaskCfg(cfg *jobs.TaskCfg) {
	if cfg != nil {
		m.TaskConfig = serializer.ToJSON(cfg)
	}
}

// UpdateCronSpec 更新 CronSpec 字段
func (m *JobConfigModel) UpdateCronSpec(cronSpec string) error {
	taskCfg, err := m.ToTaskCfg()
	if err != nil {
		return fmt.Errorf("failed to parse task config: %w", err)
	}
	if taskCfg == nil {
		return fmt.Errorf("task config is nil")
	}
	taskCfg.CronSpec = cronSpec
	m.UpdateFromTaskCfg(taskCfg)
	return nil
}

// JobStatus Job运行状态信息
type JobStatus struct {
	JobName         string     `json:"job_name"`
	CronSpec        string     `json:"cron_spec"`
	LastExecutedAt  *time.Time `json:"last_executed_at"`
	NextExecutionAt *time.Time `json:"next_execution_at"`
	IsRunning       bool       `json:"is_running"`
	ExecutionCount  int64      `json:"execution_count"`
}
