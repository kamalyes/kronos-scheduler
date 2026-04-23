/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-25 15:05:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 16:09:51
 * @FilePath: \kronos-scheduler\scheduler\cron_scheduler_test.go
 * @Description: Cron调度器测试 - 使用 assert 校验
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/kamalyes/go-config/pkg/jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewCronScheduler 测试创建调度器
func TestNewCronScheduler(t *testing.T) {
	scheduler := NewCronScheduler()

	assert.NotNil(t, scheduler)
	assert.NotNil(t, scheduler.entries)
	assert.NotNil(t, scheduler.chain)
	assert.NotNil(t, scheduler.parser)
	assert.NotNil(t, scheduler.metrics)
	assert.Equal(t, DefaultMaxConcurrency, scheduler.maxConcurrency)
}

// TestNewCronSchedulerWithOptions 测试使用选项创建调度器
func TestNewCronSchedulerWithOptions(t *testing.T) {
	customParser := SecondParser
	customChain := NewChain()
	maxConcurrency := 20

	scheduler := NewCronScheduler(
		WithParser(customParser),
		WithChain(customChain),
		WithMaxConcurrency(maxConcurrency),
	)

	assert.NotNil(t, scheduler)
	assert.Equal(t, customParser, scheduler.parser)
	assert.Equal(t, customChain, scheduler.chain)
	assert.Equal(t, maxConcurrency, scheduler.maxConcurrency)
	assert.NotNil(t, scheduler.semaphore)
	assert.Equal(t, maxConcurrency, cap(scheduler.semaphore))
}

// TestRegisterJob 测试注册任务
func TestRegisterJob(t *testing.T) {
	scheduler := NewCronScheduler()

	j := &mockJob{
		name:        "test-job",
		description: "test job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	config := jobs.TaskCfg{
		CronSpec: "*/5 * * * * *",
		Enabled:  true,
	}

	err := scheduler.RegisterJob(j, config)

	assert.NoError(t, err)
	assert.Len(t, scheduler.entries, 1)
	assert.Contains(t, scheduler.entries, "test-job")
}

// TestRegisterJobWithNilJob 测试注册空任务
func TestRegisterJobWithNilJob(t *testing.T) {
	scheduler := NewCronScheduler()

	config := jobs.TaskCfg{
		CronSpec: "*/5 * * * * *",
		Enabled:  true,
	}

	err := scheduler.RegisterJob(nil, config)

	assert.Error(t, err)
	assert.Equal(t, ErrJobNil, err)
}

// TestRegisterJobWithEmptyName 测试注册空名称任务
func TestRegisterJobWithEmptyName(t *testing.T) {
	scheduler := NewCronScheduler()

	j := &mockJob{
		name:        "",
		description: "test job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	config := jobs.TaskCfg{
		CronSpec: "*/5 * * * * *",
		Enabled:  true,
	}

	err := scheduler.RegisterJob(j, config)

	assert.Error(t, err)
	assert.Equal(t, ErrJobNameEmpty, err)
}

// TestRegisterJobWithInvalidCron 测试注册无效 cron 表达式的任务
func TestRegisterJobWithInvalidCron(t *testing.T) {
	scheduler := NewCronScheduler()

	j := &mockJob{
		name:        "test-job",
		description: "test job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	config := jobs.TaskCfg{
		CronSpec: "invalid cron",
		Enabled:  true,
	}

	err := scheduler.RegisterJob(j, config)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrParseCronFailed)
}

// TestUnregisterJob 测试注销任务
func TestUnregisterJob(t *testing.T) {
	scheduler := NewCronScheduler()

	j := &mockJob{
		name:        "test-job",
		description: "test job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	config := jobs.TaskCfg{
		CronSpec: "*/5 * * * * *",
		Enabled:  true,
	}

	err := scheduler.RegisterJob(j, config)
	require.NoError(t, err)

	err = scheduler.UnregisterJob("test-job")

	assert.NoError(t, err)
	assert.Empty(t, scheduler.entries)
}

// TestUnregisterNonExistentJob 测试注销不存在的任务
func TestUnregisterNonExistentJob(t *testing.T) {
	scheduler := NewCronScheduler()

	err := scheduler.UnregisterJob("non-existent")

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrJobNotFound)
}

// TestStartAndStop 测试启动和停止调度器
func TestStartAndStop(t *testing.T) {
	scheduler := NewCronScheduler()

	err := scheduler.Start()
	assert.NoError(t, err)
	assert.True(t, scheduler.IsRunning())

	err = scheduler.Stop()
	assert.NoError(t, err)
	assert.False(t, scheduler.IsRunning())
}

// TestStartAlreadyRunning 测试启动已运行的调度器
func TestStartAlreadyRunning(t *testing.T) {
	scheduler := NewCronScheduler()

	err := scheduler.Start()
	require.NoError(t, err)

	err = scheduler.Start()
	assert.Error(t, err)
	assert.Equal(t, ErrSchedulerAlreadyRunning, err)

	scheduler.Stop()
}

// TestStopNotRunning 测试停止未运行的调度器
func TestStopNotRunning(t *testing.T) {
	scheduler := NewCronScheduler()

	err := scheduler.Stop()
	assert.Error(t, err)
	assert.Equal(t, ErrSchedulerNotRunning, err)
}

// TestGetJobStatus 测试获取任务状态
func TestGetJobStatus(t *testing.T) {
	scheduler := NewCronScheduler()

	j := &mockJob{
		name:        "test-job",
		description: "test job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	config := jobs.TaskCfg{
		CronSpec: "*/5 * * * * *",
		Enabled:  true,
	}

	err := scheduler.RegisterJob(j, config)
	require.NoError(t, err)

	status, err := scheduler.GetJobStatus("test-job")

	assert.NoError(t, err)
	assert.NotNil(t, status)
	assert.Equal(t, "test-job", status.JobName)
	assert.Equal(t, "*/5 * * * * *", status.CronSpec)
	assert.NotNil(t, status.NextExecutionAt)
}

// TestGetJobStatusNonExistent 测试获取不存在任务的状态
func TestGetJobStatusNonExistent(t *testing.T) {
	scheduler := NewCronScheduler()

	status, err := scheduler.GetJobStatus("non-existent")

	assert.Error(t, err)
	assert.Nil(t, status)
	assert.ErrorIs(t, err, ErrJobNotFound)
}

// TestListJobs 测试列出所有任务
func TestListJobs(t *testing.T) {
	scheduler := NewCronScheduler()

	job1 := &mockJob{
		name:        "job1",
		executeFunc: func(ctx context.Context) error { return nil },
	}
	job2 := &mockJob{
		name:        "job2",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	config := jobs.TaskCfg{
		CronSpec: "*/5 * * * * *",
		Enabled:  true,
	}

	scheduler.RegisterJob(job1, config)
	scheduler.RegisterJob(job2, config)

	jobList := scheduler.ListJobs()

	assert.Len(t, jobList, 2)
	assert.Contains(t, jobList, "job1")
	assert.Contains(t, jobList, "job2")
}

// TestHealthCheck 测试健康检查
func TestHealthCheck(t *testing.T) {
	scheduler := NewCronScheduler()

	// 未启动时不健康
	assert.False(t, scheduler.HealthCheck())

	// 启动但没有任务时不健康
	scheduler.Start()
	assert.False(t, scheduler.HealthCheck())

	// 添加任务后健康
	j := &mockJob{
		name:        "test-job",
		executeFunc: func(ctx context.Context) error { return nil },
	}
	config := jobs.TaskCfg{
		CronSpec: "*/5 * * * * *",
		Enabled:  true,
	}
	scheduler.RegisterJob(j, config)
	assert.True(t, scheduler.HealthCheck())

	scheduler.Stop()
}

// TestGetEntry 测试获取任务条目
func TestGetEntry(t *testing.T) {
	scheduler := NewCronScheduler()

	j := &mockJob{
		name:        "test-job",
		description: "test job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	config := jobs.TaskCfg{
		CronSpec: "*/5 * * * * *",
		Enabled:  true,
	}

	err := scheduler.RegisterJob(j, config)
	require.NoError(t, err)

	entry, ok := scheduler.GetEntry("test-job")

	assert.True(t, ok)
	assert.NotNil(t, entry)
	assert.Equal(t, "test-job", entry.JobName)
}

// TestGetNodeID 测试获取节点ID
func TestGetNodeID(t *testing.T) {
	scheduler := NewCronScheduler()

	nodeID := scheduler.GetNodeID()

	assert.NotEmpty(t, nodeID)
}

// TestRecordMetrics 测试记录指标
func TestRecordMetrics(t *testing.T) {
	scheduler := NewCronScheduler()

	// 先记录开始
	scheduler.metrics.RecordStart("test-job")

	// 记录成功
	scheduler.RecordMetrics("test-job", 100*time.Millisecond, nil)

	metrics := scheduler.GetJobMetrics("test-job")
	assert.NotNil(t, metrics)
	assert.Equal(t, int64(1), metrics["execution_count"])
	assert.Equal(t, int64(1), metrics["success_count"])
	assert.Equal(t, int64(0), metrics["failure_count"])
}

// TestUpdateCronSpec 测试动态更新 Cron 表达式
func TestUpdateCronSpec(t *testing.T) {
	scheduler := NewCronScheduler()

	j := &mockJob{
		name:        "test-job",
		description: "test job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	config := jobs.TaskCfg{
		CronSpec: "*/5 * * * * *",
		Enabled:  true,
	}

	err := scheduler.RegisterJob(j, config)
	require.NoError(t, err)

	// 更新表达式
	newSpec := "*/10 * * * * *"
	err = scheduler.UpdateCronSpec(context.Background(), "test-job", newSpec)

	assert.NoError(t, err)

	// 验证更新
	status, err := scheduler.GetJobStatus("test-job")
	require.NoError(t, err)
	assert.Equal(t, newSpec, status.CronSpec)
}

// TestUpdateCronSpecInvalid 测试更新为无效的 Cron 表达式
func TestUpdateCronSpecInvalid(t *testing.T) {
	scheduler := NewCronScheduler()

	j := &mockJob{
		name:        "test-job",
		description: "test job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	config := jobs.TaskCfg{
		CronSpec: "*/5 * * * * *",
		Enabled:  true,
	}

	err := scheduler.RegisterJob(j, config)
	require.NoError(t, err)

	// 尝试更新为无效表达式
	err = scheduler.UpdateCronSpec(context.Background(), "test-job", "invalid")

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidCronSpec)
}

// TestLoadFromConfig 测试从配置加载
func TestLoadFromConfig(t *testing.T) {
	scheduler := NewCronScheduler()

	cfg := &jobs.Jobs{
		Enabled:           true,
		TimeZone:          "Asia/Shanghai",
		GracefulShutdown:  30,
		MaxRetries:        3,
		RetryInterval:     1000,
		MaxConcurrentJobs: 15,
		Tasks:             make(map[string]jobs.TaskCfg),
	}

	err := scheduler.LoadFromConfig(cfg)

	assert.NoError(t, err)
	assert.Equal(t, cfg, scheduler.globalConfig)
	assert.Equal(t, 15, scheduler.maxConcurrency)
}

// TestLoadFromConfigNil 测试加载空配置
func TestLoadFromConfigNil(t *testing.T) {
	scheduler := NewCronScheduler()

	err := scheduler.LoadFromConfig(nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be nil")
}

// TestSetJobProtector 测试设置任务保护器
func TestSetJobProtector(t *testing.T) {
	scheduler := NewCronScheduler()

	j := &mockJob{
		name:        "test-job",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	config := jobs.TaskCfg{
		CronSpec: "*/5 * * * * *",
		Enabled:  true,
	}

	err := scheduler.RegisterJob(j, config)
	require.NoError(t, err)

	protector := NewJobProtector("test-job")
	err = scheduler.SetJobProtector("test-job", protector)

	assert.NoError(t, err)
	assert.Equal(t, protector, scheduler.GetJobProtector("test-job"))
}

// TestSetJobProtectorNonExistent 测试为不存在的任务设置保护器
func TestSetJobProtectorNonExistent(t *testing.T) {
	scheduler := NewCronScheduler()

	protector := NewJobProtector("test-job")
	err := scheduler.SetJobProtector("non-existent", protector)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrJobNotFound)
}

// TestGetAllTasksDetails 测试获取所有任务详情
func TestGetAllTasksDetails(t *testing.T) {
	scheduler := NewCronScheduler()

	job1 := &mockJob{
		name:        "job1",
		executeFunc: func(ctx context.Context) error { return nil },
	}
	job2 := &mockJob{
		name:        "job2",
		executeFunc: func(ctx context.Context) error { return nil },
	}

	config := jobs.TaskCfg{
		CronSpec: "*/5 * * * * *",
		Enabled:  true,
	}

	scheduler.RegisterJob(job1, config)
	scheduler.RegisterJob(job2, config)

	details := scheduler.GetAllTasksDetails()

	assert.NotNil(t, details)
	assert.Equal(t, 2, details["total_tasks"])
	assert.Equal(t, DefaultMaxConcurrency, details["max_concurrency"])
	assert.False(t, details["is_running"].(bool))

	tasks := details["tasks"].(map[string]interface{})
	assert.Len(t, tasks, 2)
}
