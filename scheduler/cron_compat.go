/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 21:40:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 15:26:57
 * @FilePath: \kronos-scheduler\scheduler\cron_compat.go
 * @Description: Cron 兼容层 - 兼容 robfig/cron/v3 API，支持平滑迁移
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"time"

	"github.com/kamalyes/go-config/pkg/jobs"
)

// Cron 兼容 robfig/cron/v3 的调度器(可平滑替换)
type Cron struct {
	scheduler *CronScheduler
	location  *time.Location
	parser    *CronParser
	chain     *JobChain
}

// Job 兼容 robfig/cron/v3 的任务接口
type Job interface {
	Run()
}

// FuncJob 将函数包装为任务
type FuncJob func()

// Run 实现 Job 接口
func (f FuncJob) Run() { f() }

// New 创建新的 Cron 实例(兼容 robfig/cron/v3 API)
func New(opts ...CronOption) *Cron {
	c := &Cron{
		location: time.Local,
		parser:   SecondParser,
		chain:    NewChain(),
	}

	// 应用选项
	for _, opt := range opts {
		opt(c)
	}

	// 创建底层调度器
	c.scheduler = NewCronScheduler(
		WithCronLocationOpt(c.location),
		WithParser(c.parser),
		WithCronChainOpt(c.chain),
	)

	return c
}

// WithCronLocationOpt 设置时区(兼容 robfig/cron/v3)
func WithCronLocationOpt(loc *time.Location) CronSchedulerOption {
	return func(c *CronScheduler) {
		c.location = loc
	}
}

// WithCronChainOpt 设置装饰器链(兼容 robfig/cron/v3)
func WithCronChainOpt(chain *JobChain) CronSchedulerOption {
	return func(c *CronScheduler) {
		c.chain = chain
	}
}

// CronOption 配置选项(兼容 robfig/cron/v3)
type CronOption func(*Cron)

// WithCronLocation 设置时区(兼容 robfig/cron/v3)
func WithCronLocation(loc *time.Location) CronOption {
	return func(c *Cron) {
		c.location = loc
	}
}

// WithSeconds 使用带秒的解析器(兼容 robfig/cron/v3)
func WithSeconds() CronOption {
	return func(c *Cron) {
		c.parser = SecondParser
	}
}

// WithCronLogger 设置日志器(兼容 robfig/cron/v3)
func WithCronLogger(log Logger) CronOption {
	return func(c *Cron) {
		// 将日志器传递给调度器
		if c.scheduler != nil {
			c.scheduler.logger = log
		}
	}
}

// WithCronChain 设置任务链(兼容 robfig/cron/v3)
func WithCronChain(chain *JobChain) CronOption {
	return func(c *Cron) {
		c.chain = chain
	}
}

// AddFunc 添加函数任务(兼容 robfig/cron/v3)
func (c *Cron) AddFunc(spec string, cmd func()) (EntryId, error) {
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob 添加任务(兼容 robfig/cron/v3)
func (c *Cron) AddJob(spec string, cmd Job) (EntryId, error) {
	// 包装为内部 job.Job 接口
	wrappedJob := &compatJob{
		name:    fmt.Sprintf("job_%d", time.Now().UnixNano()),
		cronJob: cmd,
	}

	config := jobs.TaskCfg{
		CronSpec: spec,
		Enabled:  true,
	}

	if err := c.scheduler.RegisterJob(wrappedJob, config); err != nil {
		return 0, err
	}

	// 直接查找对应的 EntryID
	c.scheduler.mu.RLock()
	defer c.scheduler.mu.RUnlock()

	if entry, exists := c.scheduler.entries[wrappedJob.Name()]; exists {
		return entry.Id, nil
	}

	return 0, ErrJobAddFailed
}

// Schedule 添加调度任务(兼容 robfig/cron/v3)
func (c *Cron) Schedule(schedule Schedule, cmd Job) EntryId {
	wrappedJob := &compatJob{
		name:    fmt.Sprintf("job_%d", time.Now().UnixNano()),
		cronJob: cmd,
	}

	// 从 Schedule 对象构建配置
	entry := &Entry{
		Id:         c.scheduler.nextId,
		JobName:    wrappedJob.Name(),
		Schedule:   schedule,
		Job:        wrappedJob,
		WrappedJob: c.scheduler.chain.Then(wrappedJob),
		Next:       schedule.Next(time.Now().In(c.location)),
		Config: jobs.TaskCfg{
			Enabled: true,
		},
	}
	c.scheduler.nextId++

	// 直接注册条目
	c.scheduler.mu.Lock()
	c.scheduler.entries[entry.JobName] = entry
	heap.Push(&c.scheduler.heap, entry)
	c.scheduler.mu.Unlock()

	// 唤醒调度循环
	c.scheduler.wakeUp()

	return entry.Id
}

// Entries 获取所有任务条目(兼容 robfig/cron/v3)
func (c *Cron) Entries() []Entry {
	c.scheduler.mu.RLock()
	defer c.scheduler.mu.RUnlock()

	result := make([]Entry, 0, len(c.scheduler.heap))
	for _, e := range c.scheduler.heap {
		result = append(result, *e)
	}
	return result
}

// Entry 获取指定任务条目(兼容 robfig/cron/v3)
func (c *Cron) Entry(id EntryId) Entry {
	c.scheduler.mu.RLock()
	defer c.scheduler.mu.RUnlock()

	for _, entry := range c.scheduler.entries {
		if entry.Id == id {
			return *entry
		}
	}
	return Entry{}
}

// Remove 移除任务(兼容 robfig/cron/v3)
func (c *Cron) Remove(id EntryId) {
	c.scheduler.mu.RLock()
	var jobName string
	for _, entry := range c.scheduler.entries {
		if entry.Id == id {
			jobName = entry.JobName
			break
		}
	}
	c.scheduler.mu.RUnlock()

	if jobName != "" {
		c.scheduler.UnregisterJob(jobName)
	}
}

// Start 启动调度器(兼容 robfig/cron/v3)
func (c *Cron) Start() {
	c.scheduler.Start()
}

// Stop 停止调度器并返回上下文(兼容 robfig/cron/v3)
func (c *Cron) Stop() context.Context {
	c.scheduler.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	// 等待所有任务完成后取消上下文
	go func() {
		c.scheduler.jobWaiter.Wait()
		cancel()
	}()
	return ctx
}

// Location 获取时区(兼容 robfig/cron/v3)
func (c *Cron) Location() *time.Location {
	return c.location
}

// compatJob 兼容层任务包装
type compatJob struct {
	name    string
	cronJob Job
}

func (j *compatJob) Name() string        { return j.name }
func (j *compatJob) Description() string { return "cron compatible job" }
func (j *compatJob) Execute(ctx context.Context) error {
	j.cronJob.Run()
	return nil
}
