/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 15:05:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-26 21:50:45
 * @FilePath: \kronos-scheduler\scheduler\cron_scheduler.go
 * @Description: Cron调度器核心实现 - 高性能、链式调用、集成数仓
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kamalyes/go-config/pkg/jobs"
	"github.com/kamalyes/go-toolbox/pkg/breaker"
	"github.com/kamalyes/go-toolbox/pkg/idgen"
	"github.com/kamalyes/go-toolbox/pkg/osx"
	"github.com/kamalyes/go-toolbox/pkg/queue"
	"github.com/kamalyes/go-toolbox/pkg/retry"
	"github.com/kamalyes/kronos-scheduler/job"
	"github.com/kamalyes/kronos-scheduler/logger"
	"github.com/kamalyes/kronos-scheduler/models"
	"github.com/kamalyes/kronos-scheduler/pubsub"
	"github.com/kamalyes/kronos-scheduler/repository"
)

// EntryId 任务条目 Id
type EntryId int

// ExecutionResult 任务执行结果
type ExecutionResult struct {
	JobName   string
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
	Success   bool
	Error     error
	Retries   int
}

// NodeResult 节点执行结果
type NodeResult struct {
	JobName   string
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
	Error     error
	Retries   int
}

// Entry 调度条目
type Entry struct {
	Id         EntryId      // 条目 Id
	JobName    string       // 任务名称
	Schedule   Schedule     // 调度规范
	Job        job.Job      // 原始任务
	WrappedJob job.Job      // 包装后的任务
	Next       time.Time    // 下次执行时间
	Prev       time.Time    // 上次执行时间
	Config     jobs.TaskCfg // 任务配置(使用 go-config)
}

// Valid 检查条目是否有效
func (e *Entry) Valid() bool { return e.Id != 0 }

// CronScheduler Cron 调度器 - 高性能实现(使用 go-toolbox 优化)
type CronScheduler struct {
	// 核心存储(优化：最小堆自动排序)
	entries map[string]*Entry // 任务条目映射(O(1) 查找)
	heap    entryHeap         // 最小堆(O(log n) 插入/删除)

	chain    *JobChain                 // 装饰器链
	parser   *CronParser               // 表达式解析器
	location *time.Location            // 时区
	logger   logger.Logger             // 日志器
	metrics  *breaker.MetricsCollector // 指标收集器

	// 全局配置（来自 go-config）
	globalConfig *jobs.Jobs // go-config 全局配置

	// 任务保护
	protectors map[string]*JobProtector // 任务保护器映射

	// 数仓集成
	jobRepo          repository.JobConfigRepository          // 任务配置仓库
	execSnapshotRepo repository.IExecutionSnapshotRepository // 执行快照仓库
	cacheRepo        repository.CacheRepository              // 缓存仓库

	schedulerCache *repository.SchedulerCache // 调度器缓存
	nodeRegistry   *NodeRegistry              // 节点注册器
	distributed    bool                       // 是否启用分布式
	pubsubClient   pubsub.PubSub              // 独立的 PubSub 客户端

	// 高级特性(使用 go-toolbox)
	priorityQueue *queue.PriorityQueue // 优先级队列(手动触发任务)
	retryManager  *retry.Retry         // 重试管理器
	idGenerator   idgen.IDGenerator    // ID生成器

	// 节点标识(初始化时生成)
	workerId int64  // Worker ID(0-1023)
	nodeID   string // 节点ID(主机名或环境变量)

	// 并发控制
	maxConcurrency int             // 最大并发数
	semaphore      chan struct{}   // 并发控制信号量
	executions     map[string]bool // 正在执行的任务

	// 远程执行器
	remoteExecutor *RemoteExecutor
	loader         *Loader
	loadStrategy   LoadStrategy

	// 运行控制(优化：减少锁竞争)
	running   bool          // 是否运行中
	runningMu sync.RWMutex  // 运行状态锁
	mu        sync.RWMutex  // 条目操作锁
	stop      chan struct{} // 停止信号
	wakeup    chan struct{} // 唤醒信号

	// 并发控制
	jobWaiter sync.WaitGroup // 任务等待组
	nextId    EntryId        // 下一个条目 Id
}

// entryHeap 最小堆实现(按 Next 时间排序)
type entryHeap []*Entry

func (h entryHeap) Len() int { return len(h) }
func (h entryHeap) Less(i, j int) bool {
	// 零时间排到最后
	if h[i].Next.IsZero() {
		return false
	}
	if h[j].Next.IsZero() {
		return true
	}
	return h[i].Next.Before(h[j].Next)
}
func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *entryHeap) Push(x interface{}) {
	*h = append(*h, x.(*Entry))
}

func (h *entryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// CronSchedulerOption 调度器选项
type CronSchedulerOption func(*CronScheduler)

// WithParser 设置解析器
func WithParser(parser *CronParser) CronSchedulerOption {
	return func(c *CronScheduler) {
		c.parser = parser
	}
}

// WithChain 设置装饰器链
func WithChain(chain *JobChain) CronSchedulerOption {
	return func(c *CronScheduler) {
		c.chain = chain
	}
}

// WithLoggerOption 设置日志器
func WithLoggerOption(log logger.Logger) CronSchedulerOption {
	return func(c *CronScheduler) {
		c.logger = log
	}
}

// WithRepositories 设置数仓(集成持久化能力)
func WithRepositories(
	jobRepo repository.JobConfigRepository,
	execSnapshotRepo *repository.ExecutionSnapshotRepository,
	cacheRepo repository.CacheRepository,
) CronSchedulerOption {
	return func(c *CronScheduler) {
		c.jobRepo = jobRepo
		c.execSnapshotRepo = execSnapshotRepo
		c.cacheRepo = cacheRepo
	}
}

// WithMaxConcurrency 设置最大并发数
func WithMaxConcurrency(maxConcurrency int) CronSchedulerOption {
	return func(c *CronScheduler) {
		if maxConcurrency > 0 {
			c.maxConcurrency = maxConcurrency
		}
	}
}

// WithSchedulerCache 设置调度器缓存(分布式特性)
func WithSchedulerCache(cache *repository.SchedulerCache) CronSchedulerOption {
	return func(c *CronScheduler) {
		c.schedulerCache = cache
	}
}

// WithDistributed 启用分布式模式
func WithDistributed(nodeRegistry *NodeRegistry) CronSchedulerOption {
	return func(c *CronScheduler) {
		c.distributed = true
		c.nodeRegistry = nodeRegistry
	}
}

// WithPubSub 设置 PubSub 客户端
func WithPubSub(ps pubsub.PubSub) CronSchedulerOption {
	return func(c *CronScheduler) {
		c.pubsubClient = ps
	}
}

// WithConfigLoadStrategy 设置配置加载策略
func WithConfigLoadStrategy(strategy LoadStrategy) CronSchedulerOption {
	return func(c *CronScheduler) {
		c.loadStrategy = strategy
	}
}

// NewCronScheduler 创建 Cron 调度器(高性能版本 + go-toolbox 工具库)
func NewCronScheduler(opts ...CronSchedulerOption) *CronScheduler {
	c := &CronScheduler{
		entries:    make(map[string]*Entry),
		heap:       make(entryHeap, 0, DefaultHeapCapacity), // 预分配容量
		chain:      NewChain(),
		parser:     SecondParser, // 默认使用带秒的解析器
		location:   time.Local,
		metrics:    breaker.NewMetricsCollector(),
		protectors: make(map[string]*JobProtector), // 初始化保护器映射
		stop:       make(chan struct{}),
		wakeup:     make(chan struct{}, 1), // 带缓冲，避免阻塞
		nextId:     1,
		logger:     logger.NewSchedulerLogger("Scheduler"),

		// go-toolbox 集成
		priorityQueue: queue.NewPriorityQueue(),
		retryManager: retry.NewRetry().
			SetAttemptCount(3).
			SetInterval(time.Second).
			SetBackoffMultiplier(2.0).
			SetJitter(true), // 默认配置，实际使用时根据任务配置动态调整
		idGenerator: idgen.NewIDGenerator(idgen.GeneratorTypeDefault),

		// 并发控制(默认 10 个并发)
		maxConcurrency: DefaultMaxConcurrency,
		executions:     make(map[string]bool),

		// 节点标识(使用 osx 包获取)
		workerId: osx.GetWorkerId(),
		nodeID:   osx.SafeGetHostName(),
	}

	// 应用选项
	for _, opt := range opts {
		opt(c)
	}

	// 初始化并发信号量
	if c.maxConcurrency > 0 {
		c.semaphore = make(chan struct{}, c.maxConcurrency)
	}

	// 初始化远程执行器和配置加载器
	if c.schedulerCache != nil && c.schedulerCache.GetRedis() != nil {
		c.remoteExecutor = newRemoteExecutor(c, c.schedulerCache.GetRedis())
	}
	c.loader = newLoader(c, c.loadStrategy)

	return c
}

// LoadFromConfig 从 go-config 加载所有任务配置
func (c *CronScheduler) LoadFromConfig(cfg *jobs.Jobs) error {
	if cfg == nil {
		return ErrJobsConfigNil
	}

	// 验证配置
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidJobsConfig, err)
	}

	// 保存全局配置
	c.globalConfig = cfg

	// 应用全局配置
	if loc, err := cfg.GetTimeZoneLocation(); err == nil {
		c.location = loc
	}

	// 应用全局并发限制
	if cfg.MaxConcurrentJobs > 0 {
		c.maxConcurrency = cfg.MaxConcurrentJobs
		c.semaphore = make(chan struct{}, c.maxConcurrency)
	}

	c.logger.Info("加载任务配置: 启用=%v, 任务数=%d, 时区=%s",
		cfg.Enabled, len(cfg.Tasks), cfg.TimeZone)

	// 注册所有启用的任务（需要外部提供 Job 实例）
	// 注意：这里只保存配置，实际任务注册由 RegisterJob 完成
	return nil
}

// mergeTaskConfig 合并任务配置（子配置继承全局配置）
func (c *CronScheduler) mergeTaskConfig(taskCfg jobs.TaskCfg) jobs.TaskCfg {
	if c.globalConfig == nil {
		return taskCfg
	}

	merged := taskCfg

	// 继承全局重试配置
	if merged.MaxRetries == 0 && c.globalConfig.MaxRetries > 0 {
		merged.MaxRetries = c.globalConfig.MaxRetries
	}
	if merged.RetryInterval == 0 && c.globalConfig.RetryInterval > 0 {
		merged.RetryInterval = c.globalConfig.RetryInterval
	}
	if merged.RetryJitter == 0 && c.globalConfig.RetryJitter > 0 {
		merged.RetryJitter = c.globalConfig.RetryJitter
	}

	return merged
}

// RegisterJob 注册任务
func (c *CronScheduler) RegisterJob(j job.Job, config jobs.TaskCfg) error {
	if j == nil {
		return ErrJobNil
	}
	if j.Name() == "" {
		return ErrJobNameEmpty
	}

	// 合并全局配置和任务配置
	config = c.mergeTaskConfig(config)

	// 解析 cron 表达式
	schedule, err := c.parser.Parse(config.CronSpec)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrParseCronFailed, err)
	}

	// 应用装饰器链
	wrappedJob := c.chain.Then(j)

	// 创建条目
	entry := &Entry{
		Id:         c.nextId,
		JobName:    j.Name(),
		Schedule:   schedule,
		Job:        j,
		WrappedJob: wrappedJob,
		Config:     config,
		Next:       schedule.Next(time.Now().In(c.location)),
	}
	c.nextId++

	// 保存任务配置
	if err := c.saveTaskConfig(context.Background(), j.Name(), config); err != nil {
		// 记录错误但不中断注册流程
	}

	// 优化：直接操作，减少通道开销
	c.mu.Lock()
	c.entries[entry.JobName] = entry
	heap.Push(&c.heap, entry)
	c.mu.Unlock()

	// 唤醒调度循环
	c.wakeUp()

	return nil
}

// SetJobProtector 为任务设置保护器
func (c *CronScheduler) SetJobProtector(jobName string, protector *JobProtector) error {
	if jobName == "" {
		return ErrJobNameEmpty
	}
	if protector == nil {
		return ErrProtectorNil
	}

	c.mu.Lock()
	if _, ok := c.entries[jobName]; !ok {
		c.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrJobNotFound, jobName)
	}
	c.protectors[jobName] = protector
	c.mu.Unlock()

	return nil
}

// GetJobProtector 获取任务保护器
func (c *CronScheduler) GetJobProtector(jobName string) *JobProtector {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.protectors[jobName]
}

// UnregisterJob 注销任务
func (c *CronScheduler) UnregisterJob(jobName string) error {
	if jobName == "" {
		return ErrJobNameEmpty
	}

	c.mu.Lock()
	_, ok := c.entries[jobName]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrJobNotFound, jobName)
	}

	delete(c.entries, jobName)

	// 从堆中移除(O(n) 查找 + O(log n) 删除)
	for i, e := range c.heap {
		if e.JobName == jobName {
			heap.Remove(&c.heap, i)
			break
		}
	}

	// 清理保护器
	delete(c.protectors, jobName)

	c.mu.Unlock()

	// 删除任务配置
	c.deleteTaskConfig(context.Background(), jobName)

	// 唤醒调度循环
	c.wakeUp()

	return nil
}

// wakeUp 唤醒调度循环(非阻塞)
func (c *CronScheduler) wakeUp() {
	select {
	case c.wakeup <- struct{}{}:
	default:
		// 已有唤醒信号，跳过
	}
}

// Start 启动调度器
func (c *CronScheduler) Start() error {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	if c.running {
		return ErrSchedulerAlreadyRunning
	}

	c.running = true
	c.stop = make(chan struct{})

	ctx := context.Background()

	// 注册节点（分布式模式）
	if err := c.registerNode(ctx); err != nil {
		c.running = false
		return err
	}

	// 使用配置加载器加载配置
	if c.loader != nil {
		if err := c.loader.LoadConfigs(ctx); err != nil {
			c.logger.Warnf("⚠️ 加载配置失败: %v", err)
		}
	}

	// 启动远程执行器
	if c.remoteExecutor != nil {
		if err := c.remoteExecutor.Start(ctx); err != nil {
			c.logger.Warnf("⚠️ 启动远程执行器失败: %v", err)
		}
	}

	// 初始化所有任务的下次执行时间
	now := time.Now().In(c.location)
	c.mu.Lock()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
	}
	heap.Init(&c.heap) // 重建堆
	c.mu.Unlock()

	// 启动调度循环
	go c.run()

	return nil
}

// Stop 停止调度器
func (c *CronScheduler) Stop() error {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	if !c.running {
		return ErrSchedulerNotRunning
	}

	c.running = false
	close(c.stop)
	c.jobWaiter.Wait()

	// 停止远程执行器
	if c.remoteExecutor != nil {
		c.remoteExecutor.Stop()
	}

	// 分布式模式：停止节点注册
	if c.distributed && c.nodeRegistry != nil {
		if err := c.nodeRegistry.Stop(context.Background()); err != nil {
			c.logger.Errorf("节点注销失败: %v", err)
		}
	}

	return nil
}

// IsRunning 检查是否运行中
func (c *CronScheduler) IsRunning() bool {
	c.runningMu.RLock()
	defer c.runningMu.RUnlock()
	return c.running
}

// TriggerManual 手动触发任务
func (c *CronScheduler) TriggerManual(ctx context.Context, jobName string) error {
	if c.IsRunning() {
		return ErrSchedulerAlreadyRunning
	}

	entry, ok := c.GetEntry(jobName)
	if !ok {
		return fmt.Errorf("%w: %s", ErrJobNotFound, jobName)
	}

	// 直接异步执行，不走调度循环
	go c.startJob(entry)

	return nil
}

// GetJobStatus 获取任务状态(优化：直接读取，无通道)
func (c *CronScheduler) GetJobStatus(jobName string) (*models.JobStatus, error) {
	entry, ok := c.GetEntry(jobName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrJobNotFound, jobName)
	}

	next := entry.Next
	prev := entry.Prev
	return &models.JobStatus{
		JobName:         entry.JobName,
		CronSpec:        entry.Config.CronSpec,
		NextExecutionAt: &next,
		LastExecutedAt:  &prev,
		IsRunning:       false,
	}, nil
}

// ListJobs 列出所有任务(优化：直接读取)
func (c *CronScheduler) ListJobs() []string {
	c.mu.RLock()
	jobs := make([]string, 0, len(c.entries))
	for jobName := range c.entries {
		jobs = append(jobs, jobName)
	}
	c.mu.RUnlock()
	return jobs
}

// HealthCheck 健康检查
func (c *CronScheduler) HealthCheck() bool {
	c.runningMu.RLock()
	running := c.running
	c.runningMu.RUnlock()

	if !running {
		return false
	}

	// 检查是否有活跃的任务
	c.mu.RLock()
	hasJobs := len(c.entries) > 0
	c.mu.RUnlock()

	return hasJobs
}

// GetMetricsSnapshot 获取指标快照
func (c *CronScheduler) GetMetricsSnapshot() map[string]interface{} {
	if c.metrics == nil {
		return nil
	}
	return map[string]interface{}{
		"collector": c.metrics,
	}
}

// run 调度循环(最小堆 + 精确唤醒)
func (c *CronScheduler) run() {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		waitDuration := c.processScheduledJobs()

		// 等待定时器或唤醒信号
		if waitDuration < 0 {
			// 没有任务，等待新任务或停止信号
			select {
			case <-c.wakeup:
				continue
			case <-c.stop:
				return
			}
		}

		// 设置定时器
		if timer == nil {
			timer = time.NewTimer(waitDuration)
		} else {
			timer.Reset(waitDuration)
		}

		// 等待定时器或唤醒信号
		select {
		case <-timer.C:
			// 时间到，继续循环执行任务
		case <-c.wakeup:
			// 有新任务或触发，重新计算
			timer.Stop()
		case <-c.stop:
			return
		}
	}
}

// processScheduledJobs 处理到期的任务并返回等待时间
func (c *CronScheduler) processScheduledJobs() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.heap.Len() == 0 {
		return -1 // 无任务信号
	}

	now := time.Now().In(c.location)

	// 执行所有到期的任务
	for c.heap.Len() > 0 && !c.heap[0].Next.After(now) {
		entry := heap.Pop(&c.heap).(*Entry)

		// 异步执行任务
		go c.startJob(entry)

		// 更新时间并重新加入堆
		entry.Prev = entry.Next
		entry.Next = entry.Schedule.Next(now)
		heap.Push(&c.heap, entry)
	}

	// 计算等待时间
	if c.heap.Len() > 0 {
		next := c.heap[0].Next
		waitDuration := next.Sub(now)
		if waitDuration < 0 {
			return 0
		}
		return waitDuration
	}

	return DefaultWaitDuration
}

// startJob 启动任务执行
func (c *CronScheduler) startJob(entry *Entry) {
	c.jobWaiter.Add(1)
	go func() {
		defer c.jobWaiter.Done()

		ctx := context.Background()
		start := time.Now()

		// 记录开始执行
		if c.metrics != nil {
			c.metrics.RecordStart(entry.JobName)
		}

		// 分布式锁
		unlock, acquired := c.AcquireDistributedLock(ctx, entry.JobName, start)
		if !acquired {
			return
		}
		if unlock != nil {
			defer unlock()
		}

		// 创建执行快照
		snapshot := c.CreateSnapshot(ctx, entry.JobName, entry.Config.CronSpec, models.JobTypeCron)

		// 执行任务
		err := c.executeWithProtector(ctx, entry)
		duration := time.Since(start)

		// 更新执行快照
		c.UpdateSnapshot(ctx, snapshot, err, nil)

		// 记录指标
		c.RecordMetrics(entry.JobName, duration, err)
	}()
}

// WithDecorators 链式添加装饰器(流畅 API)
func (c *CronScheduler) WithDecorators(wrappers ...JobWrapper) *CronScheduler {
	c.chain = NewChain(wrappers...)
	return c
}

// AppendDecorator 追加装饰器
func (c *CronScheduler) AppendDecorator(wrapper JobWrapper) *CronScheduler {
	c.chain.Append(wrapper)
	return c
}

// GetMetrics 获取指标统计
func (c *CronScheduler) GetMetrics() *breaker.MetricsCollector {
	if c.metrics == nil {
		return &breaker.MetricsCollector{}
	}
	return c.metrics
}

// GetJobMetrics 获取指定任务的指标
func (c *CronScheduler) GetJobMetrics(jobName string) map[string]interface{} {
	if c.metrics == nil {
		return make(map[string]interface{})
	}

	jobMetrics := c.metrics.GetMetrics(jobName)
	return map[string]interface{}{
		"job_name":            jobMetrics.Name,
		"execution_count":     jobMetrics.ExecutionCount,
		"success_count":       jobMetrics.SuccessCount,
		"failure_count":       jobMetrics.FailureCount,
		"running_count":       jobMetrics.RunningCount,
		"avg_execution_time":  jobMetrics.AvgExecutionTime,
		"max_execution_time":  jobMetrics.MaxExecutionTime,
		"min_execution_time":  jobMetrics.MinExecutionTime,
		"last_execution_time": jobMetrics.LastExecutionTime,
		"success_rate":        jobMetrics.SuccessRate,
	}
}

// UpdateCronSpec 动态更新 Cron 表达式
func (c *CronScheduler) UpdateCronSpec(ctx context.Context, jobName, cronSpec string) error {
	// 验证新的 Cron 表达式
	if _, err := c.parser.Parse(cronSpec); err != nil {
		return fmt.Errorf("%w [%s]: %w", ErrInvalidCronSpec, cronSpec, err)
	}

	// 更新数据库
	if c.jobRepo != nil {
		if err := c.jobRepo.UpdateCronSpec(ctx, jobName, cronSpec); err != nil {
			return fmt.Errorf("%w: %w", ErrUpdateDBConfigFailed, err)
		}

		// 使缓存失效
		if c.schedulerCache != nil {
			c.schedulerCache.InvalidateTaskConfig(ctx, jobName)
		}

		// 分布式模式：通知其他节点
		if c.distributed && c.pubsubClient != nil {
			if err := c.pubsubClient.PublishCronSpecUpdate(ctx, jobName, cronSpec); err != nil {
				c.logger.Warnf("发布 CronSpec 更新事件失败: %v", err)
			}
		}
	}

	// 获取现有条目
	c.mu.RLock()
	entry, ok := c.entries[jobName]
	if !ok {
		c.mu.RUnlock()
		return fmt.Errorf("%w: %s", ErrJobNotFound, jobName)
	}

	// 保存任务和配置信息
	job := entry.Job
	config := entry.Config
	config.CronSpec = cronSpec
	c.mu.RUnlock()

	// 重新注册任务(会自动移除旧条目并添加新条目)
	if err := c.UnregisterJob(jobName); err != nil {
		return fmt.Errorf("%w: %w", ErrRemoveOldJobFailed, err)
	}

	if err := c.RegisterJob(job, config); err != nil {
		return fmt.Errorf("%w: %w", ErrRegisterNewJobFailed, err)
	}

	c.logger.InfoContext(ctx, "✅ 已更新任务 [%s] 的 Cron 表达式: %s", jobName, cronSpec)

	return nil
}

// TriggerScheduled 定时触发(XXL-JOB 风格)
func (c *CronScheduler) TriggerScheduled(ctx context.Context, jobName string, executeTime time.Time, repeatCount int) error {
	entry, ok := c.GetEntry(jobName)
	if !ok {
		return fmt.Errorf("%w: %s", ErrJobNotFound, jobName)
	}

	go func() {
		// 等待到执行时间
		waitDuration := time.Until(executeTime)
		if waitDuration > 0 {
			c.logger.InfoContext(ctx, "⏰ [定时执行] %s 将在 %v 后执行", jobName, waitDuration)
			select {
			case <-time.After(waitDuration):
			case <-ctx.Done():
				return
			}
		}

		// 执行指定次数
		for i := 0; i < repeatCount; i++ {
			c.jobWaiter.Add(1)

			start := time.Now()

			// 创建执行快照
			jobID := fmt.Sprintf(RedisKeyTemplateManualJob, start.Unix())
			snapshot := c.CreateSnapshot(ctx, jobName, jobID, models.JobTypeManual)

			// 执行任务
			err := c.executeWithProtector(ctx, entry)
			c.logger.InfoContext(ctx, "✅ [定时执行] %s 第 %d/%d 次 (耗时: %v), err: %v", jobName, i+1, repeatCount, time.Since(start), err)

			// 更新快照
			c.UpdateSnapshot(ctx, snapshot, err, nil)

			c.jobWaiter.Done()

			if i < repeatCount-1 {
				time.Sleep(1 * time.Second)
			}
		}
	}()

	return nil
}

// subscribeConfigChanges 订阅配置变更事件(分布式场景)
func (c *CronScheduler) subscribeConfigChanges() {
	if c.pubsubClient == nil {
		c.logger.Warn("⚠️ PubSub 客户端未配置，跳过订阅配置变更事件")
		return
	}

	ctx := context.Background()

	// 订阅任务配置更新事件
	if err := c.pubsubClient.SubscribeConfigUpdate(ctx, func(jobName string) {
		c.logger.Infof("[PubSub] 收到任务配置更新事件: %s", jobName)
		c.reloadJobConfig(jobName)
	}); err != nil {
		c.logger.Errorf("订阅任务配置更新事件失败: %v", err)
	}

	// 订阅 CronSpec 更新事件
	if err := c.pubsubClient.SubscribeCronSpecUpdate(ctx, func(jobName, cronSpec string) {
		c.logger.Infof("[PubSub] 收到任务 CronSpec 更新事件: %s -> %s", jobName, cronSpec)
		c.reloadJobConfig(jobName)
	}); err != nil {
		c.logger.Errorf("订阅 CronSpec 更新事件失败: %v", err)
	}

	// 订阅任务删除事件
	if err := c.pubsubClient.SubscribeJobDeleted(ctx, func(jobName string) {
		c.logger.Infof("[PubSub] 收到任务删除事件: %s", jobName)
		// 本地清理任务
		c.mu.Lock()
		delete(c.entries, jobName)
		for i, e := range c.heap {
			if e.JobName == jobName {
				heap.Remove(&c.heap, i)
				break
			}
		}
		c.mu.Unlock()
	}); err != nil {
		c.logger.Errorf("订阅任务删除事件失败: %v", err)
	}

	// 订阅手动执行事件
	if err := c.pubsubClient.SubscribeManualExecute(ctx, func(jobName string) {
		c.logger.Infof("[PubSub] 收到手动执行事件: %s", jobName)
		if err := c.TriggerManual(ctx, jobName); err != nil {
			c.logger.Errorf("手动执行任务失败: %v", err)
		}
	}); err != nil {
		c.logger.Errorf("订阅手动执行事件失败: %v", err)
	}

	// 订阅定时执行事件
	if err := c.pubsubClient.SubscribeScheduledExecute(ctx, func(jobName string, executeTime time.Time, repeatCount int) {
		c.logger.Infof("[PubSub] 收到定时执行事件: %s, 时间=%v, 次数=%d", jobName, executeTime, repeatCount)
		if err := c.TriggerScheduled(ctx, jobName, executeTime, repeatCount); err != nil {
			c.logger.Errorf("定时执行任务失败: %v", err)
		}
	}); err != nil {
		c.logger.Errorf("订阅定时执行事件失败: %v", err)
	}

	c.logger.Info("✅ 已订阅所有 PubSub 事件")
}

// reloadJobConfig 重新加载任务配置(从缓存或数据库)
func (c *CronScheduler) reloadJobConfig(jobName string) {
	ctx := context.Background()

	// 优先从缓存获取
	if c.schedulerCache != nil {
		if config, err := c.schedulerCache.GetTaskConfig(ctx, jobName); err == nil {
			c.logger.InfoContext(ctx, "[缓存] 重新加载任务配置: %s", jobName)
			_ = config
			return
		}
	}

	// 从数据库获取
	if c.jobRepo != nil {
		if config, err := c.jobRepo.GetByJobName(ctx, jobName); err == nil {
			c.logger.InfoContext(ctx, "[数据库] 重新加载任务配置: %s", jobName)
			// 更新缓存
			if c.schedulerCache != nil {
				if taskConfig, err := config.ToTaskCfg(); err == nil && taskConfig != nil {
					c.schedulerCache.SetTaskConfig(ctx, jobName, taskConfig)
				}
			}
		}
	}
}

// GetTaskDetails 获取任务详细信息
func (c *CronScheduler) GetTaskDetails(jobName string) (map[string]interface{}, error) {
	entry, ok := c.GetEntry(jobName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrJobNotFound, jobName)
	}

	details := make(map[string]interface{})
	details["job_name"] = entry.JobName
	details["config"] = entry.Config
	details["schedule"] = entry.Schedule
	details["next_run"] = entry.Next
	details["prev_run"] = entry.Prev

	return details, nil
}

// GetAllTasksDetails 获取所有任务的详细信息
func (c *CronScheduler) GetAllTasksDetails() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]interface{})
	result["total_tasks"] = len(c.entries)
	result["max_concurrency"] = c.maxConcurrency
	result["is_running"] = c.IsRunning()

	tasks := make(map[string]interface{})
	for name := range c.entries {
		if details, err := c.GetTaskDetails(name); err == nil {
			tasks[name] = details
		}
	}
	result["tasks"] = tasks

	return result
}

// ============ 通用辅助方法 ============

// GetEntry 安全获取任务条目（公有方法）
func (c *CronScheduler) GetEntry(jobName string) (*Entry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[jobName]
	return entry, ok
}

// GetNodeID 获取当前节点ID（公有方法）
func (c *CronScheduler) GetNodeID() string {
	if c.distributed && c.nodeRegistry != nil {
		return c.nodeRegistry.nodeID
	}
	return c.nodeID
}

// CreateSnapshot 创建执行快照
func (c *CronScheduler) CreateSnapshot(ctx context.Context, jobName, jobID string, jobType models.JobType) *models.ExecutionSnapshotModel {
	if c.execSnapshotRepo == nil {
		return nil
	}

	traceID := c.idGenerator.GenerateTraceID()
	nodeID := c.GetNodeID()
	snapshot := models.NewExecutionSnapshot(traceID, jobID, jobName, jobType, nodeID)

	if err := c.execSnapshotRepo.Save(ctx, snapshot); err != nil {
		c.logger.Warnf("保存执行快照失败: %v", err)
	}

	return snapshot
}

// UpdateSnapshot 更新执行快照
func (c *CronScheduler) UpdateSnapshot(ctx context.Context, snapshot *models.ExecutionSnapshotModel, err error, data interface{}) {
	if c.execSnapshotRepo == nil || snapshot == nil {
		return
	}

	// 完成快照
	snapshot.Complete(err, data)

	if updateErr := c.execSnapshotRepo.Save(ctx, snapshot); updateErr != nil {
		c.logger.Warnf("更新执行快照失败: %v", updateErr)
	}
}

// executeWithProtector 使用保护器执行任务
func (c *CronScheduler) executeWithProtector(ctx context.Context, entry *Entry) error {
	if protector := c.GetJobProtector(entry.JobName); protector != nil {
		return protector.Execute(ctx, entry.JobName, func() error {
			return entry.WrappedJob.Execute(ctx)
		})
	}
	return entry.WrappedJob.Execute(ctx)
}

// saveTaskConfig 保存任务配置到数据库和缓存
func (c *CronScheduler) saveTaskConfig(ctx context.Context, jobName string, config jobs.TaskCfg) error {
	if c.jobRepo == nil {
		return nil
	}

	// 保存到数据库
	dbConfig := models.FromTaskCfg(jobName, &config)
	if _, err := c.jobRepo.EnsureConfigExists(ctx, dbConfig); err != nil {
		c.logger.Errorf("保存任务配置失败: %s, error=%v", jobName, err)
		return err
	}

	// 更新缓存
	if c.schedulerCache != nil {
		if err := c.schedulerCache.SetTaskConfig(ctx, jobName, config); err != nil {
			c.logger.Warnf("缓存任务配置失败: %s, error=%v", jobName, err)
		}
	}

	return nil
}

// deleteTaskConfig 删除任务配置
func (c *CronScheduler) deleteTaskConfig(ctx context.Context, jobName string) {
	if c.jobRepo == nil {
		return
	}

	if err := c.jobRepo.Delete(ctx, jobName); err != nil {
		c.logger.Errorf("删除任务配置失败: %s, error=%v", jobName, err)
	}

	// 使缓存失效
	if c.schedulerCache != nil {
		c.schedulerCache.InvalidateTaskConfig(ctx, jobName)
	}

	// 分布式模式：通知其他节点
	if c.distributed && c.pubsubClient != nil {
		if err := c.pubsubClient.PublishJobDeleted(ctx, jobName); err != nil {
			c.logger.Warnf("发布任务删除事件失败: %v", err)
		}
	}
}

// RecordMetrics 记录任务执行指标（公有方法）
func (c *CronScheduler) RecordMetrics(jobName string, duration time.Duration, err error) {
	if c == nil || c.metrics == nil {
		return
	}
	if err != nil {
		c.metrics.RecordFailure(jobName, duration)
	} else {
		c.metrics.RecordSuccess(jobName, duration)
	}
}

// AcquireDistributedLock 获取分布式锁（公有方法）
// 返回值：解锁函数, 是否成功获取锁
func (c *CronScheduler) AcquireDistributedLock(ctx context.Context, jobName string, start time.Time) (func(), bool) {
	if !c.distributed || c.schedulerCache == nil {
		return func() {}, true
	}

	lockKey := fmt.Sprintf(RedisKeyTemplateExecLock, jobName, start.Unix())
	lock := c.schedulerCache.GetLockManager().GetLock(lockKey)

	if err := lock.Lock(ctx); err != nil {
		c.logger.Warnf("[分布式锁] 任务 %s 获取锁失败，跳过执行: %v", jobName, err)
		return nil, false
	}

	return func() { lock.Unlock(ctx) }, true
}

// registerNode 注册节点信息
func (c *CronScheduler) registerNode(ctx context.Context) error {
	if !c.distributed || c.nodeRegistry == nil {
		return nil
	}

	if err := c.nodeRegistry.Start(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrNodeRegisterFailed, err)
	}

	if c.schedulerCache != nil {
		nodeInfo := map[string]interface{}{
			"node_id":    c.nodeRegistry.nodeID,
			"started_at": time.Now(),
			"status":     "running",
		}
		if err := c.schedulerCache.SetNodeInfo(ctx, c.nodeRegistry.nodeID, nodeInfo); err != nil {
			c.logger.Warnf("注册节点信息到缓存失败: %v", err)
		}
	}

	// 订阅任务配置变更事件（使用独立的 PubSub 客户端）
	if c.pubsubClient != nil {
		go c.subscribeConfigChanges()
	}

	return nil
}

// loadConfigsFromRepo 从数据库加载任务配置
func (c *CronScheduler) loadConfigsFromRepo(ctx context.Context) {
	if c.jobRepo == nil {
		return
	}

	configs, err := c.jobRepo.ListAll(ctx)
	if err != nil {
		return
	}

	for _, config := range configs {
		if taskCfg, err := config.ToTaskCfg(); err == nil && taskCfg != nil {
			c.logger.InfoContext(ctx, "从数仓加载任务配置: %s, cron=%s", config.JobName, taskCfg.CronSpec)
		}
	}
}
