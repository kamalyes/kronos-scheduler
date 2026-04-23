/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-25 15:30:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 15:30:00
 * @FilePath: \kronos-scheduler\scheduler\worker_pool.go
 * @Description: 工作池 - 提供并发任务执行和资源管理
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerPool 工作池
type WorkerPool struct {
	// 配置
	workerCount int // 工作协程数量
	queueSize   int // 任务队列大小

	// 任务通道
	taskQueue   chan Task       // 任务队列
	resultQueue chan TaskResult // 结果队列

	// 控制
	ctx    context.Context    // 上下文
	cancel context.CancelFunc // 取消函数
	wg     sync.WaitGroup     // 等待组

	// 状态
	running atomic.Bool // 是否运行中
	paused  atomic.Bool // 是否暂停

	// 统计
	stats *PoolStats // 统计信息

	// 钩子
	onTaskStart func(task Task)
	onTaskEnd   func(task Task, result TaskResult)
	onError     func(task Task, err error)
}

// Task 任务接口
type Task interface {
	ID() string
	Execute(ctx context.Context) (interface{}, error)
	Priority() int // 优先级(可选)
}

// TaskResult 任务结果
type TaskResult struct {
	TaskID    string
	Result    interface{}
	Error     error
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
}

// PoolStats 工作池统计信息
type PoolStats struct {
	mu sync.RWMutex

	// 计数器
	TotalSubmitted int64 // 总提交数
	TotalCompleted int64 // 总完成数
	TotalFailed    int64 // 总失败数
	ActiveWorkers  int32 // 活跃工作者数
	QueuedTasks    int32 // 队列中的任务数

	// 时间统计
	TotalDuration time.Duration // 总执行时间
	AvgDuration   time.Duration // 平均执行时间
	MaxDuration   time.Duration // 最大执行时间
	MinDuration   time.Duration // 最小执行时间

	// 吞吐量
	StartTime    time.Time // 启动时间
	LastTaskTime time.Time // 最后任务时间
}

// NewWorkerPool 创建工作池
func NewWorkerPool(workerCount, queueSize int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		workerCount: workerCount,
		queueSize:   queueSize,
		taskQueue:   make(chan Task, queueSize),
		resultQueue: make(chan TaskResult, queueSize),
		ctx:         ctx,
		cancel:      cancel,
		stats:       &PoolStats{StartTime: time.Now()},
	}
}

// Start 启动工作池
func (wp *WorkerPool) Start() error {
	if wp.running.Load() {
		return fmt.Errorf("worker pool is already running")
	}

	wp.running.Store(true)

	// 启动工作协程
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	return nil
}

// Stop 停止工作池
func (wp *WorkerPool) Stop(timeout time.Duration) error {
	if !wp.running.Load() {
		return ErrWorkerPoolNotRunning
	}

	wp.running.Store(false)

	// 关闭任务队列
	close(wp.taskQueue)

	// 等待所有任务完成或超时
	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 正常完成
	case <-time.After(timeout):
		// 超时，强制取消
		wp.cancel()
		return fmt.Errorf("%w after %v", ErrWorkerPoolStopTimeout, timeout)
	}

	close(wp.resultQueue)
	return nil
}

// Submit 提交任务
func (wp *WorkerPool) Submit(task Task) error {
	if !wp.running.Load() {
		return ErrWorkerPoolNotRunning
	}

	if wp.paused.Load() {
		return fmt.Errorf("worker pool is paused")
	}

	select {
	case wp.taskQueue <- task:
		atomic.AddInt64(&wp.stats.TotalSubmitted, 1)
		atomic.AddInt32(&wp.stats.QueuedTasks, 1)
		return nil
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	default:
		return ErrTaskQueueFull
	}
}

// SubmitWithContext 带超时的提交任务
func (wp *WorkerPool) SubmitWithContext(ctx context.Context, task Task) error {
	if !wp.running.Load() {
		return ErrWorkerPoolNotRunning
	}

	if wp.paused.Load() {
		return fmt.Errorf("worker pool is paused")
	}

	select {
	case wp.taskQueue <- task:
		atomic.AddInt64(&wp.stats.TotalSubmitted, 1)
		atomic.AddInt32(&wp.stats.QueuedTasks, 1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-wp.ctx.Done():
		return ErrWorkerPoolShuttingDown
	}
}

// worker 工作协程
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case task, ok := <-wp.taskQueue:
			if !ok {
				// 任务队列已关闭
				return
			}

			// 暂停检查
			for wp.paused.Load() {
				time.Sleep(100 * time.Millisecond)
				if !wp.running.Load() {
					return
				}
			}

			// 执行任务
			wp.executeTask(task)

		case <-wp.ctx.Done():
			return
		}
	}
}

// executeTask 执行任务
func (wp *WorkerPool) executeTask(task Task) {
	atomic.AddInt32(&wp.stats.ActiveWorkers, 1)
	atomic.AddInt32(&wp.stats.QueuedTasks, -1)
	defer atomic.AddInt32(&wp.stats.ActiveWorkers, -1)

	startTime := time.Now()

	// 任务开始钩子
	if wp.onTaskStart != nil {
		wp.onTaskStart(task)
	}

	// 执行任务
	result, err := task.Execute(wp.ctx)

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	// 更新统计
	wp.updateStats(duration, err == nil)

	// 构建结果
	taskResult := TaskResult{
		TaskID:    task.ID(),
		Result:    result,
		Error:     err,
		StartTime: startTime,
		EndTime:   endTime,
		Duration:  duration,
	}

	// 发送结果
	select {
	case wp.resultQueue <- taskResult:
	default:
		// 结果队列已满，丢弃结果
	}

	// 任务结束钩子
	if wp.onTaskEnd != nil {
		wp.onTaskEnd(task, taskResult)
	}

	// 错误钩子
	if err != nil && wp.onError != nil {
		wp.onError(task, err)
	}
}

// updateStats 更新统计信息
func (wp *WorkerPool) updateStats(duration time.Duration, success bool) {
	wp.stats.mu.Lock()
	defer wp.stats.mu.Unlock()

	if success {
		atomic.AddInt64(&wp.stats.TotalCompleted, 1)
	} else {
		atomic.AddInt64(&wp.stats.TotalFailed, 1)
	}

	wp.stats.LastTaskTime = time.Now()
	wp.stats.TotalDuration += duration

	// 更新平均时间
	completed := atomic.LoadInt64(&wp.stats.TotalCompleted)
	if completed > 0 {
		wp.stats.AvgDuration = wp.stats.TotalDuration / time.Duration(completed)
	}

	// 更新最大/最小时间
	if duration > wp.stats.MaxDuration {
		wp.stats.MaxDuration = duration
	}
	if wp.stats.MinDuration == 0 || duration < wp.stats.MinDuration {
		wp.stats.MinDuration = duration
	}
}

// Pause 暂停工作池
func (wp *WorkerPool) Pause() {
	wp.paused.Store(true)
}

// Resume 恢复工作池
func (wp *WorkerPool) Resume() {
	wp.paused.Store(false)
}

// GetStats 获取统计信息
func (wp *WorkerPool) GetStats() PoolStats {
	wp.stats.mu.RLock()
	defer wp.stats.mu.RUnlock()

	return PoolStats{
		TotalSubmitted: atomic.LoadInt64(&wp.stats.TotalSubmitted),
		TotalCompleted: atomic.LoadInt64(&wp.stats.TotalCompleted),
		TotalFailed:    atomic.LoadInt64(&wp.stats.TotalFailed),
		ActiveWorkers:  atomic.LoadInt32(&wp.stats.ActiveWorkers),
		QueuedTasks:    atomic.LoadInt32(&wp.stats.QueuedTasks),
		TotalDuration:  wp.stats.TotalDuration,
		AvgDuration:    wp.stats.AvgDuration,
		MaxDuration:    wp.stats.MaxDuration,
		MinDuration:    wp.stats.MinDuration,
		StartTime:      wp.stats.StartTime,
		LastTaskTime:   wp.stats.LastTaskTime,
	}
}

// GetThroughput 获取吞吐量(任务数/秒)
func (wp *WorkerPool) GetThroughput() float64 {
	stats := wp.GetStats()
	elapsed := time.Since(stats.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(stats.TotalCompleted) / elapsed
}

// GetQueueUtilization 获取队列利用率(0-1)
func (wp *WorkerPool) GetQueueUtilization() float64 {
	queuedTasks := atomic.LoadInt32(&wp.stats.QueuedTasks)
	return float64(queuedTasks) / float64(wp.queueSize)
}

// GetWorkerUtilization 获取工作者利用率(0-1)
func (wp *WorkerPool) GetWorkerUtilization() float64 {
	activeWorkers := atomic.LoadInt32(&wp.stats.ActiveWorkers)
	return float64(activeWorkers) / float64(wp.workerCount)
}

// SetOnTaskStart 设置任务开始钩子
func (wp *WorkerPool) SetOnTaskStart(fn func(task Task)) {
	wp.onTaskStart = fn
}

// SetOnTaskEnd 设置任务结束钩子
func (wp *WorkerPool) SetOnTaskEnd(fn func(task Task, result TaskResult)) {
	wp.onTaskEnd = fn
}

// SetOnError 设置错误钩子
func (wp *WorkerPool) SetOnError(fn func(task Task, err error)) {
	wp.onError = fn
}

// GetResultQueue 获取结果队列(只读)
func (wp *WorkerPool) GetResultQueue() <-chan TaskResult {
	return wp.resultQueue
}

// IsRunning 判断是否运行中
func (wp *WorkerPool) IsRunning() bool {
	return wp.running.Load()
}

// IsPaused 判断是否暂停
func (wp *WorkerPool) IsPaused() bool {
	return wp.paused.Load()
}

// Resize 动态调整工作池大小
func (wp *WorkerPool) Resize(newWorkerCount int) error {
	if !wp.running.Load() {
		wp.workerCount = newWorkerCount
		return nil
	}

	// 运行时调整
	currentCount := wp.workerCount
	diff := newWorkerCount - currentCount

	if diff > 0 {
		// 增加工作者
		for i := 0; i < diff; i++ {
			wp.wg.Add(1)
			go wp.worker(currentCount + i)
		}
	} else if diff < 0 {
		// 减少工作者(通过暂停和超时机制)
		// 这里简化处理，实际可能需要更复杂的逻辑
		return fmt.Errorf("减少工作者数量需要重启工作池")
	}

	wp.workerCount = newWorkerCount
	return nil
}
