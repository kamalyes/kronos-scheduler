/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-23 21:20:00
 * @FilePath: \kronos-scheduler\job\sharding.go
 * @Description: 任务分片执行 - Map-Reduce风格
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package job

import (
	"context"
	"fmt"
	"sync"
)

// ShardingContext 分片上下文
type ShardingContext struct {
	ShardIndex     int    // 当前分片索引
	TotalShards    int    // 总分片数
	ShardParameter string // 分片参数
	JobName        string // 任务名称
	JobParameter   string // 任务参数
}

// ShardingJob 支持分片的任务接口
type ShardingJob interface {
	Job

	// FetchShardingItems 获取分片项(返回要处理的数据列表)
	FetchShardingItems(ctx context.Context, sharding ShardingContext) ([]interface{}, error)

	// ProcessShardingItem 处理单个分片项
	ProcessShardingItem(ctx context.Context, item interface{}, sharding ShardingContext) error
}

// MapReduceJob Map-Reduce风格的任务
type MapReduceJob interface {
	Job

	// Map 映射阶段 - 将数据分片并处理
	Map(ctx context.Context, data interface{}) ([]interface{}, error)

	// Reduce 归约阶段 - 合并结果
	Reduce(ctx context.Context, results []interface{}) (interface{}, error)
}

// ShardingExecutor 分片执行器
type ShardingExecutor struct {
	job             ShardingJob
	totalShards     int
	parallelWorkers int
	mu              sync.Mutex
}

// NewShardingExecutor 创建分片执行器
func NewShardingExecutor(job ShardingJob, totalShards int) *ShardingExecutor {
	return &ShardingExecutor{
		job:             job,
		totalShards:     totalShards,
		parallelWorkers: totalShards,
	}
}

// Execute 执行分片任务
func (se *ShardingExecutor) Execute(ctx context.Context) error {
	var wg sync.WaitGroup
	errChan := make(chan error, se.totalShards)

	// 为每个分片启动goroutine
	for shardIndex := 0; shardIndex < se.totalShards; shardIndex++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			shardCtx := ShardingContext{
				ShardIndex:  index,
				TotalShards: se.totalShards,
				JobName:     se.job.Name(),
			}

			if err := se.executeSharding(ctx, shardCtx); err != nil {
				errChan <- fmt.Errorf("shard %d failed: %w", index, err)
			}
		}(shardIndex)
	}

	// 等待所有分片完成
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// 收集错误
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("sharding execution failed: %v", errors)
	}

	return nil
}

// executeSharding 执行单个分片
func (se *ShardingExecutor) executeSharding(ctx context.Context, shardCtx ShardingContext) error {
	// 获取分片数据
	items, err := se.job.FetchShardingItems(ctx, shardCtx)
	if err != nil {
		return fmt.Errorf("fetch sharding items failed: %w", err)
	}

	// 处理每个分片项
	for _, item := range items {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := se.job.ProcessShardingItem(ctx, item, shardCtx); err != nil {
				return fmt.Errorf("process item failed: %w", err)
			}
		}
	}

	return nil
}

// SetParallelWorkers 设置并行worker数量
func (se *ShardingExecutor) SetParallelWorkers(workers int) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.parallelWorkers = workers
}

// MapReduceExecutor Map-Reduce执行器
type MapReduceExecutor struct {
	job     MapReduceJob
	mappers int
}

// NewMapReduceExecutor 创建Map-Reduce执行器
func NewMapReduceExecutor(job MapReduceJob, mappers int) *MapReduceExecutor {
	return &MapReduceExecutor{
		job:     job,
		mappers: mappers,
	}
}

// Execute 执行Map-Reduce任务
func (me *MapReduceExecutor) Execute(ctx context.Context, data interface{}) (interface{}, error) {
	// Map阶段
	mapResults, err := me.job.Map(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("map phase failed: %w", err)
	}

	// Reduce阶段
	result, err := me.job.Reduce(ctx, mapResults)
	if err != nil {
		return nil, fmt.Errorf("reduce phase failed: %w", err)
	}

	return result, nil
}

// BaseShardingJob 基础分片任务实现
type BaseShardingJob struct {
	*BaseJob
	fetchFunc   func(ctx context.Context, sharding ShardingContext) ([]interface{}, error)
	processFunc func(ctx context.Context, item interface{}, sharding ShardingContext) error
}

// NewBaseShardingJob 创建基础分片任务
func NewBaseShardingJob(
	name, description string,
	fetchFunc func(ctx context.Context, sharding ShardingContext) ([]interface{}, error),
	processFunc func(ctx context.Context, item interface{}, sharding ShardingContext) error,
) *BaseShardingJob {
	return &BaseShardingJob{
		BaseJob:     NewBaseJob(name, description),
		fetchFunc:   fetchFunc,
		processFunc: processFunc,
	}
}

// FetchShardingItems 实现ShardingJob接口
func (b *BaseShardingJob) FetchShardingItems(ctx context.Context, sharding ShardingContext) ([]interface{}, error) {
	return b.fetchFunc(ctx, sharding)
}

// ProcessShardingItem 实现ShardingJob接口
func (b *BaseShardingJob) ProcessShardingItem(ctx context.Context, item interface{}, sharding ShardingContext) error {
	return b.processFunc(ctx, item, sharding)
}

// Execute 实现Job接口
func (b *BaseShardingJob) Execute(ctx context.Context) error {
	// 默认单分片执行
	executor := NewShardingExecutor(b, 1)
	return executor.Execute(ctx)
}
