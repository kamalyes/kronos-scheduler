/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 17:17:18
 * @FilePath: \kronos-scheduler\repository\cache.go
 * @Description: 缓存仓储实现(基于 Redis)
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/kamalyes/go-cachex"
	"github.com/redis/go-redis/v9"
)

const (
	// JobConfigCacheKeyPrefix Job配置缓存key前缀
	JobConfigCacheKeyPrefix = "job:config:"
	// DefaultCacheTTL 默认缓存过期时间
	DefaultCacheTTL = 5 * time.Minute
)

// RedisCacheRepository Redis 缓存仓储实现
type RedisCacheRepository struct {
	redis redis.UniversalClient
}

// NewRedisCacheRepository 创建 Redis 缓存仓储
func NewRedisCacheRepository(redisClient redis.UniversalClient) CacheRepository {
	return &RedisCacheRepository{
		redis: redisClient,
	}
}

func (r *RedisCacheRepository) Get(ctx context.Context, key string) (interface{}, error) {
	return r.redis.Get(ctx, key).Result()
}

func (r *RedisCacheRepository) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return r.redis.Set(ctx, key, value, ttl).Err()
}

func (r *RedisCacheRepository) Delete(ctx context.Context, keys ...string) error {
	return r.redis.Del(ctx, keys...).Err()
}

func (r *RedisCacheRepository) InvalidateJobConfig(ctx context.Context, jobName string) error {
	key := fmt.Sprintf("%s%s", JobConfigCacheKeyPrefix, jobName)
	return r.redis.Del(ctx, key).Err()
}

// SchedulerCache Scheduler 缓存封装
type SchedulerCache struct {
	redis     redis.UniversalClient // Redis 客户端
	nodeCache *cachex.SmartCache    // 节点信息缓存
	jobCache  *cachex.SmartCache    // 任务配置缓存
	lockMgr   *cachex.LockManager
	pubsub    *cachex.PubSub
}

// NewSchedulerCache 创建 Scheduler 缓存
func NewSchedulerCache(redisClient redis.UniversalClient) *SchedulerCache {
	namespace := "scheduler"
	// 节点信息缓存(带热key保护和自动刷新)
	nodeCache := cachex.NewCacheBuilder(redisClient, namespace).
		WithKeyPattern("node:{node_id}").
		WithTTL(30 * time.Second).
		WithHotKey().
		WithRefreshThreshold(0.3). // 剩余30%时刷新
		Build()

	// 任务配置缓存(带分布式锁防止击穿)
	jobCache := cachex.NewCacheBuilder(redisClient, namespace).
		WithKeyPattern("job:config:{job_name}").
		WithTTL(5 * time.Minute).
		WithLock(3 * time.Second).
		WithPubSub().
		Build()

	// 分布式锁管理器(用于任务执行)
	lockMgr := cachex.NewLockManager(redisClient, cachex.LockConfig{
		TTL:              30 * time.Second,
		RetryInterval:    100 * time.Millisecond,
		MaxRetries:       10,
		Namespace:        namespace + ":job",
		EnableWatchdog:   true,
		WatchdogInterval: 10 * time.Second,
	})

	// PubSub(用于分布式事件通知)
	pubsubInst := cachex.NewPubSub(redisClient, cachex.PubSubConfig{
		Namespace:  namespace,
		MaxRetries: 3,
		RetryDelay: 100 * time.Millisecond,
		BufferSize: 100,
	})

	return &SchedulerCache{
		redis:     redisClient,
		nodeCache: nodeCache,
		jobCache:  jobCache,
		lockMgr:   lockMgr,
		pubsub:    pubsubInst,
	}
}

// GetRedis 获取 Redis 客户端
func (sc *SchedulerCache) GetRedis() redis.UniversalClient {
	return sc.redis
}

// GetLockManager 获取锁管理器
func (sc *SchedulerCache) GetLockManager() *cachex.LockManager {
	return sc.lockMgr
}

// GetPubSub 获取 PubSub
func (sc *SchedulerCache) GetPubSub() *cachex.PubSub {
	return sc.pubsub
}

// SetNodeInfo 设置节点信息
func (sc *SchedulerCache) SetNodeInfo(ctx context.Context, nodeID string, info interface{}) error {
	return sc.nodeCache.Set(ctx, nodeID, info)
}

// GetNodeInfo 获取节点信息
func (sc *SchedulerCache) GetNodeInfo(ctx context.Context, nodeID string) (interface{}, error) {
	return sc.nodeCache.Get(ctx, nodeID)
}

// SetTaskConfig 设置任务配置
func (sc *SchedulerCache) SetTaskConfig(ctx context.Context, jobName string, config interface{}) error {
	return sc.jobCache.Set(ctx, jobName, config)
}

// GetTaskConfig 获取任务配置
func (sc *SchedulerCache) GetTaskConfig(ctx context.Context, jobName string) (interface{}, error) {
	return sc.jobCache.Get(ctx, jobName)
}

// InvalidateTaskConfig 使任务配置缓存失效
func (sc *SchedulerCache) InvalidateTaskConfig(ctx context.Context, jobName string) error {
	return sc.jobCache.Delete(ctx, jobName)
}
