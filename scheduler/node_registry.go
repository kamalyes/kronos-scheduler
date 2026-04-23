/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-23 17:54:15
 * @FilePath: \kronos-scheduler\scheduler\node_registry.go
 * @Description: 节点注册器(使用 go-cachex 实现)
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kamalyes/kronos-scheduler/logger"
	"github.com/kamalyes/kronos-scheduler/repository"
	"github.com/redis/go-redis/v9"
)

// NodeInfo 节点信息
type NodeInfo struct {
	NodeID        string    `json:"node_id"`        // 节点ID
	Host          string    `json:"host"`           // 主机地址
	StartTime     time.Time `json:"start_time"`     // 启动时间
	LastHeartbeat time.Time `json:"last_heartbeat"` // 最后心跳时间
	RunningJobs   []string  `json:"running_jobs"`   // 正在执行的任务列表
}

// NodeRegistry 节点注册器(基于 Redis)
type NodeRegistry struct {
	redis             redis.UniversalClient      // 直接使用 Redis 客户端
	cache             *repository.SchedulerCache // 智能缓存
	logger            logger.Logger
	nodeID            string
	host              string
	heartbeatInterval time.Duration
	nodeExpiration    time.Duration
	stopChan          chan struct{}
}

// NewNodeRegistry 创建节点注册器
func NewNodeRegistry(cache *repository.SchedulerCache, log logger.Logger, nodeID, host string) *NodeRegistry {
	var redisClient redis.UniversalClient
	if cache != nil {
		redisClient = cache.GetRedis()
	}

	return &NodeRegistry{
		redis:             redisClient,
		cache:             cache,
		logger:            log,
		nodeID:            nodeID,
		host:              host,
		heartbeatInterval: 10 * time.Second, // 每 10 秒心跳
		nodeExpiration:    30 * time.Second, // 30 秒过期
		stopChan:          make(chan struct{}),
	}
}

// Start 启动节点注册和心跳
func (n *NodeRegistry) Start(ctx context.Context) error {
	// 初始注册
	if err := n.register(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrNodeRegisterFailed, err)
	}

	n.logger.Info("🌐 节点已注册: %s (%s)", n.nodeID, n.host)

	// 启动心跳
	go n.heartbeat(ctx)

	return nil
}

// Stop 停止节点
func (n *NodeRegistry) Stop(ctx context.Context) error {
	close(n.stopChan)

	// 从 Hash 中删除节点信息
	if err := n.redis.HDel(ctx, RedisKeySchedulerNodes, n.nodeID).Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrNodeUnregisterFailed, err)
	}

	n.logger.Info("🔌 节点已注销: %s", n.nodeID)
	return nil
}

// UpdateRunningJobs 更新正在执行的任务列表
func (n *NodeRegistry) UpdateRunningJobs(ctx context.Context, jobs []string) error {
	info := &NodeInfo{
		NodeID:        n.nodeID,
		Host:          n.host,
		StartTime:     time.Now(),
		LastHeartbeat: time.Now(),
		RunningJobs:   jobs,
	}

	// 使用 SmartCache 存储节点信息（自动处理序列化、TTL、热key保护）
	if n.cache != nil {
		return n.cache.SetNodeInfo(ctx, n.nodeID, info)
	}

	// 降级：直接使用 Redis（兼容未配置 cache 的场景）
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNodeInfoSerializeFailed, err)
	}

	if err := n.redis.HSet(ctx, RedisKeySchedulerNodes, n.nodeID, string(data)).Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrNodeInfoUpdateFailed, err)
	}

	expireKey := fmt.Sprintf("%s%s", RedisKeyNodeExpirePrefix, n.nodeID)
	return n.redis.Set(ctx, expireKey, "1", n.nodeExpiration).Err()
}

// GetActiveNodes 获取所有活跃节点
func (n *NodeRegistry) GetActiveNodes(ctx context.Context) ([]*NodeInfo, error) {
	// 优先使用 SmartCache（自动处理反序列化、缓存预热）
	if n.cache != nil {
		// SmartCache 的 TTL 机制会自动过滤过期节点
		// 这里只需要获取当前节点信息
		if data, err := n.cache.GetNodeInfo(ctx, n.nodeID); err == nil {
			if node, ok := data.(*NodeInfo); ok {
				return []*NodeInfo{node}, nil
			}
		}
		// 如果当前节点信息获取失败，返回空列表
		return []*NodeInfo{}, nil
	}

	// 降级：使用原始 Redis 操作
	nodesData, err := n.redis.HGetAll(ctx, RedisKeySchedulerNodes).Result()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrGetNodeListFailed, err)
	}

	var nodes []*NodeInfo
	for nodeID, data := range nodesData {
		var node NodeInfo
		if err := json.Unmarshal([]byte(data), &node); err != nil {
			n.logger.Warn("解析节点信息失败 [%s]: %v", nodeID, err)
			continue
		}

		expireKey := fmt.Sprintf("%s%s", RedisKeyNodeExpirePrefix, nodeID)
		exists, _ := n.redis.Exists(ctx, expireKey).Result()
		if exists > 0 {
			nodes = append(nodes, &node)
		} else {
			n.redis.HDel(ctx, RedisKeySchedulerNodes, nodeID)
		}
	}

	return nodes, nil
}

// register 注册节点
func (n *NodeRegistry) register(ctx context.Context) error {
	return n.UpdateRunningJobs(ctx, []string{})
}

// heartbeat 心跳循环
func (n *NodeRegistry) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := n.register(ctx); err != nil {
				n.logger.Error("心跳更新失败: %v", err)
			}
		case <-n.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}
