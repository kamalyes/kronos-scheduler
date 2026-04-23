/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-25 17:16:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 17:16:00
 * @FilePath: \kronos-scheduler\repository\node.go
 * @Description: 节点仓储接口和实现
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// NodeInfo 节点信息
type NodeInfo struct {
	NodeID        string    `json:"node_id"`        // 节点ID
	Host          string    `json:"host"`           // 主机地址
	StartTime     time.Time `json:"start_time"`     // 启动时间
	LastHeartbeat time.Time `json:"last_heartbeat"` // 最后心跳时间
	Status        string    `json:"status"`         // 状态: online, offline, busy
	RunningJobs   []string  `json:"running_jobs"`   // 正在执行的任务
	TotalExecuted int64     `json:"total_executed"` // 总执行次数
	FailureCount  int64     `json:"failure_count"`  // 失败次数
}

// NodeRepository 节点仓储接口
type NodeRepository interface {
	// Register 注册节点
	Register(ctx context.Context, node *NodeInfo) error

	// Unregister 注销节点
	Unregister(ctx context.Context, nodeID string) error

	// UpdateHeartbeat 更新心跳
	UpdateHeartbeat(ctx context.Context, nodeID string) error

	// UpdateStatus 更新节点状态
	UpdateStatus(ctx context.Context, nodeID string, status string) error

	// UpdateRunningJobs 更新正在执行的任务列表
	UpdateRunningJobs(ctx context.Context, nodeID string, jobs []string) error

	// GetNode 获取节点信息
	GetNode(ctx context.Context, nodeID string) (*NodeInfo, error)

	// ListActiveNodes 列出所有活跃节点
	ListActiveNodes(ctx context.Context) ([]*NodeInfo, error)

	// ListAllNodes 列出所有节点（包括离线）
	ListAllNodes(ctx context.Context) ([]*NodeInfo, error)
}

// RedisNodeRepository Redis节点仓储实现
type RedisNodeRepository struct {
	redis     redis.UniversalClient
	nodesKey  string        // 节点Hash键
	expireKey string        // 过期键前缀
	nodeTTL   time.Duration // 节点TTL
}

// NewRedisNodeRepository 创建Redis节点仓储
func NewRedisNodeRepository(redisClient redis.UniversalClient, nodesKey, expireKeyPrefix string, nodeTTL time.Duration) *RedisNodeRepository {
	return &RedisNodeRepository{
		redis:     redisClient,
		nodesKey:  nodesKey,
		expireKey: expireKeyPrefix,
		nodeTTL:   nodeTTL,
	}
}

// Register 注册节点
func (r *RedisNodeRepository) Register(ctx context.Context, node *NodeInfo) error {
	node.LastHeartbeat = time.Now()
	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("序列化节点信息失败: %w", err)
	}

	// 保存节点信息
	if err := r.redis.HSet(ctx, r.nodesKey, node.NodeID, string(data)).Err(); err != nil {
		return fmt.Errorf("保存节点信息失败: %w", err)
	}

	// 设置过期标记
	expireKey := fmt.Sprintf("%s%s", r.expireKey, node.NodeID)
	return r.redis.Set(ctx, expireKey, "1", r.nodeTTL).Err()
}

// Unregister 注销节点
func (r *RedisNodeRepository) Unregister(ctx context.Context, nodeID string) error {
	// 删除节点信息
	if err := r.redis.HDel(ctx, r.nodesKey, nodeID).Err(); err != nil {
		return fmt.Errorf("删除节点信息失败: %w", err)
	}

	// 删除过期标记
	expireKey := fmt.Sprintf("%s%s", r.expireKey, nodeID)
	return r.redis.Del(ctx, expireKey).Err()
}

// UpdateHeartbeat 更新心跳
func (r *RedisNodeRepository) UpdateHeartbeat(ctx context.Context, nodeID string) error {
	// 获取节点信息
	node, err := r.GetNode(ctx, nodeID)
	if err != nil {
		return err
	}

	// 更新心跳时间
	node.LastHeartbeat = time.Now()
	return r.Register(ctx, node)
}

// UpdateStatus 更新节点状态
func (r *RedisNodeRepository) UpdateStatus(ctx context.Context, nodeID string, status string) error {
	node, err := r.GetNode(ctx, nodeID)
	if err != nil {
		return err
	}

	node.Status = status
	return r.Register(ctx, node)
}

// UpdateRunningJobs 更新正在执行的任务列表
func (r *RedisNodeRepository) UpdateRunningJobs(ctx context.Context, nodeID string, jobs []string) error {
	node, err := r.GetNode(ctx, nodeID)
	if err != nil {
		return err
	}

	node.RunningJobs = jobs
	return r.Register(ctx, node)
}

// GetNode 获取节点信息
func (r *RedisNodeRepository) GetNode(ctx context.Context, nodeID string) (*NodeInfo, error) {
	data, err := r.redis.HGet(ctx, r.nodesKey, nodeID).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("node not found: %s", nodeID)
		}
		return nil, fmt.Errorf("获取节点信息失败: %w", err)
	}

	var node NodeInfo
	if err := json.Unmarshal([]byte(data), &node); err != nil {
		return nil, fmt.Errorf("解析节点信息失败: %w", err)
	}

	return &node, nil
}

// ListActiveNodes 列出所有活跃节点
func (r *RedisNodeRepository) ListActiveNodes(ctx context.Context) ([]*NodeInfo, error) {
	nodesData, err := r.redis.HGetAll(ctx, r.nodesKey).Result()
	if err != nil {
		return nil, fmt.Errorf("获取节点列表失败: %w", err)
	}

	var nodes []*NodeInfo
	for nodeID, data := range nodesData {
		var node NodeInfo
		if err := json.Unmarshal([]byte(data), &node); err != nil {
			continue
		}

		// 检查过期标记键是否存在(心跳是否还在)
		expireKey := fmt.Sprintf("%s%s", r.expireKey, nodeID)
		exists, _ := r.redis.Exists(ctx, expireKey).Result()
		if exists > 0 {
			nodes = append(nodes, &node)
		} else {
			// 节点已过期，从 Hash 中清理
			r.redis.HDel(ctx, r.nodesKey, nodeID)
		}
	}

	return nodes, nil
}

// ListAllNodes 列出所有节点（包括离线）
func (r *RedisNodeRepository) ListAllNodes(ctx context.Context) ([]*NodeInfo, error) {
	nodesData, err := r.redis.HGetAll(ctx, r.nodesKey).Result()
	if err != nil {
		return nil, fmt.Errorf("获取节点列表失败: %w", err)
	}

	var nodes []*NodeInfo
	for _, data := range nodesData {
		var node NodeInfo
		if err := json.Unmarshal([]byte(data), &node); err != nil {
			continue
		}
		nodes = append(nodes, &node)
	}

	return nodes, nil
}
