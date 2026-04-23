/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-25 15:20:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 15:20:00
 * @FilePath: \kronos-scheduler\models\throttle_config.go
 * @Description: 节流配置 - 提供任务执行的流量控制和限速功能
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package models

import (
	"fmt"
	"time"
)

// ThrottleConfig 节流配置
type ThrottleConfig struct {
	// CooldownDuration 冷却时间 - 任务执行后需要等待的最小时间间隔
	CooldownDuration time.Duration `json:"cooldown_duration"`

	// SleepDuration 睡眠时间 - 任务执行后强制休眠的时间
	SleepDuration time.Duration `json:"sleep_duration"`

	// MaxConcurrent 最大并发数 - 同一任务允许同时执行的最大实例数
	MaxConcurrent int `json:"max_concurrent"`

	// RateLimit 速率限制 - 每秒允许执行的最大次数(0表示无限制)
	RateLimit int `json:"rate_limit"`

	// BurstSize 突发大小 - 允许的突发请求数量
	BurstSize int `json:"burst_size"`

	// MaxQueueSize 最大队列大小 - 等待执行的任务队列最大长度
	MaxQueueSize int `json:"max_queue_size"`

	// QueueTimeout 队列超时 - 任务在队列中的最大等待时间
	QueueTimeout time.Duration `json:"queue_timeout"`

	// MaxRetries 最大重试次数
	MaxRetries int `json:"max_retries"`

	// RetryDelay 重试延迟 - 每次重试之间的等待时间
	RetryDelay time.Duration `json:"retry_delay"`

	// RetryBackoff 重试退避策略 - 是否启用指数退避
	RetryBackoff bool `json:"retry_backoff"`

	// MaxRetryDelay 最大重试延迟 - 退避策略的最大延迟时间
	MaxRetryDelay time.Duration `json:"max_retry_delay"`
}

// NewThrottleConfig 创建默认节流配置
func NewThrottleConfig() *ThrottleConfig {
	return &ThrottleConfig{
		CooldownDuration: 0,
		SleepDuration:    0,
		MaxConcurrent:    5,
		RateLimit:        0,
		BurstSize:        10,
		MaxQueueSize:     100,
		QueueTimeout:     5 * time.Minute,
		MaxRetries:       3,
		RetryDelay:       5 * time.Second,
		RetryBackoff:     true,
		MaxRetryDelay:    1 * time.Minute,
	}
}

// WithCooldown 设置冷却时间
func (c *ThrottleConfig) WithCooldown(duration time.Duration) *ThrottleConfig {
	c.CooldownDuration = duration
	return c
}

// WithSleep 设置睡眠时间
func (c *ThrottleConfig) WithSleep(duration time.Duration) *ThrottleConfig {
	c.SleepDuration = duration
	return c
}

// WithMaxConcurrent 设置最大并发数
func (c *ThrottleConfig) WithMaxConcurrent(max int) *ThrottleConfig {
	c.MaxConcurrent = max
	return c
}

// WithRateLimit 设置速率限制(每秒最大执行次数)
func (c *ThrottleConfig) WithRateLimit(limit int) *ThrottleConfig {
	c.RateLimit = limit
	return c
}

// WithBurstSize 设置突发大小
func (c *ThrottleConfig) WithBurstSize(size int) *ThrottleConfig {
	c.BurstSize = size
	return c
}

// WithMaxQueueSize 设置最大队列大小
func (c *ThrottleConfig) WithMaxQueueSize(size int) *ThrottleConfig {
	c.MaxQueueSize = size
	return c
}

// WithQueueTimeout 设置队列超时
func (c *ThrottleConfig) WithQueueTimeout(timeout time.Duration) *ThrottleConfig {
	c.QueueTimeout = timeout
	return c
}

// WithRetry 设置重试配置
func (c *ThrottleConfig) WithRetry(maxRetries int, retryDelay time.Duration, backoff bool) *ThrottleConfig {
	c.MaxRetries = maxRetries
	c.RetryDelay = retryDelay
	c.RetryBackoff = backoff
	return c
}

// WithMaxRetryDelay 设置最大重试延迟
func (c *ThrottleConfig) WithMaxRetryDelay(duration time.Duration) *ThrottleConfig {
	c.MaxRetryDelay = duration
	return c
}

// Validate 验证配置有效性
func (c *ThrottleConfig) Validate() error {
	if c.CooldownDuration < 0 {
		return fmt.Errorf("cooldown duration cannot be negative: %v", c.CooldownDuration)
	}

	if c.SleepDuration < 0 {
		return fmt.Errorf("sleep duration cannot be negative: %v", c.SleepDuration)
	}

	if c.MaxConcurrent <= 0 {
		return fmt.Errorf("max concurrent must be positive: %d", c.MaxConcurrent)
	}

	if c.RateLimit < 0 {
		return fmt.Errorf("rate limit cannot be negative: %d", c.RateLimit)
	}

	if c.BurstSize < 0 {
		return fmt.Errorf("burst size cannot be negative: %d", c.BurstSize)
	}

	if c.MaxQueueSize < 0 {
		return fmt.Errorf("max queue size cannot be negative: %d", c.MaxQueueSize)
	}

	if c.QueueTimeout < 0 {
		return fmt.Errorf("queue timeout cannot be negative: %v", c.QueueTimeout)
	}

	if c.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative: %d", c.MaxRetries)
	}

	if c.RetryDelay < 0 {
		return fmt.Errorf("retry delay cannot be negative: %v", c.RetryDelay)
	}

	if c.MaxRetryDelay < 0 {
		return fmt.Errorf("max retry delay cannot be negative: %v", c.MaxRetryDelay)
	}

	if c.RetryBackoff && c.MaxRetryDelay < c.RetryDelay {
		return fmt.Errorf("max retry delay must be greater than or equal to retry delay")
	}

	return nil
}

// CalculateRetryDelay 计算重试延迟时间
func (c *ThrottleConfig) CalculateRetryDelay(attempt int) time.Duration {
	if !c.RetryBackoff {
		return c.RetryDelay
	}

	// 指数退避：delay * 2^attempt
	delay := c.RetryDelay * time.Duration(1<<uint(attempt))

	// 限制最大延迟
	if delay > c.MaxRetryDelay {
		delay = c.MaxRetryDelay
	}

	return delay
}

// ShouldRetry 判断是否应该重试
func (c *ThrottleConfig) ShouldRetry(attempt int) bool {
	return attempt < c.MaxRetries
}

// HasCooldown 判断是否配置了冷却时间
func (c *ThrottleConfig) HasCooldown() bool {
	return c.CooldownDuration > 0
}

// HasSleep 判断是否配置了睡眠时间
func (c *ThrottleConfig) HasSleep() bool {
	return c.SleepDuration > 0
}

// HasRateLimit 判断是否配置了速率限制
func (c *ThrottleConfig) HasRateLimit() bool {
	return c.RateLimit > 0
}

// HasQueueTimeout 判断是否配置了队列超时
func (c *ThrottleConfig) HasQueueTimeout() bool {
	return c.QueueTimeout > 0
}

// String 返回配置的字符串表示
func (c *ThrottleConfig) String() string {
	return fmt.Sprintf(
		"ThrottleConfig{Cooldown=%v, Sleep=%v, MaxConcurrent=%d, RateLimit=%d/s, BurstSize=%d, MaxQueue=%d, QueueTimeout=%v, MaxRetries=%d, RetryDelay=%v, Backoff=%v}",
		c.CooldownDuration,
		c.SleepDuration,
		c.MaxConcurrent,
		c.RateLimit,
		c.BurstSize,
		c.MaxQueueSize,
		c.QueueTimeout,
		c.MaxRetries,
		c.RetryDelay,
		c.RetryBackoff,
	)
}

// ThrottleMode 节流模式
type ThrottleMode int

const (
	// ThrottleModeNone 无节流
	ThrottleModeNone ThrottleMode = iota
	// ThrottleModeLight 轻度节流(仅基本限制)
	ThrottleModeLight
	// ThrottleModeModerate 中度节流(标准限制)
	ThrottleModeModerate
	// ThrottleModeStrict 严格节流(严格限制)
	ThrottleModeStrict
)

// NewThrottleConfigWithMode 根据模式创建节流配置
func NewThrottleConfigWithMode(mode ThrottleMode) *ThrottleConfig {
	config := NewThrottleConfig()

	switch mode {
	case ThrottleModeNone:
		config.MaxConcurrent = 100
		config.MaxQueueSize = 1000
		config.MaxRetries = 0

	case ThrottleModeLight:
		config.CooldownDuration = 100 * time.Millisecond
		config.MaxConcurrent = 20
		config.RateLimit = 100
		config.MaxQueueSize = 500
		config.MaxRetries = 2

	case ThrottleModeModerate:
		config.CooldownDuration = 1 * time.Second
		config.MaxConcurrent = 10
		config.RateLimit = 50
		config.MaxQueueSize = 200
		config.MaxRetries = 3

	case ThrottleModeStrict:
		config.CooldownDuration = 5 * time.Second
		config.SleepDuration = 1 * time.Second
		config.MaxConcurrent = 3
		config.RateLimit = 10
		config.MaxQueueSize = 50
		config.MaxRetries = 1
	}

	return config
}
