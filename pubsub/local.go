/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-23 17:55:05
 * @FilePath: \kronos-scheduler\pubsub\local.go
 * @Description: 本地PubSub实现(单机模式，基于channel)
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package pubsub

import (
	"context"
	"sync"
	"time"
)

// LocalPubSub 本地PubSub实现(用于单机场景)
type LocalPubSub struct {
	configUpdateCh     chan string
	cronSpecUpdateCh   chan cronSpecUpdate
	manualExecuteCh    chan string
	scheduledExecuteCh chan scheduledExecute
	jobDeletedCh       chan string
	mu                 sync.RWMutex
	closed             bool
}

type cronSpecUpdate struct {
	JobName     string
	NewCronSpec string
}

type scheduledExecute struct {
	JobName     string
	ExecuteTime time.Time
	RepeatCount int
}

// NewLocalPubSub 创建本地PubSub
func NewLocalPubSub() PubSub {
	return &LocalPubSub{
		configUpdateCh:     make(chan string, 100),
		cronSpecUpdateCh:   make(chan cronSpecUpdate, 100),
		manualExecuteCh:    make(chan string, 100),
		scheduledExecuteCh: make(chan scheduledExecute, 100),
		jobDeletedCh:       make(chan string, 100),
	}
}

// PublishConfigUpdate 发布配置更新消息
func (p *LocalPubSub) PublishConfigUpdate(ctx context.Context, jobName string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil
	}
	select {
	case p.configUpdateCh <- jobName:
	default:
	}
	return nil
}

// PublishCronSpecUpdate 发布Cron表达式更新消息
func (p *LocalPubSub) PublishCronSpecUpdate(ctx context.Context, jobName, newCronSpec string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil
	}
	select {
	case p.cronSpecUpdateCh <- cronSpecUpdate{JobName: jobName, NewCronSpec: newCronSpec}:
	default:
	}
	return nil
}

// PublishManualExecute 发布手动执行消息
func (p *LocalPubSub) PublishManualExecute(ctx context.Context, jobName string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil
	}
	select {
	case p.manualExecuteCh <- jobName:
	default:
	}
	return nil
}

// PublishScheduledExecute 发布定时执行消息
func (p *LocalPubSub) PublishScheduledExecute(ctx context.Context, jobName string, executeTime time.Time, repeatCount int) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil
	}
	select {
	case p.scheduledExecuteCh <- scheduledExecute{JobName: jobName, ExecuteTime: executeTime, RepeatCount: repeatCount}:
	default:
	}
	return nil
}

// PublishJobDeleted 发布任务删除消息
func (p *LocalPubSub) PublishJobDeleted(ctx context.Context, jobName string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil
	}
	select {
	case p.jobDeletedCh <- jobName:
	default:
	}
	return nil
}

// SubscribeConfigUpdate 订阅配置更新消息
func (p *LocalPubSub) SubscribeConfigUpdate(ctx context.Context, handler func(jobName string)) error {
	go func() {
		for {
			select {
			case jobName := <-p.configUpdateCh:
				handler(jobName)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// SubscribeCronSpecUpdate 订阅Cron表达式更新消息
func (p *LocalPubSub) SubscribeCronSpecUpdate(ctx context.Context, handler func(jobName, cronSpec string)) error {
	go func() {
		for {
			select {
			case update := <-p.cronSpecUpdateCh:
				handler(update.JobName, update.NewCronSpec)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// SubscribeManualExecute 订阅手动执行消息
func (p *LocalPubSub) SubscribeManualExecute(ctx context.Context, handler func(jobName string)) error {
	go func() {
		for {
			select {
			case jobName := <-p.manualExecuteCh:
				handler(jobName)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// SubscribeScheduledExecute 订阅定时执行消息
func (p *LocalPubSub) SubscribeScheduledExecute(ctx context.Context, handler func(jobName string, executeTime time.Time, repeatCount int)) error {
	go func() {
		for {
			select {
			case scheduled := <-p.scheduledExecuteCh:
				handler(scheduled.JobName, scheduled.ExecuteTime, scheduled.RepeatCount)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// SubscribeJobDeleted 订阅任务删除消息
func (p *LocalPubSub) SubscribeJobDeleted(ctx context.Context, handler func(jobName string)) error {
	go func() {
		for {
			select {
			case jobName := <-p.jobDeletedCh:
				handler(jobName)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// Close 关闭PubSub
func (p *LocalPubSub) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		close(p.configUpdateCh)
		close(p.cronSpecUpdateCh)
		close(p.manualExecuteCh)
		close(p.scheduledExecuteCh)
		close(p.jobDeletedCh)
	}
	return nil
}
