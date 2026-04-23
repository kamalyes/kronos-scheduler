/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-23 18:00:00
 * @FilePath: \kronos-scheduler\pubsub\redis.go
 * @Description: Redis PubSub实现(基于 go-cachex)
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kamalyes/go-cachex"
)

// RedisPubSub Redis PubSub实现(分布式场景)
type RedisPubSub struct {
	pubsub *cachex.PubSub
}

// NewRedisPubSub 创建Redis PubSub
func NewRedisPubSub(pubsub *cachex.PubSub) PubSub {
	return &RedisPubSub{
		pubsub: pubsub,
	}
}

func (r *RedisPubSub) PublishConfigUpdate(ctx context.Context, jobName string) error {
	return r.pubsub.Publish(ctx, EventJobConfigUpdate, jobName)
}

func (r *RedisPubSub) PublishCronSpecUpdate(ctx context.Context, jobName, newCronSpec string) error {
	payload := fmt.Sprintf("%s:%s", jobName, newCronSpec)
	return r.pubsub.Publish(ctx, EventJobCronSpecUpdate, payload)
}

func (r *RedisPubSub) PublishManualExecute(ctx context.Context, jobName string) error {
	return r.pubsub.Publish(ctx, EventJobManualExecute, jobName)
}

func (r *RedisPubSub) PublishScheduledExecute(ctx context.Context, jobName string, executeTime time.Time, repeatCount int) error {
	payload, err := serializeScheduledExecute(jobName, executeTime, repeatCount)
	if err != nil {
		return err
	}
	return r.pubsub.Publish(ctx, EventJobScheduledExecute, payload)
}

func (r *RedisPubSub) PublishJobDeleted(ctx context.Context, jobName string) error {
	return r.pubsub.Publish(ctx, EventJobDeleted, jobName)
}

func (r *RedisPubSub) SubscribeConfigUpdate(ctx context.Context, handler func(jobName string)) error {
	_, err := r.pubsub.Subscribe([]string{EventJobConfigUpdate}, func(ctx context.Context, channel string, message string) error {
		handler(message)
		return nil
	})
	return err
}

func (r *RedisPubSub) SubscribeCronSpecUpdate(ctx context.Context, handler func(jobName, cronSpec string)) error {
	_, err := r.pubsub.Subscribe([]string{EventJobCronSpecUpdate}, func(ctx context.Context, channel string, message string) error {
		var jobName, cronSpec string
		fmt.Sscanf(message, "%s:%s", &jobName, &cronSpec)
		handler(jobName, cronSpec)
		return nil
	})
	return err
}

func (r *RedisPubSub) SubscribeManualExecute(ctx context.Context, handler func(jobName string)) error {
	_, err := r.pubsub.Subscribe([]string{EventJobManualExecute}, func(ctx context.Context, channel string, message string) error {
		handler(message)
		return nil
	})
	return err
}

func (r *RedisPubSub) SubscribeScheduledExecute(ctx context.Context, handler func(jobName string, executeTime time.Time, repeatCount int)) error {
	_, err := r.pubsub.Subscribe([]string{EventJobScheduledExecute}, func(ctx context.Context, channel string, message string) error {
		jobName, executeTime, repeatCount, err := deserializeScheduledExecute(message)
		if err != nil {
			return err
		}
		handler(jobName, executeTime, repeatCount)
		return nil
	})
	return err
}

func (r *RedisPubSub) SubscribeJobDeleted(ctx context.Context, handler func(jobName string)) error {
	_, err := r.pubsub.Subscribe([]string{EventJobDeleted}, func(ctx context.Context, channel string, message string) error {
		handler(message)
		return nil
	})
	return err
}

func (r *RedisPubSub) Close() error {
	return nil
}

// serializeScheduledExecute 序列化定时执行数据
func serializeScheduledExecute(jobName string, executeTime time.Time, repeatCount int) (string, error) {
	data := map[string]interface{}{
		"job_name":     jobName,
		"execute_time": executeTime.Unix(),
		"repeat_count": repeatCount,
	}
	bytes, err := json.Marshal(data)
	return string(bytes), err
}

// deserializeScheduledExecute 反序列化定时执行数据
func deserializeScheduledExecute(payload string) (string, time.Time, int, error) {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		return "", time.Time{}, 0, err
	}

	jobName := data["job_name"].(string)
	executeTime := time.Unix(int64(data["execute_time"].(float64)), 0)
	repeatCount := int(data["repeat_count"].(float64))

	return jobName, executeTime, repeatCount, nil
}
