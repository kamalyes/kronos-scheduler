/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-23 17:55:15
 * @FilePath: \kronos-scheduler\pubsub\events.go
 * @Description: 事件定义
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package pubsub

const (
	// EventJobConfigUpdate Job配置更新事件
	EventJobConfigUpdate = "job:config:update"

	// EventJobCronSpecUpdate Job CronSpec更新事件
	EventJobCronSpecUpdate = "job:cronspec:update"

	// EventJobManualExecute Job手动执行事件
	EventJobManualExecute = "job:manual:execute"

	// EventJobScheduledExecute Job定时执行事件
	EventJobScheduledExecute = "job:scheduled:execute"

	// EventJobDeleted Job删除事件
	EventJobDeleted = "job:deleted"

	// EventNodeRegistered 节点注册事件
	EventNodeRegistered = "node:registered"

	// EventNodeHeartbeat 节点心跳事件
	EventNodeHeartbeat = "node:heartbeat"
)
