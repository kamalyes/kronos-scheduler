/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 15:08:58
 * @FilePath: \kronos-scheduler\logger\logger.go
 * @Description: Logger封装 - 基于 go-logger(类型别名模式，参考 go-wsc)
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package logger

import (
	gologger "github.com/kamalyes/go-logger"
	"time"
)

// Logger 是 go-logger ILogger 接口的类型别名
// 使用类型别名而非接口，直接复用 go-logger 的类型系统
type Logger = gologger.ILogger

// NewSchedulerLogger 创建带前缀的 Scheduler 日志器
func NewSchedulerLogger(prefix string) Logger {
	config := gologger.DefaultConfig().
		WithPrefix(prefix).
		WithLevel(gologger.INFO).
		WithShowCaller(false).
		WithColorful(true).
		WithTimeFormat(time.RFC3339)

	return gologger.NewLogger(config)
}

// NewNoOpLogger 创建空日志器(用于测试)
func NewNoOpLogger() Logger {
	return gologger.NewEmptyLogger()
}
