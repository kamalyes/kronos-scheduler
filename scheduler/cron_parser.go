/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-25 15:05:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 16:10:05
 * @FilePath: \kronos-scheduler\scheduler\cron_parser.go
 * @Description: Cron表达式解析器适配层 - 使用 go-toolbox/pkg/cron 的实现
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package scheduler

import (
	"github.com/kamalyes/go-toolbox/pkg/cron"
)

// 类型别名 - 直接使用 go-toolbox 的实现
type (
	// ParseOption 解析选项配置
	ParseOption = cron.CronParseOption

	// CronParser Cron 表达式解析器
	CronParser = cron.CronParser
)

// 常量映射 - 将本地常量映射到 go-toolbox 的常量
const (
	// Second 秒字段，默认 0
	Second = cron.CronSecond
	// SecondOptional 可选秒字段，默认 0
	SecondOptional = cron.CronSecondOptional
	// Minute 分钟字段，默认 0
	Minute = cron.CronMinute
	// Hour 小时字段，默认 0
	Hour = cron.CronHour
	// Dom 月中日期字段，默认 *
	Dom = cron.CronDom
	// Month 月份字段，默认 *
	Month = cron.CronMonth
	// Dow 星期字段，默认 *
	Dow = cron.CronDow
	// DowOptional 可选星期字段，默认 *
	DowOptional = cron.CronDowOptional
	// Descriptor 允许描述符，如 @monthly, @weekly 等
	Descriptor = cron.CronDescriptor
)

// NewCronParser 创建一个自定义配置的解析器
//
// 示例：
//
//	标准解析器(不包含秒)
//	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)
//	schedule, err := cron.Parse("0 0 15 */3 *")
//
//	包含秒的解析器
//	parser := NewCronParser(Second | Minute | Hour | Dom | Month | Dow)
//	schedule, err := cron.Parse("0 0 0 15 */3 *")
func NewCronParser(options ParseOption) *CronParser {
	return cron.NewCronParser(options)
}

// StandardParser 标准解析器(分 时 日 月 周，5个字段)
var StandardParser = cron.CronStandardParser

// SecondParser 带秒的解析器(秒 分 时 日 月 周，6个字段)
var SecondParser = cron.CronSecondParser

// ParseStandard 使用标准解析器解析 cron 表达式(5个字段)
func ParseStandard(spec string) (Schedule, error) {
	cronSchedule, err := cron.ParseCronStandard(spec)
	if err != nil {
		return nil, err
	}
	return cronSchedule, nil
}

// ParseWithSeconds 使用带秒的解析器解析 cron 表达式(6个字段)
func ParseWithSeconds(spec string) (Schedule, error) {
	cronSchedule, err := cron.ParseCronWithSeconds(spec)
	if err != nil {
		return nil, err
	}
	return cronSchedule, nil
}
