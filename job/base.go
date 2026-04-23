/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-23 18:00:00
 * @FilePath: \kronos-scheduler\job\base.go
 * @Description: 基础Job实现
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package job

import (
	"context"
	"errors"
)

var (
	// ErrNotImplemented Execute方法未实现错误
	ErrNotImplemented = errors.New("Execute method must be implemented")
)

// BaseJob 基础Job实现(可嵌入到具体Job中)
type BaseJob struct {
	name        string
	description string
}

// NewBaseJob 创建基础Job
func NewBaseJob(name, description string) *BaseJob {
	return &BaseJob{
		name:        name,
		description: description,
	}
}

// Name 实现Job接口
func (b *BaseJob) Name() string {
	return b.name
}

// Description 实现Job接口
func (b *BaseJob) Description() string {
	return b.description
}

// Execute 需要子类实现 - 直接调用会返回错误而非panic
func (b *BaseJob) Execute(ctx context.Context) error {
	return ErrNotImplemented
}

// FuncJob 函数式Job(快速创建简单Job)
type FuncJob struct {
	*BaseJob
	executeFunc func(ctx context.Context) error
}

// NewFuncJob 创建函数式Job
func NewFuncJob(name, description string, executeFunc func(ctx context.Context) error) *FuncJob {
	return &FuncJob{
		BaseJob:     NewBaseJob(name, description),
		executeFunc: executeFunc,
	}
}

// Execute 执行函数
func (f *FuncJob) Execute(ctx context.Context) error {
	return f.executeFunc(ctx)
}
