/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-23 20:00:00
 * @FilePath: \kronos-scheduler\job\errors.go
 * @Description: Job错误定义
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package job

import (
	"errors"
	"fmt"
)

var (
	// ErrJobTimeout 任务执行超时
	ErrJobTimeout = errors.New("job execution timeout")
	// ErrJobAlreadyRunning 任务已在运行中(重叠执行防止)
	ErrJobAlreadyRunning = errors.New("job is already running")
	// ErrMaxRetriesExceeded 超过最大重试次数
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
)

// JobError 任务错误(带上下文信息)
type JobError struct {
	JobName string
	Err     error
	Message string
}

func (e *JobError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("job [%s]: %s: %v", e.JobName, e.Message, e.Err)
	}
	return fmt.Sprintf("job [%s]: %v", e.JobName, e.Err)
}

func (e *JobError) Unwrap() error {
	return e.Err
}

// NewJobError 创建任务错误
func NewJobError(jobName string, err error, message string) error {
	return &JobError{
		JobName: jobName,
		Err:     err,
		Message: message,
	}
}

// IsTimeout 判断是否为超时错误
func IsTimeout(err error) bool {
	return errors.Is(err, ErrJobTimeout)
}

// IsAlreadyRunning 判断是否为重叠执行错误
func IsAlreadyRunning(err error) bool {
	return errors.Is(err, ErrJobAlreadyRunning)
}

// IsMaxRetriesExceeded 判断是否超过最大重试次数
func IsMaxRetriesExceeded(err error) bool {
	return errors.Is(err, ErrMaxRetriesExceeded)
}
