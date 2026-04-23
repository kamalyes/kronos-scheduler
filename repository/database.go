/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-23 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-23 17:35:17
 * @FilePath: \kronos-scheduler\repository\database.go
 * @Description: 数据库仓储实现(基于 GORM)
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/kamalyes/kronos-scheduler/models"
	"gorm.io/gorm"
)

const (
	// whereJobName SQL where clause for job_name field
	whereJobName = "job_name = ?"
)

// DatabaseJobConfigRepository 数据库Job配置仓储
type DatabaseJobConfigRepository struct {
	db *gorm.DB
}

// NewDatabaseJobConfigRepository 创建数据库仓储
func NewDatabaseJobConfigRepository(db *gorm.DB) (JobConfigRepository, error) {
	// 自动迁移表结构
	if err := db.AutoMigrate(&models.JobConfigModel{}); err != nil {
		return nil, fmt.Errorf("auto migrate job_configs failed: %w", err)
	}

	return &DatabaseJobConfigRepository{
		db: db,
	}, nil
}

func (r *DatabaseJobConfigRepository) GetByJobName(ctx context.Context, jobName string) (*models.JobConfigModel, error) {
	var config models.JobConfigModel
	err := r.db.WithContext(ctx).Where(whereJobName, jobName).First(&config).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf(ErrJobConfigNotFound, jobName)
		}
		return nil, err
	}
	return &config, nil
}

func (r *DatabaseJobConfigRepository) Create(ctx context.Context, config *models.JobConfigModel) error {
	return r.db.WithContext(ctx).Create(config).Error
}

func (r *DatabaseJobConfigRepository) Update(ctx context.Context, config *models.JobConfigModel) error {
	return r.db.WithContext(ctx).Model(&models.JobConfigModel{}).
		Where(whereJobName, config.JobName).
		Updates(config).Error
}

func (r *DatabaseJobConfigRepository) EnsureConfigExists(ctx context.Context, config *models.JobConfigModel) (*models.JobConfigModel, error) {
	existing, err := r.GetByJobName(ctx, config.JobName)
	if err == nil {
		return existing, nil
	}

	if err := r.Create(ctx, config); err != nil {
		return nil, err
	}
	return config, nil
}

func (r *DatabaseJobConfigRepository) UpdateCronSpec(ctx context.Context, jobName, cronSpec string) error {
	var config models.JobConfigModel
	if err := r.db.WithContext(ctx).Where(whereJobName, jobName).First(&config).Error; err != nil {
		return err
	}

	if err := config.UpdateCronSpec(cronSpec); err != nil {
		return err
	}

	return r.db.WithContext(ctx).Model(&models.JobConfigModel{}).
		Where(whereJobName, jobName).
		Update("task_config", config.TaskConfig).Error
}

func (r *DatabaseJobConfigRepository) UpdateJobExecutionTime(ctx context.Context, jobName string, lastExec, nextExec time.Time) error {
	return r.db.WithContext(ctx).Model(&models.JobConfigModel{}).
		Where(whereJobName, jobName).
		Updates(map[string]interface{}{
			"last_executed_at":  lastExec,
			"next_execution_at": nextExec,
		}).Error
}

func (r *DatabaseJobConfigRepository) ListAll(ctx context.Context) ([]*models.JobConfigModel, error) {
	var configs []*models.JobConfigModel
	err := r.db.WithContext(ctx).Find(&configs).Error
	return configs, err
}

func (r *DatabaseJobConfigRepository) Delete(ctx context.Context, jobName string) error {
	return r.db.WithContext(ctx).Where(whereJobName, jobName).Delete(&models.JobConfigModel{}).Error
}
