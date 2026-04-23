/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-25 17:45:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 19:55:05
 * @FilePath: \kronos-scheduler\repository\snapshot.go
 * @Description: 基于 GORM 的快照仓库实现 - 支持单机和分布式
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-sqlbuilder/constants"
	sqldb "github.com/kamalyes/go-sqlbuilder/db"
	sqlrepo "github.com/kamalyes/go-sqlbuilder/repository"
	"github.com/kamalyes/kronos-scheduler/models"
	"gorm.io/gorm"
)

// SnapshotFilter 快照过滤器
type SnapshotFilter struct {
	JobID     string                  // 任务ID
	NodeID    string                  // 节点ID
	Status    *models.ExecutionStatus // 状态（使用指针以区分"未设置"和"值为0"）
	FromTime  time.Time               // 开始时间
	ToTime    time.Time               // 结束时间
	Page      int                     // 页码
	PageSize  int                     // 每页数量
	OrderBy   string                  // 排序字段
	OrderDesc bool                    // 降序
}

// SnapshotStatistics 快照统计信息
type SnapshotStatistics struct {
	TotalCount    int           // 总执行次数
	SuccessCount  int           // 成功次数
	FailureCount  int           // 失败次数
	TimeoutCount  int           // 超时次数
	AvgDuration   time.Duration // 平均执行时间
	MaxDuration   time.Duration // 最大执行时间
	MinDuration   time.Duration // 最小执行时间
	SuccessRate   float64       // 成功率
	LastExecution time.Time     // 最后执行时间
}

// ExecutionSnapshotRepository 基于 BaseRepository 的快照仓库
type ExecutionSnapshotRepository struct {
	base *sqlrepo.BaseRepository[models.ExecutionSnapshotModel]
}

// NewExecutionSnapshotRepository 创建 GORM 快照仓库
// 支持 SQLite（单机）、MySQL、PostgreSQL 等
func NewExecutionSnapshotRepository(db *gorm.DB, logger logger.ILogger) (*ExecutionSnapshotRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}

	// 自动迁移表结构
	if err := db.AutoMigrate(&models.ExecutionSnapshotModel{}); err != nil {
		return nil, fmt.Errorf("failed to migrate snapshot table: %w", err)
	}

	// 创建数据库处理器
	dbHandler, err := sqldb.NewGormHandler(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create db handler: %w", err)
	}

	// 创建基础仓库
	baseRepo := sqlrepo.NewBaseRepository(
		dbHandler,
		logger,
		models.ExecutionSnapshotModel{}.TableName(),
		sqlrepo.WithBatchSize[models.ExecutionSnapshotModel](100),
		sqlrepo.WithDefaultOrder[models.ExecutionSnapshotModel]("created_at DESC"),
		sqlrepo.WithAutoFields[models.ExecutionSnapshotModel](),
	)

	return &ExecutionSnapshotRepository{
		base: baseRepo,
	}, nil
}

// Save 保存快照（存在则更新，不存在则创建）
func (r *ExecutionSnapshotRepository) Save(ctx context.Context, snapshot *models.ExecutionSnapshotModel) error {
	if snapshot == nil {
		return fmt.Errorf("snapshot cannot be nil")
	}

	_, _, err := r.base.CreateOrUpdate(ctx, snapshot, "trace_id")
	return err
}

// Get 获取快照
func (r *ExecutionSnapshotRepository) Get(ctx context.Context, traceID string) (*models.ExecutionSnapshotModel, error) {
	return r.base.FindOneWhere(ctx, "trace_id", traceID)
}

// List 列出快照
func (r *ExecutionSnapshotRepository) List(ctx context.Context, filter SnapshotFilter) ([]*models.ExecutionSnapshotModel, error) {
	query := sqlrepo.NewQuery().
		AddFilterIfNotEmpty("job_id", filter.JobID).
		AddFilterIfNotEmpty("node_id", filter.NodeID).
		AddTimeRangeFilter("start_time", filter.FromTime, filter.ToTime).
		WithPaging(filter.Page, filter.PageSize)

	// Status 使用指针，只有非 nil 时才过滤
	if filter.Status != nil {
		query.AddFilterIfNotEmpty("status", int(*filter.Status))
	}

	snapshots, err := r.base.List(ctx, query)
	if err != nil {
		return nil, err
	}

	return snapshots, nil
}

// Delete 删除快照
func (r *ExecutionSnapshotRepository) Delete(ctx context.Context, traceID string) error {
	return r.base.DeleteWhere(ctx, "trace_id", traceID)
}

// DeleteExpired 删除过期快照（直接返回删除条数）
func (r *ExecutionSnapshotRepository) DeleteExpired(ctx context.Context, before time.Time) (int64, error) {
	return r.base.DeleteWhereOpWithCount(ctx, "created_at", constants.OP_LT, before)
}

// GetStatistics 获取统计信息
func (r *ExecutionSnapshotRepository) GetStatistics(ctx context.Context, jobID string, from, to time.Time) (*SnapshotStatistics, error) {
	stats := &SnapshotStatistics{}

	// 构建基础查询（复用查询条件）
	baseQuery := sqlrepo.NewQuery().
		AddFilterIfNotEmpty("job_id", jobID).
		AddTimeRangeFilter("start_time", from, to)

	// 使用顺序查询获取统计数据（SQLite 共享内存不适合并发查询）
	totalCount, _ := r.base.Count(ctx, baseQuery.Filters...)
	stats.TotalCount = int(totalCount)

	successQuery := sqlrepo.NewQuery().AddFilters(baseQuery.Filters...).AddFilterIfNotEmpty("status", int(models.StatusSuccess))
	successCount, _ := r.base.Count(ctx, successQuery.Filters...)
	stats.SuccessCount = int(successCount)

	failureQuery := sqlrepo.NewQuery().AddFilters(baseQuery.Filters...).AddFilterIfNotEmpty("status", int(models.StatusFailure))
	failureCount, _ := r.base.Count(ctx, failureQuery.Filters...)
	stats.FailureCount = int(failureCount)

	timeoutQuery := sqlrepo.NewQuery().AddFilters(baseQuery.Filters...).AddFilterIfNotEmpty("status", int(models.StatusTimeout))
	timeoutCount, _ := r.base.Count(ctx, timeoutQuery.Filters...)
	stats.TimeoutCount = int(timeoutCount)

	// 使用 SQL 聚合函数计算持续时间统计（直接在数据库层面计算）
	type DurationStats struct {
		AvgDuration  float64 `gorm:"column:avg_duration"`   // 平均持续时间（微秒，使用 float64 兼容 SQLite）
		MaxDuration  float64 `gorm:"column:max_duration"`   // 最大持续时间（微秒）
		MinDuration  float64 `gorm:"column:min_duration"`   // 最小持续时间（微秒）
		MaxStartTime string  `gorm:"column:max_start_time"` // 最后执行时间（SQLite 返回字符串）
	}

	var durationStats DurationStats
	db := r.base.DBHandler().GetDB().WithContext(ctx).Table(models.ExecutionSnapshotModel{}.TableName())

	// 根据数据库类型使用不同的时间差计算方式
	dialector := db.Dialector.Name()
	var selectSQL string

	switch dialector {
	case "mysql":
		// MySQL: TIMESTAMPDIFF 返回微秒
		selectSQL = `
			AVG(TIMESTAMPDIFF(MICROSECOND, start_time, end_time)) as avg_duration,
			MAX(TIMESTAMPDIFF(MICROSECOND, start_time, end_time)) as max_duration,
			MIN(TIMESTAMPDIFF(MICROSECOND, start_time, end_time)) as min_duration,
			MAX(start_time) as max_start_time`
	case "sqlite":
		// SQLite: julianday 返回天数，转换为微秒
		selectSQL = `
			AVG((julianday(end_time) - julianday(start_time)) * 86400000000) as avg_duration,
			MAX((julianday(end_time) - julianday(start_time)) * 86400000000) as max_duration,
			MIN((julianday(end_time) - julianday(start_time)) * 86400000000) as min_duration,
			MAX(start_time) as max_start_time`
	case "postgres", "postgresql":
		// PostgreSQL: EXTRACT(EPOCH) 返回秒，转换为微秒
		selectSQL = `
			AVG(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000000) as avg_duration,
			MAX(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000000) as max_duration,
			MIN(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000000) as min_duration,
			MAX(start_time) as max_start_time`
	default:
		// 默认使用 MySQL 语法
		selectSQL = `
			AVG(TIMESTAMPDIFF(MICROSECOND, start_time, end_time)) as avg_duration,
			MAX(TIMESTAMPDIFF(MICROSECOND, start_time, end_time)) as max_duration,
			MIN(TIMESTAMPDIFF(MICROSECOND, start_time, end_time)) as min_duration,
			MAX(start_time) as max_start_time`
	}

	db = db.Select(selectSQL)

	// 应用过滤条件
	for _, filter := range baseQuery.Filters {
		db = sqlrepo.ApplyFilter(db, filter)
	}

	if err := db.Scan(&durationStats).Error; err == nil {
		// 转换为 time.Duration（微秒 -> 纳秒）
		// SQLite 返回 float64，需要转换为 int64 再转为 Duration
		stats.AvgDuration = time.Duration(int64(durationStats.AvgDuration)) * time.Microsecond
		stats.MaxDuration = time.Duration(int64(durationStats.MaxDuration)) * time.Microsecond
		stats.MinDuration = time.Duration(int64(durationStats.MinDuration)) * time.Microsecond

		// 解析 SQLite 时间字符串
		if durationStats.MaxStartTime != "" {
			// SQLite 可能返回多种格式，尝试多种解析方式
			timeFormats := []string{
				"2006-01-02 15:04:05.9999999-07:00",   // SQLite 带时区格式（7位小数）
				"2006-01-02 15:04:05.9999999+08:00",   // SQLite 带正时区（7位小数）
				"2006-01-02 15:04:05.999999999-07:00", // SQLite 带时区格式（9位小数）
				"2006-01-02 15:04:05.999999999+08:00", // SQLite 带正时区（9位小数）
				"2006-01-02 15:04:05.999999999 -07:00",
				"2006-01-02 15:04:05.999999999",
				"2006-01-02 15:04:05",
				time.RFC3339Nano,
				time.RFC3339,
			}
			for _, format := range timeFormats {
				if parsedTime, err := time.Parse(format, durationStats.MaxStartTime); err == nil {
					stats.LastExecution = parsedTime
					break
				}
			}
		}
	}

	// 计算成功率
	if stats.TotalCount > 0 {
		stats.SuccessRate = float64(stats.SuccessCount) / float64(stats.TotalCount) * 100
	}

	return stats, nil
}

// Count 获取快照总数
func (r *ExecutionSnapshotRepository) Count(ctx context.Context) int64 {
	count, err := r.base.Count(ctx)
	if err != nil {
		return 0
	}
	return count
}

// Clear 清空所有快照（危险操作，仅用于测试）
func (r *ExecutionSnapshotRepository) Clear(ctx context.Context) error {
	return r.base.DeleteWhereOp(ctx, "id", sqlrepo.NewGtFilter("id", 0).Operator, 0)
}
