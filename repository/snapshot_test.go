/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-25 19:30:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-12-25 19:34:10
 * @FilePath: \kronos-scheduler\repository\snapshot_test.go
 * @Description: 快照仓库测试
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/kronos-scheduler/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// setupTestDB 创建测试数据库（SQLite 共享内存模式）
func setupTestDB(t *testing.T) *gorm.DB {
	// 使用共享内存模式,每个测试使用唯一的数据库名避免数据污染
	// 使用测试名称作为数据库名的一部分
	dbName := fmt.Sprintf("file:memdb_%s?mode=memory&cache=shared", t.Name())
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		PrepareStmt:                              false, // 禁用预编译语句以提高并发性能
	})
	require.NoError(t, err)

	// 设置连接池参数以支持并发
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(100)  // 最大打开连接数
	sqlDB.SetMaxIdleConns(10)   // 最大空闲连接数
	sqlDB.SetConnMaxLifetime(0) // 连接最大生命周期 - 0表示永不过期
	sqlDB.SetConnMaxIdleTime(0) // 连接最大空闲时间 - 0表示永不过期

	// 执行SQLite特定设置以提高并发性能
	db.Exec("PRAGMA journal_mode=WAL")   // 使用WAL模式提高并发性能
	db.Exec("PRAGMA synchronous=NORMAL") // 降低同步级别提高性能
	db.Exec("PRAGMA cache_size=10000")   // 增加缓存大小
	db.Exec("PRAGMA temp_store=MEMORY")  // 临时表存储在内存中
	db.Exec("PRAGMA busy_timeout=5000")  // 设置5秒的忙等待超时

	return db
}

// setupTestRepo 创建测试仓库
func setupTestRepo(t *testing.T) (*ExecutionSnapshotRepository, *gorm.DB) {
	db := setupTestDB(t)
	log := logger.New()
	repo, err := NewExecutionSnapshotRepository(db, log)
	require.NoError(t, err)
	require.NotNil(t, repo)

	// 清空表以确保测试隔离
	db.Exec("DELETE FROM job_execution_snapshots")

	return repo, db
}

// createTestSnapshot 创建测试快照
func createTestSnapshot(traceID, jobID, jobName, nodeID string, status models.ExecutionStatus) *models.ExecutionSnapshotModel {
	snapshot := models.NewExecutionSnapshot(traceID, jobID, jobName, models.JobTypeCron, nodeID)
	snapshot.Status = status
	snapshot.StartTime = time.Now().Add(-time.Minute)

	if status == models.StatusSuccess || status == models.StatusFailure {
		now := time.Now()
		snapshot.EndTime = &now
		if status == models.StatusSuccess {
			snapshot.Result = "test result"
		} else {
			snapshot.Error = "test error"
		}
	}

	return snapshot
}

func TestNewExecutionSnapshotRepository(t *testing.T) {
	tests := []struct {
		name    string
		db      *gorm.DB
		wantErr bool
	}{
		{
			name:    "valid database",
			db:      setupTestDB(t),
			wantErr: false,
		},
		{
			name:    "nil database",
			db:      nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logger.New()
			repo, err := NewExecutionSnapshotRepository(tt.db, log)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, repo)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, repo)
			}
		})
	}
}

func TestExecutionSnapshotRepository_Save(t *testing.T) {
	repo, _ := setupTestRepo(t)
	ctx := context.Background()

	tests := []struct {
		name     string
		snapshot *models.ExecutionSnapshotModel
		wantErr  bool
	}{
		{
			name:     "valid snapshot",
			snapshot: createTestSnapshot("trace-1", "job-1", "test-job", "node-1", models.StatusRunning),
			wantErr:  false,
		},
		{
			name:     "nil snapshot",
			snapshot: nil,
			wantErr:  true,
		},
		{
			name:     "duplicate save (update)",
			snapshot: createTestSnapshot("trace-1", "job-1", "test-job", "node-1", models.StatusSuccess),
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := repo.Save(ctx, tt.snapshot)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// 验证保存成功
				if tt.snapshot != nil {
					saved, err := repo.Get(ctx, tt.snapshot.TraceID)
					assert.NoError(t, err)
					assert.NotNil(t, saved)
					assert.Equal(t, tt.snapshot.TraceID, saved.TraceID)
					assert.Equal(t, tt.snapshot.JobID, saved.JobID)
					assert.Equal(t, tt.snapshot.Status, saved.Status)
				}
			}
		})
	}
}

func TestExecutionSnapshotRepository_Get(t *testing.T) {
	repo, _ := setupTestRepo(t)
	ctx := context.Background()

	// 准备测试数据
	snapshot := createTestSnapshot("trace-get", "job-get", "test-job", "node-1", models.StatusSuccess)
	err := repo.Save(ctx, snapshot)
	require.NoError(t, err)

	tests := []struct {
		name    string
		traceID string
		wantErr bool
		wantNil bool
	}{
		{
			name:    "existing snapshot",
			traceID: "trace-get",
			wantErr: false,
			wantNil: false,
		},
		{
			name:    "non-existing snapshot",
			traceID: "trace-not-exist",
			wantErr: true,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := repo.Get(ctx, tt.traceID)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.wantNil {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, tt.traceID, result.TraceID)
			}
		})
	}
}

func TestExecutionSnapshotRepository_List(t *testing.T) {
	ctx := context.Background()

	// 每个测试用例使用独立的 repo 避免数据污染
	repo, _ := setupTestRepo(t)

	// 准备测试数据
	snapshots := []*models.ExecutionSnapshotModel{
		createTestSnapshot("trace-1", "job-1", "test-job-1", "node-1", models.StatusSuccess),
		createTestSnapshot("trace-2", "job-1", "test-job-1", "node-1", models.StatusFailure),
		createTestSnapshot("trace-3", "job-2", "test-job-2", "node-2", models.StatusSuccess),
	}

	for _, s := range snapshots {
		err := repo.Save(ctx, s)
		require.NoError(t, err)
	}

	// 验证数据已保存
	count := repo.Count(ctx)
	require.Equal(t, int64(len(snapshots)), count, "should have 3 snapshots")
	tests := []struct {
		name        string
		filter      SnapshotFilter
		wantCount   int
		description string
	}{
		{
			name:        "no filter",
			filter:      SnapshotFilter{},
			wantCount:   3,
			description: "should return all snapshots",
		},
		{
			name: "filter by job_id",
			filter: SnapshotFilter{
				JobID: "job-1",
			},
			wantCount:   2,
			description: "should return only job-1 snapshots",
		},
		{
			name: "filter by status",
			filter: SnapshotFilter{
				Status: func() *models.ExecutionStatus { s := models.StatusSuccess; return &s }(),
			},
			wantCount:   2,
			description: "should return only success snapshots",
		},
		{
			name: "filter by node_id",
			filter: SnapshotFilter{
				NodeID: "node-2",
			},
			wantCount:   1,
			description: "should return only node-2 snapshots",
		},
		{
			name: "with limit",
			filter: SnapshotFilter{
				Page:     1,
				PageSize: 2,
			},
			wantCount:   2,
			description: "should return limited results",
		},
		{
			name: "with offset",
			filter: SnapshotFilter{
				Page:     2,
				PageSize: 1,
			},
			wantCount:   1,
			description: "should skip first 2 records",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := repo.List(ctx, tt.filter)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantCount, len(results), tt.description)
		})
	}
}

func TestExecutionSnapshotRepository_Delete(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		traceID     string
		setupData   bool // 是否需要准备数据
		wantErr     bool
		shouldExist bool // 删除前是否应该存在
	}{
		{
			name:        "delete existing snapshot",
			traceID:     "trace-delete",
			setupData:   true,
			wantErr:     false,
			shouldExist: true,
		},
		{
			name:        "delete non-existing snapshot",
			traceID:     "trace-not-exist",
			setupData:   false,
			wantErr:     false, // GORM delete 不存在的记录不报错
			shouldExist: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 每个子测试使用独立的 repo
			repo, _ := setupTestRepo(t)

			// 如果需要准备数据
			if tt.setupData {
				snapshot := createTestSnapshot(tt.traceID, "job-delete", "test-job", "node-1", models.StatusSuccess)
				err := repo.Save(ctx, snapshot)
				require.NoError(t, err)

				// 验证数据已保存
				if tt.shouldExist {
					saved, err := repo.Get(ctx, tt.traceID)
					require.NoError(t, err)
					require.NotNil(t, saved)
				}
			}

			// 执行删除
			err := repo.Delete(ctx, tt.traceID)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// 验证删除成功（记录不存在）
				result, err := repo.Get(ctx, tt.traceID)
				assert.Error(t, err) // 应该返回 record not found 错误
				assert.Nil(t, result)
			}
		})
	}
}

func TestExecutionSnapshotRepository_DeleteExpired(t *testing.T) {
	tests := []struct {
		name      string
		before    time.Time
		wantCount int64
	}{
		{
			name:      "delete older than 24 hours",
			before:    time.Now().Add(-24 * time.Hour),
			wantCount: 2,
		},
		{
			name:      "delete older than 1 hour (none)",
			before:    time.Now().Add(-1 * time.Hour),
			wantCount: 0,
		},
		{
			name:      "delete older than 100 hours (all)",
			before:    time.Now().Add(-100 * time.Hour),
			wantCount: 3, // 所有3条数据都会被删除
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 每个子测试使用独立的 repo 和数据
			repo, db := setupTestRepo(t)
			ctx := context.Background()

			// 准备测试数据（不同时间）- 先保存再更新时间戳
			old1 := createTestSnapshot("trace-old-1", "job-1", "test-job", "node-1", models.StatusSuccess)
			err := repo.Save(ctx, old1)
			require.NoError(t, err)

			old2 := createTestSnapshot("trace-old-2", "job-1", "test-job", "node-1", models.StatusSuccess)
			err = repo.Save(ctx, old2)
			require.NoError(t, err)

			recent := createTestSnapshot("trace-recent", "job-1", "test-job", "node-1", models.StatusSuccess)
			err = repo.Save(ctx, recent)
			require.NoError(t, err)

			// 手动更新创建时间（直接操作数据库）
			db.Exec("UPDATE job_execution_snapshots SET created_at = ? WHERE trace_id = ?", time.Now().Add(-48*time.Hour), "trace-old-1")
			db.Exec("UPDATE job_execution_snapshots SET created_at = ? WHERE trace_id = ?", time.Now().Add(-36*time.Hour), "trace-old-2")
			db.Exec("UPDATE job_execution_snapshots SET created_at = ? WHERE trace_id = ?", time.Now().Add(-12*time.Hour), "trace-recent")

			count, err := repo.DeleteExpired(ctx, tt.before)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantCount, count)
		})
	}
}

func TestExecutionSnapshotRepository_GetStatistics(t *testing.T) {
	repo, _ := setupTestRepo(t)
	ctx := context.Background()

	// 准备测试数据
	now := time.Now()
	snapshots := []*models.ExecutionSnapshotModel{
		createTestSnapshot("trace-stat-1", "job-stat", "test-job", "node-1", models.StatusSuccess),
		createTestSnapshot("trace-stat-2", "job-stat", "test-job", "node-1", models.StatusSuccess),
		createTestSnapshot("trace-stat-3", "job-stat", "test-job", "node-1", models.StatusFailure),
		createTestSnapshot("trace-stat-4", "job-stat", "test-job", "node-1", models.StatusTimeout),
	}

	for _, s := range snapshots {
		s.StartTime = now.Add(-10 * time.Minute)
		s.EndTime = &now
		err := repo.Save(ctx, s)
		require.NoError(t, err)
	}

	tests := []struct {
		name             string
		jobID            string
		from             time.Time
		to               time.Time
		expectedTotal    int
		expectedSuccess  int
		expectedFailure  int
		expectedTimeout  int
		checkSuccessRate bool
	}{
		{
			name:             "all statistics",
			jobID:            "job-stat",
			from:             now.Add(-1 * time.Hour),
			to:               now,
			expectedTotal:    4,
			expectedSuccess:  2,
			expectedFailure:  1,
			expectedTimeout:  1,
			checkSuccessRate: true,
		},
		{
			name:            "non-existing job",
			jobID:           "job-not-exist",
			from:            now.Add(-1 * time.Hour),
			to:              now,
			expectedTotal:   0,
			expectedSuccess: 0,
			expectedFailure: 0,
			expectedTimeout: 0,
		},
		{
			name:            "time range filter",
			jobID:           "job-stat",
			from:            now.Add(-2 * time.Hour),
			to:              now.Add(-30 * time.Minute),
			expectedTotal:   0,
			expectedSuccess: 0,
			expectedFailure: 0,
			expectedTimeout: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 为每个测试创建独立的 repo 和数据
			repo, _ := setupTestRepo(t)
			ctx := context.Background()

			// 只为需要数据的测试插入数据
			if tt.expectedTotal > 0 {
				for _, s := range snapshots {
					err := repo.Save(ctx, s)
					require.NoError(t, err)
				}
			}

			stats, err := repo.GetStatistics(ctx, tt.jobID, tt.from, tt.to)

			// 对于有数据的测试，验证统计结果
			if tt.expectedTotal > 0 {
				assert.NoError(t, err)
				assert.NotNil(t, stats)
				assert.Equal(t, tt.expectedTotal, stats.TotalCount)
				assert.Equal(t, tt.expectedSuccess, stats.SuccessCount)
				assert.Equal(t, tt.expectedFailure, stats.FailureCount)
				assert.Equal(t, tt.expectedTimeout, stats.TimeoutCount)
			} else {
				// 无数据的测试只验证不报错
				assert.NoError(t, err)
				assert.NotNil(t, stats)
				assert.Equal(t, tt.expectedTotal, stats.TotalCount)
			}
			if tt.checkSuccessRate && tt.expectedTotal > 0 {
				expectedRate := float64(tt.expectedSuccess) / float64(tt.expectedTotal) * 100
				assert.InDelta(t, expectedRate, stats.SuccessRate, 0.01)
			}

			// 验证持续时间统计
			if tt.expectedTotal > 0 {
				assert.Greater(t, stats.AvgDuration, time.Duration(0))
				assert.Greater(t, stats.MaxDuration, time.Duration(0))
				assert.Greater(t, stats.MinDuration, time.Duration(0))
				assert.False(t, stats.LastExecution.IsZero())
			}
		})
	}
}

func TestExecutionSnapshotRepository_Count(t *testing.T) {
	repo, _ := setupTestRepo(t)
	ctx := context.Background()

	// 初始计数应为0
	count := repo.Count(ctx)
	assert.Equal(t, int64(0), count)

	// 添加数据
	for i := 1; i <= 5; i++ {
		snapshot := createTestSnapshot(
			"trace-count-"+string(rune(i)),
			"job-count",
			"test-job",
			"node-1",
			models.StatusSuccess,
		)
		err := repo.Save(ctx, snapshot)
		require.NoError(t, err)
	}

	// 验证计数
	count = repo.Count(ctx)
	assert.Equal(t, int64(5), count)
}

func TestExecutionSnapshotRepository_Clear(t *testing.T) {
	repo, _ := setupTestRepo(t)
	ctx := context.Background()

	// 准备测试数据
	for i := 1; i <= 3; i++ {
		snapshot := createTestSnapshot(
			"trace-clear-"+string(rune(i)),
			"job-clear",
			"test-job",
			"node-1",
			models.StatusSuccess,
		)
		err := repo.Save(ctx, snapshot)
		require.NoError(t, err)
	}

	// 验证有数据
	count := repo.Count(ctx)
	assert.Greater(t, count, int64(0))

	// 清空
	err := repo.Clear(ctx)
	assert.NoError(t, err)

	// 验证已清空
	count = repo.Count(ctx)
	assert.Equal(t, int64(0), count)
}
