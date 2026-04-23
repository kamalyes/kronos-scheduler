/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-12-25 17:20:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-04-23 09:33:29
 * @FilePath: \kronos-scheduler\scheduler\scheduler_service.go
 * @Description: 调度器服务 - 提供调度器相关 RPC 接口
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */
package scheduler

import (
	"context"
	"fmt"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/kronos-scheduler/models"
	"github.com/kamalyes/kronos-scheduler/proto"
	"github.com/kamalyes/kronos-scheduler/repository"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
	"time"
)

// SchedulerService 调度器服务 - 提供调度器相关 RPC 接口
type SchedulerService struct {
	proto.UnimplementedSchedulerServiceServer
	scheduler    *CronScheduler
	jobRepo      repository.JobConfigRepository
	snapshotRepo repository.IExecutionSnapshotRepository
	startTime    time.Time
	logger       logger.ILogger
}

// NewSchedulerService 创建调度器服务
func NewSchedulerService(
	scheduler *CronScheduler,
	jobRepo repository.JobConfigRepository,
	snapshotRepo repository.IExecutionSnapshotRepository,
	log logger.ILogger,
) *SchedulerService {
	return &SchedulerService{
		scheduler:    scheduler,
		jobRepo:      jobRepo,
		snapshotRepo: snapshotRepo,
		startTime:    time.Now(),
		logger:       log,
	}
}

// SchedulerOverview 获取调度器概览
func (s *SchedulerService) SchedulerOverview(ctx context.Context, req *proto.SchedulerOverviewRequest) (*proto.SchedulerOverviewResponse, error) {
	jobNames := s.scheduler.ListJobs()
	totalJobs := len(jobNames)

	activeJobs := 0
	disabledJobs := 0
	jobsByType := make(map[string]int32)

	for _, name := range jobNames {
		entry, ok := s.scheduler.GetEntry(name)
		if !ok || !entry.Config.Enabled {
			disabledJobs++
			continue
		}
		activeJobs++
		jobsByType["cron"]++
	}

	resp := &proto.SchedulerOverviewResponse{
		TotalJobs:         int32(totalJobs),
		ActiveJobs:        int32(activeJobs),
		PausedJobs:        0,
		DisabledJobs:      int32(disabledJobs),
		ErrorJobs:         0,
		SchedulerUptimeMs: time.Since(s.startTime).Milliseconds(),
		JobsByType:        jobsByType,
	}

	if s.snapshotRepo != nil {
		now := time.Now()
		from := now.Add(-24 * time.Hour)

		stats, err := s.snapshotRepo.GetStatistics(ctx, "", from, now)
		if err == nil && stats != nil {
			resp.TotalCount = int64(stats.TotalCount)
			resp.SuccessCount = int64(stats.SuccessCount)
			resp.FailureCount = int64(stats.FailureCount)
			resp.TimeoutCount = int64(stats.TimeoutCount)
			resp.AvgExecutionDurationMs = float64(stats.AvgDuration.Milliseconds())

			if stats.TotalCount > 0 {
				resp.SuccessRate = stats.SuccessRate
			}
		}

		runningStatus := models.StatusRunning
		filter := repository.SnapshotFilter{
			Status:   &runningStatus,
			PageSize: 1,
		}
		if snapshots, err := s.snapshotRepo.List(ctx, filter); err == nil {
			resp.RunningExecutions = int32(len(snapshots))
		}
	}

	return resp, nil
}

// SchedulerListJobs 获取所有任务配置
func (s *SchedulerService) SchedulerListJobs(ctx context.Context, req *proto.SchedulerListJobsRequest) (*proto.SchedulerListJobsResponse, error) {
	var configs []*models.JobConfigModel
	var err error

	if s.jobRepo != nil {
		configs, err = s.jobRepo.ListAll(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list job configs: %w", err)
		}
	}

	if configs == nil {
		configs = make([]*models.JobConfigModel, 0)
	}

	filtered := make([]*models.JobConfigModel, 0, len(configs))
	for _, cfg := range configs {
		if req.FilterState != proto.SchedulerJobState_SCHEDULER_JOB_STATE_UNSPECIFIED {
			if !s.matchJobState(cfg, req.FilterState) {
				continue
			}
		}
		if req.Search != "" {
			if !containsIgnoreCase(cfg.JobName, req.Search) {
				continue
			}
		}
		filtered = append(filtered, cfg)
	}

	totalCount := int32(len(filtered))

	pageSize := int(req.PageSize)
	if pageSize <= 0 {
		pageSize = 50
	}
	offset := 0
	if req.PageToken != "" {
		if parsed, parseErr := parseInt(req.PageToken); parseErr == nil {
			offset = parsed
		}
	}

	end := offset + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}

	var paged []*models.JobConfigModel
	if offset < len(filtered) {
		paged = filtered[offset:end]
	} else {
		paged = make([]*models.JobConfigModel, 0)
	}

	jobs := make([]*proto.SchedulerJobConfigInfo, 0, len(paged))
	for _, cfg := range paged {
		jobs = append(jobs, s.jobConfigToProto(cfg))
	}

	var nextToken string
	if end < len(filtered) {
		nextToken = fmt.Sprintf("%d", end)
	}

	return &proto.SchedulerListJobsResponse{
		Jobs:          jobs,
		TotalCount:    totalCount,
		NextPageToken: nextToken,
	}, nil
}

// SchedulerGetJob 获取任务配置
func (s *SchedulerService) SchedulerGetJob(ctx context.Context, req *proto.SchedulerGetJobRequest) (*proto.SchedulerGetJobResponse, error) {
	if req.JobName == "" {
		return nil, fmt.Errorf("job name is required")
	}

	if s.jobRepo == nil {
		return nil, fmt.Errorf("job repository not configured")
	}

	cfg, err := s.jobRepo.GetByJobName(ctx, req.JobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get job config: %w", err)
	}

	return &proto.SchedulerGetJobResponse{
		Job: s.jobConfigToProto(cfg),
	}, nil
}

// SchedulerCreateJob 创建任务配置
func (s *SchedulerService) SchedulerCreateJob(ctx context.Context, req *proto.SchedulerCreateJobRequest) (*proto.SchedulerCreateJobResponse, error) {
	if req.JobName == "" {
		return nil, fmt.Errorf("job name is required")
	}
	if s.jobRepo == nil {
		return nil, fmt.Errorf("job repository not configured")
	}

	cfg := &models.JobConfigModel{
		JobName:    req.JobName,
		TaskConfig: req.TaskConfig,
		ExtraData:  req.ExtraData,
	}

	if err := s.jobRepo.Create(ctx, cfg); err != nil {
		return nil, fmt.Errorf("failed to create job config: %w", err)
	}

	saved, err := s.jobRepo.GetByJobName(ctx, req.JobName)
	if err != nil {
		return &proto.SchedulerCreateJobResponse{Job: s.jobConfigToProto(cfg)}, nil
	}

	return &proto.SchedulerCreateJobResponse{
		Job: s.jobConfigToProto(saved),
	}, nil
}

// SchedulerUpdateJob 更新任务配置
func (s *SchedulerService) SchedulerUpdateJob(ctx context.Context, req *proto.SchedulerUpdateJobRequest) (*proto.SchedulerUpdateJobResponse, error) {
	if req.JobName == "" {
		return nil, fmt.Errorf("job name is required")
	}
	if s.jobRepo == nil {
		return nil, fmt.Errorf("job repository not configured")
	}

	cfg, err := s.jobRepo.GetByJobName(ctx, req.JobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get job config: %w", err)
	}

	if req.TaskConfig != "" {
		cfg.TaskConfig = req.TaskConfig
	}
	if req.ExtraData != "" {
		cfg.ExtraData = req.ExtraData
	}

	if err := s.jobRepo.Update(ctx, cfg); err != nil {
		return nil, fmt.Errorf("failed to update job config: %w", err)
	}

	return &proto.SchedulerUpdateJobResponse{
		Job: s.jobConfigToProto(cfg),
	}, nil
}

// SchedulerDeleteJob 删除任务配置
func (s *SchedulerService) SchedulerDeleteJob(ctx context.Context, req *proto.SchedulerDeleteJobRequest) (*proto.SchedulerDeleteJobResponse, error) {
	if req.JobName == "" {
		return nil, fmt.Errorf("job name is required")
	}

	if entry, ok := s.scheduler.GetEntry(req.JobName); ok && entry.Config.Enabled {
		if !req.Force {
			return nil, fmt.Errorf("job %s is still running, use force=true to delete", req.JobName)
		}
		if err := s.scheduler.UnregisterJob(req.JobName); err != nil {
			return nil, fmt.Errorf("failed to unregister job: %w", err)
		}
	}

	if s.jobRepo != nil {
		if err := s.jobRepo.Delete(ctx, req.JobName); err != nil {
			return nil, fmt.Errorf("failed to delete job config: %w", err)
		}
	}

	return &proto.SchedulerDeleteJobResponse{
		Success: true,
		Message: fmt.Sprintf("job %s deleted successfully", req.JobName),
	}, nil
}

// SchedulerPauseJob 暂停任务
func (s *SchedulerService) SchedulerPauseJob(ctx context.Context, req *proto.SchedulerPauseJobRequest) (*proto.SchedulerPauseJobResponse, error) {
	if req.JobName == "" {
		return nil, fmt.Errorf("job name is required")
	}

	if _, ok := s.scheduler.GetEntry(req.JobName); !ok {
		return nil, fmt.Errorf("job %s not found", req.JobName)
	}

	if err := s.scheduler.UnregisterJob(req.JobName); err != nil {
		return nil, fmt.Errorf("failed to pause job %s: %w", req.JobName, err)
	}

	s.logger.InfoKV("Job paused", "job_name", req.JobName)

	return &proto.SchedulerPauseJobResponse{
		Success: true,
		Message: fmt.Sprintf("job %s paused successfully", req.JobName),
	}, nil
}

// SchedulerResumeJob 恢复任务
func (s *SchedulerService) SchedulerResumeJob(ctx context.Context, req *proto.SchedulerResumeJobRequest) (*proto.SchedulerResumeJobResponse, error) {
	if req.JobName == "" {
		return nil, fmt.Errorf("job name is required")
	}

	if s.jobRepo == nil {
		return nil, fmt.Errorf("job repository not configured")
	}

	cfg, err := s.jobRepo.GetByJobName(ctx, req.JobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get job config for resume: %w", err)
	}

	taskCfg, err := cfg.ToTaskCfg()
	if err != nil || taskCfg == nil {
		return nil, fmt.Errorf("failed to parse task config for job %s", req.JobName)
	}

	taskCfg.Enabled = true

	if _, ok := s.scheduler.GetEntry(req.JobName); ok {
		return &proto.SchedulerResumeJobResponse{
			Success: true,
			Message: fmt.Sprintf("job %s is already active", req.JobName),
		}, nil
	}

	s.logger.InfoKV("Job resumed", "job_name", req.JobName)

	return &proto.SchedulerResumeJobResponse{
		Success: true,
		Message: fmt.Sprintf("job %s config updated, will be loaded on next config reload", req.JobName),
	}, nil
}

// SchedulerTriggerJob 觋动触发任务
func (s *SchedulerService) SchedulerTriggerJob(ctx context.Context, req *proto.SchedulerTriggerJobRequest) (*proto.SchedulerTriggerJobResponse, error) {
	if req.JobName == "" {
		return nil, fmt.Errorf("job name is required")
	}

	if _, ok := s.scheduler.GetEntry(req.JobName); !ok {
		return nil, fmt.Errorf("job %s not found", req.JobName)
	}

	go func() {
		execCtx := context.Background()
		if err := s.scheduler.TriggerManual(execCtx, req.JobName); err != nil {
			s.logger.WarnKV("Manual trigger failed", "job", req.JobName, "error", err)
		}
	}()

	return &proto.SchedulerTriggerJobResponse{
		Accepted: true,
		Message:  fmt.Sprintf("job %s trigger accepted", req.JobName),
	}, nil
}

// SchedulerListExecutions 获取任务执行记录
func (s *SchedulerService) SchedulerListExecutions(ctx context.Context, req *proto.SchedulerListExecutionsRequest) (*proto.SchedulerListExecutionsResponse, error) {
	if s.snapshotRepo == nil {
		return nil, fmt.Errorf("snapshot repository not configured")
	}

	filter := repository.SnapshotFilter{
		Page:     1,
		PageSize: 50,
	}

	if req.JobName != "" {
		filter.JobID = req.JobName
	}
	if req.NodeId != "" {
		filter.NodeID = req.NodeId
	}
	if req.FilterStatus != proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_UNSPECIFIED {
		status := protoExecutionStatusToModel(req.FilterStatus)
		filter.Status = &status
	}
	if req.StartTimeMs > 0 {
		filter.FromTime = time.UnixMilli(req.StartTimeMs)
	}
	if req.EndTimeMs > 0 {
		filter.ToTime = time.UnixMilli(req.EndTimeMs)
	}

	if req.PageSize > 0 {
		filter.PageSize = int(req.PageSize)
	}
	if req.PageToken != "" {
		if page, err := parseInt(req.PageToken); err == nil && page > 0 {
			filter.Page = page
		}
	}

	snapshots, err := s.snapshotRepo.List(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to list executions: %w", err)
	}

	executions := make([]*proto.SchedulerExecutionSnapshotInfo, 0, len(snapshots))
	for _, snap := range snapshots {
		executions = append(executions, s.snapshotToProto(snap))
	}

	totalCount := s.snapshotRepo.Count(ctx)
	nextToken := ""
	if filter.Page*filter.PageSize < int(totalCount) {
		nextToken = fmt.Sprintf("%d", filter.Page+1)
	}

	return &proto.SchedulerListExecutionsResponse{
		Executions:    executions,
		TotalCount:    int32(totalCount),
		NextPageToken: nextToken,
	}, nil
}

// SchedulerGetExecution 获取任务执行记录详情
func (s *SchedulerService) SchedulerGetExecution(ctx context.Context, req *proto.SchedulerGetExecutionRequest) (*proto.SchedulerGetExecutionResponse, error) {
	if req.TraceId == "" {
		return nil, fmt.Errorf("trace id is required")
	}
	if s.snapshotRepo == nil {
		return nil, fmt.Errorf("snapshot repository not configured")
	}

	snapshot, err := s.snapshotRepo.Get(ctx, req.TraceId)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution: %w", err)
	}

	return &proto.SchedulerGetExecutionResponse{
		Execution: s.snapshotToProto(snapshot),
	}, nil
}

// SchedulerAbortExecution 中止任务执行
func (s *SchedulerService) SchedulerAbortExecution(ctx context.Context, req *proto.SchedulerAbortExecutionRequest) (*proto.SchedulerAbortExecutionResponse, error) {
	if req.TraceId == "" {
		return nil, fmt.Errorf("trace id is required")
	}
	if s.snapshotRepo == nil {
		return nil, fmt.Errorf("snapshot repository not configured")
	}

	snapshot, err := s.snapshotRepo.Get(ctx, req.TraceId)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution: %w", err)
	}

	if snapshot.Status.IsTerminal() {
		return nil, fmt.Errorf("cannot abort execution in terminal state: %s", snapshot.Status.String())
	}

	snapshot.PreviousStatus = snapshot.Status
	snapshot.Status = models.StatusAborted
	now := time.Now()
	snapshot.EndTime = &now

	if err := s.snapshotRepo.Save(ctx, snapshot); err != nil {
		return nil, fmt.Errorf("failed to update execution status: %w", err)
	}

	return &proto.SchedulerAbortExecutionResponse{
		Success: true,
		Message: fmt.Sprintf("execution %s aborted", req.TraceId),
	}, nil
}

// SchedulerRetryExecution 重试任务执行
func (s *SchedulerService) SchedulerRetryExecution(ctx context.Context, req *proto.SchedulerRetryExecutionRequest) (*proto.SchedulerRetryExecutionResponse, error) {
	if req.TraceId == "" {
		return nil, fmt.Errorf("trace id is required")
	}
	if s.snapshotRepo == nil {
		return nil, fmt.Errorf("snapshot repository not configured")
	}

	snapshot, err := s.snapshotRepo.Get(ctx, req.TraceId)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution: %w", err)
	}

	if !snapshot.Status.IsTerminal() {
		return nil, fmt.Errorf("cannot retry execution in non-terminal state: %s", snapshot.Status.String())
	}

	if _, ok := s.scheduler.GetEntry(snapshot.JobName); ok {
		go func() {
			execCtx := context.Background()
			if err := s.scheduler.TriggerManual(execCtx, snapshot.JobName); err != nil {
				s.logger.WarnKV("Retry trigger failed", "job", snapshot.JobName, "error", err)
			}
		}()
	}

	return &proto.SchedulerRetryExecutionResponse{
		Accepted: true,
		Message:  fmt.Sprintf("retry accepted for job %s", snapshot.JobName),
	}, nil
}

// SchedulerGetThrottleConfig 获取任务节流配置
func (s *SchedulerService) SchedulerGetThrottleConfig(ctx context.Context, req *proto.SchedulerGetThrottleConfigRequest) (*proto.SchedulerGetThrottleConfigResponse, error) {
	if req.JobName == "" {
		return nil, fmt.Errorf("job name is required")
	}

	return &proto.SchedulerGetThrottleConfigResponse{
		ThrottleConfig: defaultThrottleConfigProto(),
	}, nil
}

// SchedulerUpdateThrottleConfig 更新任务节流配置
func (s *SchedulerService) SchedulerUpdateThrottleConfig(ctx context.Context, req *proto.SchedulerUpdateThrottleConfigRequest) (*proto.SchedulerUpdateThrottleConfigResponse, error) {
	if req.JobName == "" {
		return nil, fmt.Errorf("job name is required")
	}
	if req.ThrottleConfig == nil {
		return nil, fmt.Errorf("throttle config is required")
	}

	if _, ok := s.scheduler.GetEntry(req.JobName); !ok {
		return nil, fmt.Errorf("job %s not found", req.JobName)
	}

	cfg := protoThrottleConfigToModel(req.ThrottleConfig)
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid throttle config: %w", err)
	}

	protector := s.scheduler.GetJobProtector(req.JobName)
	if protector == nil {
		protector = NewJobProtector(req.JobName)
		if err := s.scheduler.SetJobProtector(req.JobName, protector); err != nil {
			return nil, fmt.Errorf("failed to set job protector: %w", err)
		}
	}

	s.logger.InfoKV("Throttle config updated", "job_name", req.JobName)

	return &proto.SchedulerUpdateThrottleConfigResponse{
		Success: true,
		Message: fmt.Sprintf("throttle config updated for job %s", req.JobName),
	}, nil
}

// jobConfigToProto 转换任务配置为Proto格式
func (s *SchedulerService) jobConfigToProto(cfg *models.JobConfigModel) *proto.SchedulerJobConfigInfo {
	if cfg == nil {
		return nil
	}

	info := &proto.SchedulerJobConfigInfo{
		Id:         cfg.Id,
		JobName:    cfg.JobName,
		TaskConfig: cfg.TaskConfig,
		ExtraData:  cfg.ExtraData,
	}

	if cfg.LastExecutedAt != nil {
		info.LastExecutedAt = timestamppb.New(*cfg.LastExecutedAt)
	}
	if cfg.NextExecutionAt != nil {
		info.NextExecutionAt = timestamppb.New(*cfg.NextExecutionAt)
	}
	if !cfg.CreatedAt.IsZero() {
		info.CreatedAt = timestamppb.New(cfg.CreatedAt)
	}
	if !cfg.UpdatedAt.IsZero() {
		info.UpdatedAt = timestamppb.New(cfg.UpdatedAt)
	}

	taskCfg, err := cfg.ToTaskCfg()
	if err == nil && taskCfg != nil {
		info.CronSpec = taskCfg.CronSpec
		info.JobType = modelJobTypeToProto(models.JobTypeCron)
	}

	if entry, ok := s.scheduler.GetEntry(cfg.JobName); ok {
		info.State = proto.SchedulerJobState_SCHEDULER_JOB_STATE_ACTIVE
		if !entry.Config.Enabled {
			info.State = proto.SchedulerJobState_SCHEDULER_JOB_STATE_DISABLED
		}
	} else {
		info.State = proto.SchedulerJobState_SCHEDULER_JOB_STATE_DISABLED
	}

	return info
}

// snapshotToProto 转换任务执行记录为Proto格式
func (s *SchedulerService) snapshotToProto(snap *models.ExecutionSnapshotModel) *proto.SchedulerExecutionSnapshotInfo {
	if snap == nil {
		return nil
	}

	info := &proto.SchedulerExecutionSnapshotInfo{
		Id:               snap.ID,
		TraceId:          snap.TraceID,
		JobId:            snap.JobID,
		JobName:          snap.JobName,
		JobType:          modelJobTypeToProto(snap.JobType),
		NodeId:           snap.NodeID,
		Status:           modelExecutionStatusToProto(snap.Status),
		PreviousStatus:   modelExecutionStatusToProto(snap.PreviousStatus),
		ExecFrequency:    int32(snap.ExecFrequency),
		FailureFrequency: int32(snap.FailureFrequency),
		SuccessFrequency: int32(snap.SuccessFrequency),
		Error:            snap.Error,
		Result:           snap.Result,
		StackTrace:       snap.StackTrace,
		LogsJson:         snap.LogsJSON,
		MetricsJson:      snap.MetricsJSON,
		LabelsJson:       snap.LabelsJSON,
	}

	if !snap.TriggerAt.IsZero() {
		info.TriggerAt = timestamppb.New(snap.TriggerAt)
	}
	if !snap.StartTime.IsZero() {
		info.StartTime = timestamppb.New(snap.StartTime)
	}
	if snap.EndTime != nil {
		info.EndTime = timestamppb.New(*snap.EndTime)
	}
	if !snap.CreatedAt.IsZero() {
		info.CreatedAt = timestamppb.New(snap.CreatedAt)
	}
	if !snap.UpdatedAt.IsZero() {
		info.UpdatedAt = timestamppb.New(snap.UpdatedAt)
	}

	return info
}

// modelJobTypeToProto 转换任务类型为Proto格式
func modelJobTypeToProto(t models.JobType) proto.SchedulerJobType {
	switch t {
	case models.JobTypeCron:
		return proto.SchedulerJobType_SCHEDULER_JOB_TYPE_CRON
	case models.JobTypePeriodic:
		return proto.SchedulerJobType_SCHEDULER_JOB_TYPE_PERIODIC
	case models.JobTypeManual:
		return proto.SchedulerJobType_SCHEDULER_JOB_TYPE_MANUAL
	case models.JobTypeSyncx:
		return proto.SchedulerJobType_SCHEDULER_JOB_TYPE_SYNCX
	default:
		return proto.SchedulerJobType_SCHEDULER_JOB_TYPE_UNSPECIFIED
	}
}

// modelExecutionStatusToProto 转换任务执行状态为Proto格式
func modelExecutionStatusToProto(status models.ExecutionStatus) proto.SchedulerExecutionStatus {
	switch status {
	case models.StatusPending:
		return proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_PENDING
	case models.StatusRunning:
		return proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_RUNNING
	case models.StatusSuccess:
		return proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_SUCCESS
	case models.StatusFailure:
		return proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_FAILURE
	case models.StatusTimeout:
		return proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_TIMEOUT
	case models.StatusRetrying:
		return proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_RETRYING
	case models.StatusAborted:
		return proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_ABORTED
	case models.StatusSystemAborted:
		return proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_SYSTEM_ABORTED
	case models.StatusSkipped:
		return proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_SKIPPED
	default:
		return proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_UNSPECIFIED
	}
}

// protoExecutionStatusToModel 转换任务执行状态为模型格式
func protoExecutionStatusToModel(status proto.SchedulerExecutionStatus) models.ExecutionStatus {
	switch status {
	case proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_PENDING:
		return models.StatusPending
	case proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_RUNNING:
		return models.StatusRunning
	case proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_SUCCESS:
		return models.StatusSuccess
	case proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_FAILURE:
		return models.StatusFailure
	case proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_TIMEOUT:
		return models.StatusTimeout
	case proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_RETRYING:
		return models.StatusRetrying
	case proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_ABORTED:
		return models.StatusAborted
	case proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_SYSTEM_ABORTED:
		return models.StatusSystemAborted
	case proto.SchedulerExecutionStatus_SCHEDULER_EXECUTION_STATUS_SKIPPED:
		return models.StatusSkipped
	default:
		return models.StatusPending
	}
}

// throttleConfigToProto 转换任务节流配置为Proto格式
func throttleConfigToProto(cfg *models.ThrottleConfig) *proto.SchedulerThrottleConfigInfo {
	if cfg == nil {
		return nil
	}
	return &proto.SchedulerThrottleConfigInfo{
		CooldownDurationMs: cfg.CooldownDuration.Milliseconds(),
		SleepDurationMs:    cfg.SleepDuration.Milliseconds(),
		MaxConcurrent:      int32(cfg.MaxConcurrent),
		RateLimit:          int32(cfg.RateLimit),
		BurstSize:          int32(cfg.BurstSize),
		MaxQueueSize:       int32(cfg.MaxQueueSize),
		QueueTimeoutMs:     cfg.QueueTimeout.Milliseconds(),
		MaxRetries:         int32(cfg.MaxRetries),
		RetryDelayMs:       cfg.RetryDelay.Milliseconds(),
		RetryBackoff:       cfg.RetryBackoff,
		MaxRetryDelayMs:    cfg.MaxRetryDelay.Milliseconds(),
	}
}

// protoThrottleConfigToModel 转换任务节流配置为模型格式
func protoThrottleConfigToModel(cfg *proto.SchedulerThrottleConfigInfo) *models.ThrottleConfig {
	if cfg == nil {
		return models.NewThrottleConfig()
	}
	return &models.ThrottleConfig{
		CooldownDuration: time.Duration(cfg.CooldownDurationMs) * time.Millisecond,
		SleepDuration:    time.Duration(cfg.SleepDurationMs) * time.Millisecond,
		MaxConcurrent:    int(cfg.MaxConcurrent),
		RateLimit:        int(cfg.RateLimit),
		BurstSize:        int(cfg.BurstSize),
		MaxQueueSize:     int(cfg.MaxQueueSize),
		QueueTimeout:     time.Duration(cfg.QueueTimeoutMs) * time.Millisecond,
		MaxRetries:       int(cfg.MaxRetries),
		RetryDelay:       time.Duration(cfg.RetryDelayMs) * time.Millisecond,
		RetryBackoff:     cfg.RetryBackoff,
		MaxRetryDelay:    time.Duration(cfg.MaxRetryDelayMs) * time.Millisecond,
	}
}

// defaultThrottleConfigProto 转换默认任务节流配置为Proto格式
func defaultThrottleConfigProto() *proto.SchedulerThrottleConfigInfo {
	return throttleConfigToProto(models.NewThrottleConfig())
}

// matchJobState 匹配任务状态是否符合预期
func (s *SchedulerService) matchJobState(cfg *models.JobConfigModel, state proto.SchedulerJobState) bool {
	entry, hasEntry := s.scheduler.GetEntry(cfg.JobName)

	switch state {
	case proto.SchedulerJobState_SCHEDULER_JOB_STATE_ACTIVE:
		return hasEntry && entry.Config.Enabled
	case proto.SchedulerJobState_SCHEDULER_JOB_STATE_PAUSED:
		return false
	case proto.SchedulerJobState_SCHEDULER_JOB_STATE_DISABLED:
		return !hasEntry || !entry.Config.Enabled
	case proto.SchedulerJobState_SCHEDULER_JOB_STATE_ERROR:
		return false
	default:
		return true
	}
}

// containsIgnoreCase 忽略大小写包含子字符串
func containsIgnoreCase(s, substr string) bool {
	sLower := strings.ToLower(s)
	substrLower := strings.ToLower(substr)
	return strings.Contains(sLower, substrLower)
}

// parseInt 解析整数
func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
