package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rishansujesh/job-scheduler/internal/api/proto"
	"github.com/rishansujesh/job-scheduler/internal/jobs"
	redisx "github.com/rishansujesh/job-scheduler/internal/redis"
)

type Server struct {
	proto.UnimplementedJobServiceServer
	DB      *sql.DB
	Store   *jobs.Store
	RDB     redisClient
	Streams redisx.StreamsConfig
}

type redisClient interface {
	// minimal subset we use
}

func New(db *sql.DB, rdb any, streams redisx.StreamsConfig) *Server {
	return &Server{
		DB:      db,
		Store:   jobs.NewStore(db),
		RDB:     nil, // not used directly; we call redisx helpers with *redis.Client from main.go
		Streams: streams,
	}
}

func parseJSONMap(s string) (map[string]any, error) {
	if s == "" {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	return m, nil
}

/******** Jobs ********/

func (s *Server) CreateJob(ctx context.Context, req *proto.CreateJobRequest) (*proto.CreateJobResponse, error) {
	if req.GetName() == "" || req.GetHandler() == "" || req.GetType() == "" {
		return nil, status.Error(codes.InvalidArgument, "name, type, handler required")
	}
	args, err := parseJSONMap(req.GetArgsJson())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "args_json: %v", err)
	}
	j, err := s.Store.CreateJob(ctx, jobs.CreateJobParams{
		Name: req.GetName(), Type: req.GetType(), Handler: req.GetHandler(), Args: args, Enabled: req.GetEnabled(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create: %v", err)
	}
	return &proto.CreateJobResponse{Job: toProtoJob(*j)}, nil
}

func (s *Server) ListJobs(ctx context.Context, req *proto.ListJobsRequest) (*proto.ListJobsResponse, error) {
	jobsList, err := s.Store.ListJobs(ctx, jobs.ListJobsParams{Limit: int(req.GetLimit()), Offset: int(req.GetOffset())})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	out := make([]*proto.Job, 0, len(jobsList))
	for _, j := range jobsList {
		out = append(out, toProtoJob(j))
	}
	return &proto.ListJobsResponse{Jobs: out}, nil
}

func (s *Server) UpdateJob(ctx context.Context, req *proto.UpdateJobRequest) (*proto.UpdateJobResponse, error) {
	var name *string
	if req.Name != nil {
		name = req.Name
	}
	var args *map[string]any
	if req.ArgsJson != nil {
		m, err := parseJSONMap(req.GetArgsJson())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "args_json: %v", err)
		}
		args = &m
	}
	var enabled *bool
	if req.Enabled != nil {
		enabled = req.Enabled
	}

	j, err := s.Store.UpdateJob(ctx, jobs.UpdateJobParams{
		ID: req.GetId(), Name: name, Args: args, Enabled: enabled,
	})
	if errors.Is(err, jobs.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update: %v", err)
	}
	return &proto.UpdateJobResponse{Job: toProtoJob(*j)}, nil
}

func (s *Server) DeleteJob(ctx context.Context, req *proto.DeleteJobRequest) (*proto.DeleteJobResponse, error) {
	if err := s.Store.DisableJob(ctx, req.GetId()); errors.Is(err, jobs.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "job not found")
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &proto.DeleteJobResponse{}, nil
}

/******** Ad-hoc run ********/

func (s *Server) RunJob(ctx context.Context, req *proto.RunJobRequest) (*proto.RunJobResponse, error) {
	// Load job to get default args
	rows, err := s.DB.QueryContext(ctx, `
SELECT id, name, type, handler, args, enabled, created_at, updated_at
FROM jobs WHERE id = $1 AND enabled = true`, req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load job: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, status.Error(codes.NotFound, "job not found or disabled")
	}
	var j jobs.Job
	var argsRaw []byte
	if err := rows.Scan(&j.ID, &j.Name, &j.Type, &j.Handler, &argsRaw, &j.Enabled, &j.CreatedAt, &j.UpdatedAt); err != nil {
		return nil, status.Errorf(codes.Internal, "scan: %v", err)
	}
	_ = json.Unmarshal(argsRaw, &j.Args)

	override, err := parseJSONMap(req.GetArgsJson())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "args_json: %v", err)
	}
	// merge override onto default
	if len(override) > 0 {
		for k, v := range override {
			j.Args[k] = v
		}
	}

	runID := uuid.NewString()
	now := time.Now().UTC()
	idKey, err := jobs.ComputeIdempotencyKey(j.ID, now, j.Args)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idempotency: %v", err)
	}

	// Insert queued run record
	if _, err := s.Store.InsertRun(ctx, jobs.InsertRunParams{
		JobID: j.ID, RunID: runID, Status: jobs.StatusQueued, IdempotencyKey: idKey,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "insert run: %v", err)
	}

	// Enqueue to adhoc stream
	payload := map[string]any{
		"run_id":  runID,
		"job_id":  j.ID,
		"handler": j.Handler,
		"args":    j.Args,
	}
	// NOTE: XAddJSON expects *redis.Client; we will call it from main.go using the real client.
	// Here we just return run_id; the enqueue is done in main.go handler.
	_ = payload

	return &proto.RunJobResponse{RunId: runID}, nil
}

/******** Schedules ********/

func (s *Server) CreateSchedule(ctx context.Context, req *proto.CreateScheduleRequest) (*proto.CreateScheduleResponse, error) {
	next, err := time.Parse(time.RFC3339, req.GetNextRunAt())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "next_run_at: %v", err)
	}
	sc, err := s.Store.CreateSchedule(ctx, jobs.CreateScheduleParams{
		JobID: req.GetJobId(), CronExpr: req.CronExpr, FixedIntervalSeconds: toPtrInt(req.FixedIntervalSeconds),
		NextRunAt: next, Timezone: req.GetTimezone(), Enabled: req.GetEnabled(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create schedule: %v", err)
	}
	return &proto.CreateScheduleResponse{Schedule: toProtoSchedule(*sc)}, nil
}

func (s *Server) ListSchedules(ctx context.Context, req *proto.ListSchedulesRequest) (*proto.ListSchedulesResponse, error) {
	list, err := s.Store.ListSchedules(ctx, jobs.ListSchedulesParams{Limit: int(req.GetLimit()), Offset: int(req.GetOffset())})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list schedules: %v", err)
	}
	out := make([]*proto.Schedule, 0, len(list))
	for _, sc := range list {
		out = append(out, toProtoSchedule(sc))
	}
	return &proto.ListSchedulesResponse{Schedules: out}, nil
}

func (s *Server) UpdateSchedule(ctx context.Context, req *proto.UpdateScheduleRequest) (*proto.UpdateScheduleResponse, error) {
	var next *time.Time
	if req.NextRunAt != nil {
		t, err := time.Parse(time.RFC3339, req.GetNextRunAt())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "next_run_at: %v", err)
		}
		next = &t
	}
	sc, err := s.Store.UpdateSchedule(ctx, jobs.UpdateScheduleParams{
		ID: req.GetId(), CronExpr: req.CronExpr, FixedIntervalSeconds: toPtrInt(req.FixedIntervalSeconds),
		NextRunAt: next, Timezone: req.Timezone, Enabled: req.Enabled,
	})
	if errors.Is(err, jobs.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "schedule not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update schedule: %v", err)
	}
	return &proto.UpdateScheduleResponse{Schedule: toProtoSchedule(*sc)}, nil
}

func (s *Server) DeleteSchedule(ctx context.Context, req *proto.DeleteScheduleRequest) (*proto.DeleteScheduleResponse, error) {
	if err := s.Store.DeleteSchedule(ctx, req.GetId()); errors.Is(err, jobs.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "schedule not found")
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "delete schedule: %v", err)
	}
	return &proto.DeleteScheduleResponse{}, nil
}

/******** Runs ********/

func (s *Server) ListJobRuns(ctx context.Context, req *proto.ListJobRunsRequest) (*proto.ListJobRunsResponse, error) {
	runs, err := s.Store.ListRunsForJob(ctx, req.GetJobId(), int(req.GetLimit()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list runs: %v", err)
	}
	out := make([]*proto.JobRun, 0, len(runs))
	for _, r := range runs {
		out = append(out, toProtoRun(r))
	}
	return &proto.ListJobRunsResponse{Runs: out}, nil
}

/******** Converters ********/

func toProtoJob(j jobs.Job) *proto.Job {
	argsB, _ := json.Marshal(j.Args)
	return &proto.Job{
		Id: j.ID, Name: j.Name, Type: j.Type, Handler: j.Handler,
		ArgsJson: string(argsB), Enabled: j.Enabled,
		CreatedAt: j.CreatedAt.Format(time.RFC3339), UpdatedAt: j.UpdatedAt.Format(time.RFC3339),
	}
}
func toProtoSchedule(sc jobs.Schedule) *proto.Schedule {
	var last *string
	if sc.LastEnqueuedAt != nil {
		v := sc.LastEnqueuedAt.UTC().Format(time.RFC3339)
		last = &v
	}
	return &proto.Schedule{
		Id: sc.ID, JobId: sc.JobID, CronExpr: sc.CronExpr,
		FixedIntervalSeconds: toPtr32(sc.FixedIntervalSeconds),
		NextRunAt:            sc.NextRunAt.UTC().Format(time.RFC3339), Timezone: sc.Timezone,
		LastEnqueuedAt: last, Enabled: sc.Enabled,
	}
}
func toProtoRun(r jobs.JobRun) *proto.JobRun {
	var fin *string
	if r.FinishedAt != nil {
		v := r.FinishedAt.UTC().Format(time.RFC3339)
		fin = &v
	}
	var errText *string
	if r.ErrorText != nil {
		errText = r.ErrorText
	}
	var worker *string
	if r.WorkerID != nil {
		worker = r.WorkerID
	}
	return &proto.JobRun{
		Id: r.ID, JobId: r.JobID, RunId: r.RunID,
		StartedAt: r.StartedAt.UTC().Format(time.RFC3339), FinishedAt: fin,
		Status: string(r.Status), Attempts: int32(r.Attempts),
		ErrorText: errText, WorkerId: worker, IdempotencyKey: r.IdempotencyKey,
	}
}

func toPtrInt(v *int32) *int {
	if v == nil {
		return nil
	}
	i := int(*v)
	return &i
}
func toPtr32(v *int) *int32 {
	if v == nil {
		return nil
	}
	i := int32(*v)
	return &i
}
