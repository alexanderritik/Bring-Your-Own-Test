package queue

import (
	"context"
	"time"

	"github.com/alexanderritik/byot/model"
	"github.com/alexanderritik/byot/repository"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	TriggerManual    = "manual"
	TriggerScheduled = "scheduled"
)

type Queue struct {
	jobRepo *repository.JobRepository
}

func NewQueue(jobRepo *repository.JobRepository) *Queue {
	return &Queue{jobRepo: jobRepo}
}

func (q *Queue) Enqueue(ctx context.Context, testID string, trigger string) (*model.Job, error) {
	if trigger == "" {
		trigger = TriggerManual
	}

	job := &model.Job{
		UUID:     uuid.NewString(),
		TestID:   testID,
		Status:   "queued",
		Trigger:  trigger,
		QueuedAt: time.Now().UTC(),
	}

	if err := q.jobRepo.Create(ctx, job); err != nil {
		log.Error().Err(err).Str("test_id", testID).Msg("failed to enqueue job")
		return nil, err
	}

	log.Info().Str("job_id", job.UUID).Str("test_id", testID).Str("trigger", trigger).Msg("job enqueued")
	return job, nil
}

func (q *Queue) Dequeue(ctx context.Context, workerID string) (*model.Job, error) {
	job, err := q.jobRepo.Dequeue(ctx, workerID)
	if err != nil {
		return nil, err
	}

	if job == nil {
		return nil, nil
	}

	log.Info().Str("job_id", job.UUID).Str("worker_id", workerID).Msg("job dequeued")
	return job, nil
}

// Complete finalises a job. epoch is the claim_epoch the worker received at
// dequeue; if the epoch no longer matches (stale sweep ran while the worker
// was executing) the update is silently dropped and ErrFenced is returned.
func (q *Queue) Complete(ctx context.Context, jobID string, epoch int, status string, errorMessage *string) error {
	if err := q.jobRepo.UpdateStatus(ctx, jobID, status, errorMessage, epoch); err != nil {
		log.Error().Err(err).Str("job_id", jobID).Msg("failed to update job status")
		return err
	}

	log.Info().Str("job_id", jobID).Str("status", status).Msg("job completed")
	return nil
}

// Heartbeat extends the lease on a running job. Returns repository.ErrFenced
// if the job was swept or re-claimed since this worker started it.
func (q *Queue) Heartbeat(ctx context.Context, jobID string, workerID string, epoch int) error {
	return q.jobRepo.Heartbeat(ctx, jobID, workerID, epoch)
}

func (q *Queue) GetStatus(ctx context.Context, jobID string) (*model.Job, error) {
	return q.jobRepo.GetByID(ctx, jobID)
}

func (q *Queue) ListActiveForTest(ctx context.Context, testID string) ([]*model.Job, error) {
	return q.jobRepo.ListActiveForTest(ctx, testID)
}

func (q *Queue) CancelActiveForTest(ctx context.Context, testID string, reason string) (int, error) {
	return q.jobRepo.CancelActiveForTest(ctx, testID, reason)
}

func (q *Queue) FailStale(ctx context.Context, grace time.Duration) ([]string, error) {
	return q.jobRepo.FailStale(ctx, grace)
}
