package repository

import (
	"context"
	"errors"
	"time"

	"github.com/alexanderritik/byot/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrFenced is returned when a write is rejected because the job's claim_epoch
// no longer matches — another actor (stale sweep or re-claim) owns the job.
var ErrFenced = errors.New("job write rejected: fenced out by newer claim")

type JobRepository struct {
	pool *pgxpool.Pool
}

func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

func (r *JobRepository) Create(ctx context.Context, job *model.Job) error {
	query := `
		INSERT INTO jobs (uuid, test_id, status, queued_at, trigger)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := r.pool.Exec(ctx, query, job.UUID, job.TestID, job.Status, job.QueuedAt, job.Trigger)
	return err
}

func (r *JobRepository) ListActiveForTest(ctx context.Context, testID string) ([]*model.Job, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT uuid, test_id, status, trigger, queued_at, started_at, finished_at, worker_id, error_message
		 FROM jobs WHERE test_id = $1 AND status IN ('queued', 'running')
		 ORDER BY queued_at ASC`,
		testID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*model.Job
	for rows.Next() {
		var job model.Job
		if err := rows.Scan(
			&job.UUID,
			&job.TestID,
			&job.Status,
			&job.Trigger,
			&job.QueuedAt,
			&job.StartedAt,
			&job.FinishedAt,
			&job.WorkerID,
			&job.ErrorMessage,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, &job)
	}
	return jobs, rows.Err()
}

func (r *JobRepository) CancelActiveForTest(ctx context.Context, testID string, reason string) (int, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET status = 'cancelled',
		     finished_at = NOW(),
		     error_message = $2
		 WHERE test_id = $1 AND status IN ('queued', 'running')`,
		testID, reason,
	)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (r *JobRepository) HasActiveJob(ctx context.Context, testID string) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM jobs WHERE test_id = $1 AND status IN ('queued', 'running')
		)`,
		testID,
	).Scan(&exists)
	return exists, err
}

func (r *JobRepository) GetByID(ctx context.Context, id string) (*model.Job, error) {
	query := `
		SELECT uuid, test_id, status, trigger, queued_at, started_at, finished_at, worker_id, error_message
		FROM jobs WHERE uuid = $1
	`
	row := r.pool.QueryRow(ctx, query, id)

	var job model.Job
	err := row.Scan(
		&job.UUID,
		&job.TestID,
		&job.Status,
		&job.Trigger,
		&job.QueuedAt,
		&job.StartedAt,
		&job.FinishedAt,
		&job.WorkerID,
		&job.ErrorMessage,
	)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *JobRepository) Dequeue(ctx context.Context, workerID string) (*model.Job, error) {
	query := `
		UPDATE jobs
		SET status          = 'running',
		    started_at      = NOW(),
		    worker_id       = $1,
		    claim_epoch     = claim_epoch + 1,
		    lease_expires_at = NOW() + interval '30 seconds'
		WHERE uuid = (
			SELECT uuid FROM jobs
			WHERE status = 'queued'
			ORDER BY queued_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING uuid, test_id, status, trigger, queued_at, started_at, finished_at,
		          worker_id, error_message, claim_epoch, lease_expires_at
	`

	row := r.pool.QueryRow(ctx, query, workerID)

	var job model.Job
	err := row.Scan(
		&job.UUID,
		&job.TestID,
		&job.Status,
		&job.Trigger,
		&job.QueuedAt,
		&job.StartedAt,
		&job.FinishedAt,
		&job.WorkerID,
		&job.ErrorMessage,
		&job.ClaimEpoch,
		&job.LeaseExpiresAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &job, nil
}

// UpdateStatus finalises a job. epoch must match the claim_epoch the worker
// received at dequeue; if it doesn't (stale sweep already bumped the epoch)
// the update is a no-op and ErrFenced is returned so the caller can log and
// discard rather than treating it as a hard error.
func (r *JobRepository) UpdateStatus(ctx context.Context, jobID string, status string, errorMessage *string, epoch int) error {
	query := `
		UPDATE jobs
		SET status        = $1,
		    finished_at   = NOW(),
		    error_message = $2
		WHERE uuid        = $3
		  AND claim_epoch = $4
	`
	tag, err := r.pool.Exec(ctx, query, status, errorMessage, jobID, epoch)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrFenced
	}
	return nil
}

// Heartbeat extends the lease on a running job. Returns ErrFenced if the
// epoch no longer matches (job was swept or re-claimed while this worker
// was running), signalling the worker to stop and discard its result.
func (r *JobRepository) Heartbeat(ctx context.Context, jobID string, workerID string, epoch int) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE jobs
		SET lease_expires_at = NOW() + interval '30 seconds'
		WHERE uuid        = $1
		  AND worker_id   = $2
		  AND claim_epoch = $3
		  AND status      = 'running'
	`, jobID, workerID, epoch)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrFenced
	}
	return nil
}

func (r *JobRepository) ListByTestID(ctx context.Context, testID string) ([]*model.Job, error) {
	query := `
		SELECT uuid, test_id, status, trigger, queued_at, started_at, finished_at, worker_id, error_message
		FROM jobs WHERE test_id = $1
		ORDER BY queued_at DESC
	`
	rows, err := r.pool.Query(ctx, query, testID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*model.Job
	for rows.Next() {
		var job model.Job
		err := rows.Scan(
			&job.UUID,
			&job.TestID,
			&job.Status,
			&job.Trigger,
			&job.QueuedAt,
			&job.StartedAt,
			&job.FinishedAt,
			&job.WorkerID,
			&job.ErrorMessage,
		)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

// FailStale marks 'running' jobs as failed and bumps their claim_epoch,
// fencing out any zombie worker that is still alive. Two conditions trigger
// recovery:
//
//  1. lease_expires_at < NOW() — worker missed heartbeats (normal path for
//     jobs created after migration 015). Recovery happens within ~30 s of
//     the crash.
//  2. Fallback for jobs created before migration 015 (lease_expires_at IS
//     NULL): the old timeout+grace window is used so those jobs are never
//     stuck forever.
//
// grace is only applied to the fallback path.
func (r *JobRepository) FailStale(ctx context.Context, grace time.Duration) ([]string, error) {
	rows, err := r.pool.Query(ctx, `
		UPDATE jobs j
		SET status        = 'failed',
		    finished_at   = NOW(),
		    error_message = 'job abandoned: worker stopped or crashed before finishing',
		    claim_epoch   = claim_epoch + 1
		FROM tests t
		WHERE j.test_id = t.uuid
		  AND j.status = 'running'
		  AND (
		        j.lease_expires_at < NOW()
		        OR (
		              j.lease_expires_at IS NULL
		              AND j.started_at + make_interval(secs => t.timeout_seconds)
		                             + make_interval(secs => $1) < NOW()
		           )
		      )
		RETURNING j.uuid`, grace.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
