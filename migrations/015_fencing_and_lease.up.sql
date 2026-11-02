-- claim_epoch is bumped on every claim (dequeue) and every stale sweep.
-- Workers must present the epoch they received; a mismatched epoch means
-- another actor (stale sweep or a re-claim) already owns the job, so the
-- write is silently dropped. This prevents a zombie worker from overwriting
-- a job that has already been recovered.
ALTER TABLE jobs ADD COLUMN claim_epoch INTEGER NOT NULL DEFAULT 0;

-- lease_expires_at is refreshed by worker heartbeats every ~10 s.
-- sweepStale now uses this expiry instead of the blunt timeout+grace window,
-- so crashed workers are recovered in ~30 s rather than timeout+5 min.
ALTER TABLE jobs ADD COLUMN lease_expires_at TIMESTAMPTZ;
