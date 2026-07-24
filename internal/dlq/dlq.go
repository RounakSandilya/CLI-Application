// Package dlq implements Dead Letter Queue operations: listing permanently
// failed jobs and requeueing them for another attempt.
package dlq

import (
	"fmt"
	"time"

	"queuectl/internal/job"
	"queuectl/internal/store"
)

// List returns all jobs currently in the "dead" state.
func List(s *store.Store) ([]*job.Job, error) {
	return s.List(job.StateDead)
}

// Retry resets a dead job back to pending with a clean slate (0 attempts,
// no error, no backoff delay) so a worker will pick it up again.
func Retry(s *store.Store, id string) error {
	j, err := s.Get(id)
	if err != nil {
		return err
	}
	if j.State != job.StateDead {
		return fmt.Errorf("job %s is not in the DLQ (current state: %s)", id, j.State)
	}
	j.State = job.StatePending
	j.Attempts = 0
	j.LastError = ""
	j.NextRunAt = time.Time{}
	j.UpdatedAt = time.Now()
	return s.Update(j)
}
