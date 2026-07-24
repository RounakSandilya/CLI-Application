// Package worker implements a pool of concurrent workers that claim jobs
// from a store, execute them as shell commands, and record the result.
package worker

import (
	"context"
	"errors"
	"log"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"queuectl/internal/config"
	"queuectl/internal/job"
	"queuectl/internal/retry"
	"queuectl/internal/store"
)

// pollInterval is how often an idle worker checks for new work.
const pollInterval = 500 * time.Millisecond

// Pool manages a fixed number of concurrent workers.
type Pool struct {
	store *store.Store
	cfg   config.Config
	count int
	wg    sync.WaitGroup
}

// New creates a Pool of count workers (minimum 1) backed by s and cfg.
func New(s *store.Store, cfg config.Config, count int) *Pool {
	if count < 1 {
		count = 1
	}
	return &Pool{store: s, cfg: cfg, count: count}
}

// Run starts all workers and blocks until ctx is cancelled AND every worker
// has finished its current job. Workers never abandon a job mid-execution -
// they just stop picking up new ones once ctx is done - which is what makes
// this a graceful shutdown.
func (p *Pool) Run(ctx context.Context) {
	for i := 1; i <= p.count; i++ {
		p.wg.Add(1)
		id := "worker-" + strconv.Itoa(i)
		go func() {
			defer p.wg.Done()
			p.loop(ctx, id)
		}()
	}
	p.wg.Wait()
}

func (p *Pool) loop(ctx context.Context, id string) {
	log.Printf("[%s] starting", id)
	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] shutting down", id)
			return
		default:
		}

		j, err := p.store.ClaimNext(id)
		if errors.Is(err, store.ErrNoJob) {
			// Nothing to do right now - wait a bit, but wake up immediately
			// if shutdown is requested instead of always sleeping the full
			// interval.
			select {
			case <-ctx.Done():
				log.Printf("[%s] shutting down", id)
				return
			case <-time.After(pollInterval):
			}
			continue
		}
		if err != nil {
			log.Printf("[%s] error claiming job: %v", id, err)
			time.Sleep(pollInterval)
			continue
		}

		// A job is claimed: run it to completion even if ctx is cancelled
		// mid-flight, so we never abandon in-progress work.
		p.execute(id, j)
	}
}

func (p *Pool) execute(workerID string, j *job.Job) {
	log.Printf("[%s] running job %s: %s", workerID, j.ID, j.Command)

	cmd := exec.Command("sh", "-c", j.Command)
	output, runErr := cmd.CombinedOutput()

	j.Output = string(output)
	j.Attempts++
	j.UpdatedAt = time.Now()

	if runErr == nil {
		j.State = job.StateCompleted
		j.LastError = ""
		log.Printf("[%s] job %s completed", workerID, j.ID)
	} else {
		j.LastError = runErr.Error()
		if j.Attempts >= j.MaxRetries {
			j.State = job.StateDead
			log.Printf("[%s] job %s exhausted retries, moved to DLQ", workerID, j.ID)
		} else {
			j.State = job.StateFailed
			delay := retry.Backoff(p.cfg.BackoffBase, j.Attempts)
			j.NextRunAt = time.Now().Add(delay)
			log.Printf("[%s] job %s failed (attempt %d/%d), retrying in %s",
				workerID, j.ID, j.Attempts, j.MaxRetries, delay)
		}
	}

	if err := p.store.Update(j); err != nil {
		log.Printf("[%s] WARNING: failed to persist job %s: %v", workerID, j.ID, err)
	}
}
