// Package store implements persistent, concurrency-safe storage for jobs
// backed by a single JSON file on disk.
package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"queuectl/internal/job"
)

// ErrNoJob is returned by ClaimNext when there is currently no job eligible
// to run.
var ErrNoJob = errors.New("no job available")

// Store reads and writes a slice of *job.Job to a JSON file.
type Store struct {
	path     string
	lockPath string
	mu       sync.Mutex // guards against goroutines in THIS process
}

// New returns a Store backed by the JSON file at path.
func New(path string) *Store {
	return &Store{
		path:     path,
		lockPath: path + ".lock",
	}
}

// acquireLock creates an exclusive lock file, retrying with a short sleep
// until it succeeds or a timeout elapses. os.O_EXCL guarantees the create
// fails if the file already exists, and that check-and-create is atomic at
// the OS level - which is what makes this safe across separate processes,
// not just goroutines.
func (s *Store) acquireLock() error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		f, err := os.OpenFile(s.lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			f.Close()
			return nil
		}
		if !os.IsExist(err) {
			return err
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for store lock (is a stale " + s.lockPath + " left over?)")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (s *Store) releaseLock() {
	os.Remove(s.lockPath)
}

// transact loads all jobs, lets fn read/mutate the slice, then saves the
// result back to disk. It holds the in-process mutex first (cheap, fast
// path for goroutine safety) and the on-disk lock file second (safety net
// against other OS processes touching the same file).
func (s *Store) transact(fn func(jobs []*job.Job) ([]*job.Job, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.acquireLock(); err != nil {
		return err
	}
	defer s.releaseLock()

	jobs, err := s.load()
	if err != nil {
		return err
	}
	jobs, err = fn(jobs)
	if err != nil {
		return err
	}
	return s.save(jobs)
}

func (s *Store) load() ([]*job.Job, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return []*job.Job{}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return []*job.Job{}, nil
	}
	var jobs []*job.Job
	if err := json.Unmarshal(data, &jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// save writes jobs atomically: write to a temp file in the same directory,
// then rename over the real file. os.Rename is atomic on POSIX filesystems,
// so a crash mid-write can never leave jobs.json half-written/corrupted.
func (s *Store) save(jobs []*job.Job) error {
	data, err := json.MarshalIndent(jobs, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "jobs-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, s.path)
}

// Enqueue adds a new job. It fails if a job with the same ID already exists.
func (s *Store) Enqueue(j *job.Job) error {
	return s.transact(func(jobs []*job.Job) ([]*job.Job, error) {
		for _, existing := range jobs {
			if existing.ID == j.ID {
				return jobs, errors.New("job id already exists: " + j.ID)
			}
		}
		return append(jobs, j), nil
	})
}

// All returns every job currently in the store.
func (s *Store) All() ([]*job.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.acquireLock(); err != nil {
		return nil, err
	}
	defer s.releaseLock()
	return s.load()
}

// List returns jobs matching state, or all jobs if state is "".
func (s *Store) List(state job.State) ([]*job.Job, error) {
	all, err := s.All()
	if err != nil {
		return nil, err
	}
	if state == "" {
		return all, nil
	}
	var out []*job.Job
	for _, j := range all {
		if j.State == state {
			out = append(out, j)
		}
	}
	return out, nil
}

// Get returns a single job by ID.
func (s *Store) Get(id string) (*job.Job, error) {
	all, err := s.All()
	if err != nil {
		return nil, err
	}
	for _, j := range all {
		if j.ID == id {
			return j, nil
		}
	}
	return nil, errors.New("job not found: " + id)
}

// ClaimNext atomically finds one job eligible to run - either pending, or
// failed with its backoff delay elapsed - and flips it to "processing" so
// no other worker (goroutine or process) can pick it up too. The find + flip
// happens inside a single transact() call, which is what makes it atomic:
// nothing else can read the file between the "find" and the "flip".
func (s *Store) ClaimNext(workerID string) (*job.Job, error) {
	var claimed *job.Job
	err := s.transact(func(jobs []*job.Job) ([]*job.Job, error) {
		now := time.Now()
		for _, j := range jobs {
			eligible := j.State == job.StatePending ||
				(j.State == job.StateFailed && !j.NextRunAt.After(now))
			if eligible {
				j.State = job.StateProcessing
				j.LockedBy = workerID
				j.UpdatedAt = now
				claimed = j
				break
			}
		}
		return jobs, nil
	})
	if err != nil {
		return nil, err
	}
	if claimed == nil {
		return nil, ErrNoJob
	}
	return claimed, nil
}

// Update persists changes to a single job, matched by ID.
func (s *Store) Update(updated *job.Job) error {
	return s.transact(func(jobs []*job.Job) ([]*job.Job, error) {
		for i, j := range jobs {
			if j.ID == updated.ID {
				jobs[i] = updated
				return jobs, nil
			}
		}
		return jobs, errors.New("job not found: " + updated.ID)
	})
}

// Counts returns how many jobs are in each state, for `queuectl status`.
func (s *Store) Counts() (map[job.State]int, error) {
	all, err := s.All()
	if err != nil {
		return nil, err
	}
	counts := map[job.State]int{}
	for _, j := range all {
		counts[j.State]++
	}
	return counts, nil
}
