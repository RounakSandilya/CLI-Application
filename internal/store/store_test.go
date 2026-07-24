package store

import (
	"path/filepath"
	"testing"

	"queuectl/internal/job"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir() // a fresh temp directory, auto-cleaned after the test
	return New(filepath.Join(dir, "jobs.json"))
}

func TestEnqueueAndList(t *testing.T) {
	s := newTestStore(t)
	if err := s.Enqueue(job.New("job1", "echo hi", 3)); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	jobs, err := s.List(job.StatePending)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "job1" {
		t.Fatalf("expected 1 pending job with ID job1, got %+v", jobs)
	}
}

func TestEnqueueRejectsDuplicateID(t *testing.T) {
	s := newTestStore(t)
	if err := s.Enqueue(job.New("job1", "echo hi", 3)); err != nil {
		t.Fatalf("first Enqueue failed: %v", err)
	}
	if err := s.Enqueue(job.New("job1", "echo hi", 3)); err == nil {
		t.Fatal("expected an error enqueuing a duplicate job ID, got nil")
	}
}

func TestClaimNextPreventsDuplicateProcessing(t *testing.T) {
	s := newTestStore(t)
	if err := s.Enqueue(job.New("job1", "echo hi", 3)); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	claimed, err := s.ClaimNext("worker-1")
	if err != nil {
		t.Fatalf("ClaimNext failed: %v", err)
	}
	if claimed.State != job.StateProcessing {
		t.Fatalf("expected claimed job to be processing, got %s", claimed.State)
	}

	// A second worker asking for work right now should find nothing -
	// this is the core "no duplicate processing" guarantee.
	if _, err := s.ClaimNext("worker-2"); err != ErrNoJob {
		t.Fatalf("expected ErrNoJob for a second concurrent claim, got %v", err)
	}
}

func TestUpdatePersists(t *testing.T) {
	s := newTestStore(t)
	j := job.New("job1", "echo hi", 3)
	if err := s.Enqueue(j); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	j.State = job.StateCompleted
	if err := s.Update(j); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	got, err := s.Get("job1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.State != job.StateCompleted {
		t.Fatalf("expected completed, got %s", got.State)
	}
}
