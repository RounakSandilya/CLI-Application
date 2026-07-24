package job

import "time"

// Job represents a single background job managed by queuectl.
type Job struct {
	ID         string    `json:"id"`
	Command    string    `json:"command"`
	State      State     `json:"state"`
	Attempts   int       `json:"attempts"`
	MaxRetries int       `json:"max_retries"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`

	// NextRunAt is when a failed job becomes eligible to be claimed again
	// (set to now + exponential backoff delay). Zero value means "now".
	NextRunAt time.Time `json:"next_run_at,omitempty"`

	// LastError holds the error message from the most recent failed attempt.
	LastError string `json:"last_error,omitempty"`

	// Output holds combined stdout+stderr from the most recent execution.
	Output string `json:"output,omitempty"`

	// LockedBy records which worker last claimed this job. Useful for
	// debugging and for verifying no two workers processed it at once.
	LockedBy string `json:"locked_by,omitempty"`
}

// New creates a fresh pending job. If id is empty, one is generated.
func New(id, command string, maxRetries int) *Job {
	now := time.Now()
	if id == "" {
		id = NewID()
	}
	return &Job{
		ID:         id,
		Command:    command,
		State:      StatePending,
		Attempts:   0,
		MaxRetries: maxRetries,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}
