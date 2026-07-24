package job

// State represents where a job currently sits in its lifecycle.
type State string

const (
	StatePending    State = "pending"
	StateProcessing State = "processing"
	StateCompleted  State = "completed"
	StateFailed     State = "failed"
	StateDead       State = "dead"
)
