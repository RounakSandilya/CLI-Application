// Package retry implements the exponential backoff policy used to delay
// retries of failed jobs.
package retry

import (
	"math"
	"time"
)

// Backoff returns how long to wait before the next attempt, computed as
// base^attempts seconds, per the assignment spec. E.g. with base=2:
// attempt 1 -> 2s, attempt 2 -> 4s, attempt 3 -> 8s.
func Backoff(base float64, attempts int) time.Duration {
	seconds := math.Pow(base, float64(attempts))
	return time.Duration(seconds * float64(time.Second))
}
