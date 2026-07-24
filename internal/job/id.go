package job

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// NewID generates a reasonably unique job ID, e.g. "job-1721800000-a1b2c3d4".
// It combines a timestamp (for readability/sortability) with a few random
// bytes (to avoid collisions if two jobs are created in the same second).
func NewID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b) // crypto/rand.Read only fails if the OS RNG is broken
	return fmt.Sprintf("job-%d-%s", time.Now().Unix(), hex.EncodeToString(b))
}
