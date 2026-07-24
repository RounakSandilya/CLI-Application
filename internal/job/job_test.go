package job

import "testing"

func TestNewGeneratesIDWhenMissing(t *testing.T) {
	j := New("", "echo hi", 3)
	if j.ID == "" {
		t.Fatal("expected a generated ID, got empty string")
	}
	if j.State != StatePending {
		t.Fatalf("expected new job to start pending, got %s", j.State)
	}
	if j.Attempts != 0 {
		t.Fatalf("expected a new job to start with 0 attempts, got %d", j.Attempts)
	}
}

func TestNewKeepsProvidedID(t *testing.T) {
	j := New("job1", "echo hi", 3)
	if j.ID != "job1" {
		t.Fatalf("expected ID job1, got %s", j.ID)
	}
}
