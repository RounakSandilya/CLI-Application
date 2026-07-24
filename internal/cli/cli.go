// Package cli implements queuectl's command-line interface: parsing
// arguments and dispatching to the right subsystem (store, worker, dlq,
// config).
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"queuectl/internal/config"
	"queuectl/internal/dlq"
	"queuectl/internal/job"
	"queuectl/internal/store"
	"queuectl/internal/worker"
)

const dataDir = "data"

func paths() (jobsPath, configPath, pidPath string) {
	return filepath.Join(dataDir, "jobs.json"),
		filepath.Join(dataDir, "config.json"),
		filepath.Join(dataDir, "worker.pid")
}

// Run parses args (os.Args[1:]) and executes the matching command,
// returning a process exit code.
func Run(args []string) int {
	if len(args) == 0 {
		printUsage()
		return 1
	}

	switch args[0] {
	case "enqueue":
		return cmdEnqueue(args[1:])
	case "worker":
		return cmdWorker(args[1:])
	case "status":
		return cmdStatus(args[1:])
	case "list":
		return cmdList(args[1:])
	case "dlq":
		return cmdDLQ(args[1:])
	case "config":
		return cmdConfig(args[1:])
	case "help", "-h", "--help":
		printUsage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", args[0])
		printUsage()
		return 1
	}
}

func printUsage() {
	fmt.Print(`queuectl - a CLI background job queue with retries and a DLQ

Usage:
  queuectl enqueue '<json job>'         Add a new job to the queue
  queuectl worker start [--count N]     Start N worker goroutines (default 1)
  queuectl worker stop                  Stop running workers gracefully
  queuectl status                       Show a summary of job states
  queuectl list [--state STATE]         List jobs, optionally filtered
  queuectl dlq list                     List dead-lettered jobs
  queuectl dlq retry <id>               Requeue a dead job
  queuectl config set <key> <value>     Set config (max-retries, backoff-base)
  queuectl config show                  Show current config
  queuectl help                         Show this message

Examples:
  queuectl enqueue '{"id":"job1","command":"echo hello"}'
  queuectl worker start --count 3
  queuectl list --state pending
  queuectl dlq retry job1
`)
}

func loadConfig(path string) config.Config {
	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not load config, using defaults: %v\n", err)
		return config.Default()
	}
	return cfg
}

func cmdEnqueue(args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: queuectl enqueue '<json job>'")
		return 1
	}
	jobsPath, configPath, _ := paths()
	cfg := loadConfig(configPath)

	var input struct {
		ID         string `json:"id"`
		Command    string `json:"command"`
		MaxRetries *int   `json:"max_retries"`
	}
	if err := json.Unmarshal([]byte(args[0]), &input); err != nil {
		fmt.Fprintf(os.Stderr, "invalid job JSON: %v\n", err)
		return 1
	}
	if input.Command == "" {
		fmt.Fprintln(os.Stderr, `job must include a non-empty "command"`)
		return 1
	}
	maxRetries := cfg.MaxRetries
	if input.MaxRetries != nil {
		maxRetries = *input.MaxRetries
	}

	j := job.New(input.ID, input.Command, maxRetries)
	s := store.New(jobsPath)
	if err := s.Enqueue(j); err != nil {
		fmt.Fprintf(os.Stderr, "failed to enqueue job: %v\n", err)
		return 1
	}
	fmt.Printf("enqueued job %s (max_retries=%d)\n", j.ID, j.MaxRetries)
	return 0
}

func cmdWorker(args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: queuectl worker <start|stop> [flags]")
		return 1
	}
	jobsPath, configPath, pidPath := paths()

	switch args[0] {
	case "start":
		fs := flag.NewFlagSet("worker start", flag.ExitOnError)
		count := fs.Int("count", 1, "number of worker goroutines to start")
		fs.Parse(args[1:])

		if _, err := os.Stat(pidPath); err == nil {
			fmt.Fprintln(os.Stderr, "workers already appear to be running (found data/worker.pid). Run 'queuectl worker stop' first, or remove the file if it's stale.")
			return 1
		}

		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "failed to prepare data dir: %v\n", err)
			return 1
		}
		pid := os.Getpid()
		if err := os.WriteFile(pidPath, []byte(strconv.Itoa(pid)), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write pid file: %v\n", err)
			return 1
		}
		defer os.Remove(pidPath)

		cfg := loadConfig(configPath)
		s := store.New(jobsPath)
		pool := worker.New(s, cfg, *count)

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		fmt.Printf("started %d worker(s), pid=%d. Press Ctrl+C or run 'queuectl worker stop' to shut down gracefully.\n", *count, pid)
		pool.Run(ctx)
		fmt.Println("all workers stopped gracefully.")
		return 0

	case "stop":
		data, err := os.ReadFile(pidPath)
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(os.Stderr, "no running workers found (data/worker.pid missing)")
			return 1
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to read pid file: %v\n", err)
			return 1
		}
		pid, err := strconv.Atoi(string(data))
		if err != nil {
			fmt.Fprintf(os.Stderr, "corrupt pid file: %v\n", err)
			return 1
		}
		proc, err := os.FindProcess(pid)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not find process %d: %v\n", pid, err)
			return 1
		}
		if err := proc.Signal(syscall.SIGTERM); err != nil {
			fmt.Fprintf(os.Stderr, "failed to signal process %d: %v\n", pid, err)
			return 1
		}
		fmt.Printf("sent stop signal to worker process (pid=%d)\n", pid)
		return 0

	default:
		fmt.Fprintln(os.Stderr, "usage: queuectl worker <start|stop> [flags]")
		return 1
	}
}

func cmdStatus(args []string) int {
	jobsPath, _, pidPath := paths()
	s := store.New(jobsPath)
	counts, err := s.Counts()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read jobs: %v\n", err)
		return 1
	}

	running := "no"
	if data, err := os.ReadFile(pidPath); err == nil {
		running = fmt.Sprintf("yes (pid=%s)", string(data))
	}

	fmt.Println("queuectl status")
	fmt.Println("----------------")
	fmt.Printf("workers running: %s\n", running)
	fmt.Printf("pending:    %d\n", counts[job.StatePending])
	fmt.Printf("processing: %d\n", counts[job.StateProcessing])
	fmt.Printf("completed:  %d\n", counts[job.StateCompleted])
	fmt.Printf("failed:     %d\n", counts[job.StateFailed])
	fmt.Printf("dead:       %d\n", counts[job.StateDead])
	return 0
}

func cmdList(args []string) int {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	state := fs.String("state", "", "filter by state (pending, processing, completed, failed, dead)")
	fs.Parse(args)

	jobsPath, _, _ := paths()
	s := store.New(jobsPath)
	jobs, err := s.List(job.State(*state))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to list jobs: %v\n", err)
		return 1
	}
	if len(jobs) == 0 {
		fmt.Println("no jobs found")
		return 0
	}
	for _, j := range jobs {
		fmt.Printf("%-24s %-10s attempts=%d/%d  %s\n", j.ID, j.State, j.Attempts, j.MaxRetries, j.Command)
	}
	return 0
}

func cmdDLQ(args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: queuectl dlq <list|retry> [id]")
		return 1
	}
	jobsPath, _, _ := paths()
	s := store.New(jobsPath)

	switch args[0] {
	case "list":
		jobs, err := dlq.List(s)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to list DLQ: %v\n", err)
			return 1
		}
		if len(jobs) == 0 {
			fmt.Println("DLQ is empty")
			return 0
		}
		for _, j := range jobs {
			fmt.Printf("%-24s attempts=%d/%d  error=%q  %s\n", j.ID, j.Attempts, j.MaxRetries, j.LastError, j.Command)
		}
		return 0
	case "retry":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: queuectl dlq retry <id>")
			return 1
		}
		if err := dlq.Retry(s, args[1]); err != nil {
			fmt.Fprintf(os.Stderr, "failed to retry job: %v\n", err)
			return 1
		}
		fmt.Printf("job %s requeued\n", args[1])
		return 0
	default:
		fmt.Fprintln(os.Stderr, "usage: queuectl dlq <list|retry> [id]")
		return 1
	}
}

func cmdConfig(args []string) int {
	_, configPath, _ := paths()
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: queuectl config <set|show>")
		return 1
	}
	switch args[0] {
	case "set":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: queuectl config set <key> <value>")
			return 1
		}
		cfg := loadConfig(configPath)
		if err := cfg.Set(args[1], args[2]); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return 1
		}
		if err := cfg.Save(configPath); err != nil {
			fmt.Fprintf(os.Stderr, "failed to save config: %v\n", err)
			return 1
		}
		fmt.Printf("set %s = %s\n", args[1], args[2])
		return 0
	case "show":
		cfg := loadConfig(configPath)
		fmt.Printf("max-retries:  %d\n", cfg.MaxRetries)
		fmt.Printf("backoff-base: %g\n", cfg.BackoffBase)
		return 0
	default:
		fmt.Fprintln(os.Stderr, "usage: queuectl config <set|show>")
		return 1
	}
}
