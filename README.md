# queuectl

A CLI-based background job queue with worker goroutines, exponential-backoff
retries, and a Dead Letter Queue (DLQ) — built in Go as a learning project
(first Go project, feedback welcome).

## 1. Setup Instructions

Requires Go 1.22+ (https://go.dev/dl/).

```bash
git clone <your-repo-url>
cd queuectl
go build -o queuectl .
```

This produces a `queuectl` binary in the current directory. You can also run
without building first, using `go run . <command>`.

All state is stored under `./data/` (created automatically):
- `data/jobs.json` — the job store
- `data/config.json` — retry/backoff configuration
- `data/worker.pid` — present while workers are running

## 2. Usage Examples

**Enqueue a job:**
```bash
$ ./queuectl enqueue '{"id":"job1","command":"echo Hello World"}'
enqueued job job1 (max_retries=3)
```

`id` and `max_retries` are optional — if `id` is omitted, one is generated;
if `max_retries` is omitted, the configured default is used.

**Start workers (runs in the foreground until stopped):**
```bash
$ ./queuectl worker start --count 3
started 3 worker(s), pid=48213. Press Ctrl+C or run 'queuectl worker stop' to shut down gracefully.
2026/07/24 10:00:00 [worker-1] starting
2026/07/24 10:00:00 [worker-2] starting
2026/07/24 10:00:00 [worker-3] starting
2026/07/24 10:00:00 [worker-1] running job job1: echo Hello World
2026/07/24 10:00:00 [worker-1] job job1 completed
```

**Stop workers gracefully (from another terminal):**
```bash
$ ./queuectl worker stop
sent stop signal to worker process (pid=48213)
```
The running `worker start` process finishes whatever job(s) it's currently
executing, then exits on its own — it never abandons a job mid-run.

**Check status:**
```bash
$ ./queuectl status
queuectl status
----------------
workers running: no
pending:    0
processing: 0
completed:  1
failed:     0
dead:       0
```

**List jobs, optionally by state:**
```bash
$ ./queuectl list --state pending
job2                     pending    attempts=0/3  sleep 2
```

**Dead Letter Queue:**
```bash
$ ./queuectl dlq list
job3                     attempts=3/3  error="exit status 127"  bad-command-xyz

$ ./queuectl dlq retry job3
job3 requeued
```

**Configuration:**
```bash
$ ./queuectl config set max-retries 5
set max-retries = 5

$ ./queuectl config set backoff-base 3
set backoff-base = 3

$ ./queuectl config show
max-retries:  5
backoff-base: 3
```

## 3. Architecture Overview

**Job lifecycle:** `pending` → `processing` → `completed`, or `pending` →
`processing` → `failed` → (retry, back to `processing`) → ... → `dead` once
`attempts >= max_retries`.

**Persistence:** jobs live as a single JSON array in `data/jobs.json`. Every
read/write goes through `internal/store`, which:
- Serializes access with an in-process `sync.Mutex` (protects concurrent
  goroutines within one `worker start` process).
- Additionally uses an on-disk lock file (`jobs.json.lock`, created with
  `O_CREATE|O_EXCL`) as a safety net against two separate OS processes
  touching the file at once.
- Writes atomically: new data is written to a temp file, then renamed over
  the real file (`os.Rename` is atomic on POSIX filesystems), so a crash
  mid-write can never corrupt `jobs.json`.

**Worker logic:** `queuectl worker start --count N` runs N goroutines in a
single foreground process. Each worker loops: claim one eligible job
(`ClaimNext`, which atomically finds-and-flips a job to `processing` inside
one locked transaction — this is what prevents two workers from ever
claiming the same job), run it via `sh -c <command>`, then persist the
result. If no job is available, it polls every 500ms.

**Retry & backoff:** on failure, `attempts` increments. If
`attempts < max_retries`, the job goes to `failed` with `next_run_at` set to
`now + base^attempts` seconds; it becomes claimable again once that time
passes. If `attempts >= max_retries`, the job moves to `dead` (the DLQ).

**Graceful shutdown:** `worker start` listens for `SIGINT`/`SIGTERM` via
`context.Context`. Workers only check for shutdown *between* jobs (never
mid-execution), so a job that's already running always finishes before the
process exits. `worker stop` locates the running process via
`data/worker.pid` and sends it `SIGTERM`, triggering the same graceful path.

**Config:** `data/config.json` stores `max_retries` (default 3) and
`backoff_base` (default 2), managed via `config set`/`config show`.

## 4. Assumptions & Trade-offs

- **One worker process at a time is the supported model.** The lock file
  prevents two `worker start` invocations from corrupting the store, but the
  design assumes a single long-running worker process per queue (the
  `--count` flag is how you scale, via goroutines within that process) —
  not a distributed fleet of independent worker processes.
- **JSON file storage over SQLite.** Chosen for simplicity, zero external
  dependencies, and easy inspection/debugging while learning — at the cost
  of O(n) full-file read/write per operation, which wouldn't scale to a
  large job volume. SQLite (or a proper embedded KV store) would be the
  next step for a production version.
- **No automatic recovery of orphaned "processing" jobs.** If a worker
  process is killed ungracefully (`kill -9`, power loss), a job it was
  mid-execution on stays stuck in `processing` forever — there's no
  heartbeat/lease mechanism to detect and requeue it. A real system would
  want a lease timeout for this.
- **Commands run via `sh -c`.** Simple and matches the spec's examples
  (`echo`, `sleep`), but means job commands run with full shell
  interpretation (globbing, pipes, etc.) — fine for a trusted internal
  queue, not something to expose to untrusted input.
- **`worker stop` uses OS process signaling**, which relies on POSIX signals
  (`SIGTERM`). This works on Linux/macOS; on Windows, graceful stop via
  Ctrl+C in the same terminal still works, but the separate `worker stop`
  command may not.

## 5. Testing Instructions

Run all unit tests:
```bash
go test ./...
```

Tests cover:
- `internal/job` — job construction and ID generation
- `internal/retry` — exponential backoff math
- `internal/store` — enqueue, list, update, and (importantly) that
  `ClaimNext` never lets two callers claim the same job

**Manual end-to-end smoke test:**
```bash
go build -o queuectl .

# 1. Basic job completes successfully
./queuectl enqueue '{"id":"ok1","command":"echo hi"}'

# 2. Failing job retries then moves to DLQ (max_retries defaults to 3)
./queuectl enqueue '{"id":"bad1","command":"exit 1"}'

# 3. Multiple workers process jobs concurrently without overlap
./queuectl enqueue '{"id":"sleepy","command":"sleep 2"}'
./queuectl worker start --count 3 &
sleep 5
./queuectl worker stop

./queuectl status
./queuectl dlq list
```

You should see `ok1` and `sleepy` as `completed`, and `bad1` in the DLQ
after ~3 retries with growing delays (visible in the worker log output).

## Demo

<!-- Add your recorded CLI demo link here before submission -->
