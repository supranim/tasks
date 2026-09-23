<p align="center">
  Supranim's Task Manager<br>
  Queue jobs that may be processed in the background
</p>

<p align="center">
  <code>nimble install supranim_tasks</code>
</p>

<p align="center">
  <a href="https://supranim.github.io/tasks/">API reference</a><br>
  <img src="https://github.com/supranim/tasks/workflows/test/badge.svg" alt="Github Actions">  <img src="https://github.com/supranim/tasks/workflows/docs/badge.svg" alt="Github Actions">
</p>

## Key features
- Built on PowPow event loop & ThreadPool
- Immediate, delayed (one-shot) and repeating (interval) background tasks
- Immediate jobs cancel by id: `submit` returns a `JobId`, `cancelJob`
  drops a still-queued job synchronously (silent, authoritative bool)
- Wall-clock scheduling on `std/times` (local time): one-shot at a
  `DateTime`, daily and weekly repeats
- Past one-shot times never fire: the task stays tracked as inactive,
  observable via `taskStatus`, with no warning
- Thread-safe submission from any thread, including from inside callbacks

> [!NOTE]
> This task manager is designed for Supranim applications but works in any
> Nim project that needs background task processing. Requires a
> threads-enabled build (Nim enables threads by default).

### Threading contract
Jobs run on pool worker threads; result callbacks fire serialized on the
pool dispatch thread — never on the caller's thread. Lock shared state
in callbacks, and never touch thread-unsafe handles (e.g. an HTTP
`Request`) there.

Job closures must not capture `ref` objects across threads (directly or
nested, e.g. `seq[SomeRef]`). Capture values, strings, seqs of values,
locks, atomics and raw pointers instead. This matches powpow's
`submitWork` envelope: closures capturing true refs are equally unsafe
there (Nim ORC tracks cycle candidates per thread).

`stop`, `shutdown` and `close` join pool threads, so call them from
outside jobs/callbacks. From inside a job/callback, call `halt` to stop
(the recommended way to stop from there).

### Example usage

```nim
import std/[os, locks]
import pkg/supranim_tasks

var m = newTaskManager(poolSize = 4)

# Immediate: runs on a worker, callback on the dispatch thread
let id = m.submit(
  proc(): string = "hello",
  proc(res: string) = echo "got: ", res
)
if m.cancelJob(id):
  echo "was still queued, will never run"

# Named task: runs once after 2 seconds, cancellable by id or name
discard m.submitDelayed(2000,
  proc(): int = 40 + 2,
  proc(res: int) = echo "named: ", res,
  name = "cleanup"
)
m.removeTask("cleanup") # strict: no further fires, name freed for reuse

# Repeating: runs every 5 minutes until cancelled
let every = m.submitRepeating(5 * 60 * 1000,
  proc(): int = 1,
  proc(res: int) = echo "tick"
)
sleep(1000)
m.cancel(every)

# Scheduled: once at a wall-clock time (local time)
import std/times
discard m.scheduleAt(dateTime(2026, mSep, 15, 9, 0, 0, 0, local()),
  proc(): string = "morning",
  proc(res: string) = echo "scheduled: ", res,
  name = "standup"
)

# Daily and weekly repeats re-arm per occurrence (DST-safe)
discard m.scheduleDaily(9, 30,
  proc(): int = 1,
  proc(res: int) = echo "daily",
  name = "standup-daily"
)
discard m.scheduleWeekly(dMon, 9, 0,
  proc(): int = 1,
  proc(res: int) = echo "weekly",
  name = "weekly-report"
)
echo m.taskStatus("standup") # taskArmed / taskInactive / ...

# Stop accepting work after 30 more seconds, then tear down
discard m.halt(30_000)
m.close()
```

Runnable versions live in [`examples/`](examples/): `basics.nim`
(immediate/delayed/repeating), `named_tasks.nim`
(`cancelTask`/`removeTask` by id or name), `nonblocking.nim`
(concurrent overlapping batch, mixed task kinds, scheduler staying
responsive while the pool is saturated),
`cancellable.nim` (immediate `cancelJob` by id) and
`scheduled.nim` (wall-clock `scheduleAt`/`scheduleDaily`, past times
staying inactive, `taskStatus`).

### Scheduling notes

- Times are local wall-clock (`std/times`, `local()` zone).
- Daily/weekly tasks chain one-shots: after every fire the next
  occurrence is recomputed from local `now()`, so DST shifts land on
  one 23h/25h day instead of drifting.
- Chain re-arms demand the next occurrence at least 60s out: a fire
  landing inside its own target second rolls to the next day/week
  instead of echoing twice. Explicit same-second schedules still
  fire ASAP.
- A past `scheduleAt` never fires and logs nothing: it stays tracked
  as `taskInactive` (name still reserved). `cancelTask` on it is a
  no-op; `removeTask` drops tracking and frees the name.
- For chains, prefer cancelling by name: the name resolves on the
  scheduler thread, so a re-arm in flight still stops. A stale id
  follows supersession to the live occurrence.
- `nextDailyDelayMsFrom` / `nextWeeklyDelayMsFrom` preview in how many
  milliseconds a daily/weekly task will fire from a given `Time`.

### API

| Proc | Description |
|---|---|
| `newTaskManager(poolSize = 4)` | Create the manager; starts the pool and scheduler thread |
| `submit(job, cb, onError = nil)` | Run `job` now; returns a `JobId` (`JobId(0)` when rejected) |
| `cancelJob(id)` | Cancel a still-queued immediate job; true iff it will never run |
| `isValid(id)` | True for a real `JobId` (anything but `JobId(0)`) |
| `submitDelayed(delayMs, job, cb, onError = nil, name = "")` | Run `job` once after `delayMs`; returns a `TimerId` |
| `submitRepeating(intervalMs, job, cb, onError = nil, name = "")` | Run `job` every `intervalMs` until `cancel`; returns a `TimerId` |
| `scheduleAt(at, job, cb, onError = nil, name = "")` | Run `job` once at wall-clock `at` (`DateTime`, local); past stays `taskInactive` |
| `scheduleDaily(hour, minute, second = 0, job, cb, onError = nil, name = "")` | Run `job` daily at local time; returns the first `TimerId` |
| `scheduleWeekly(weekday, hour, minute, second = 0, job, cb, onError = nil, name = "")` | Run `job` weekly on `weekday` at local time |
| `cancelTask(id)` / `cancelTask(name)` | Drop a timer; unknown ids/names ignored, name stays reserved |
| `cancel(id)` | Alias of `cancelTask` by id |
| `removeTask(id)` / `removeTask(name)` | Strict removal: no further fires, name freed for reuse |
| `hasTask(id)` / `hasTask(name)` | True while a task is tracked (armed, cancelled or inactive) |
| `taskStatus(id)` / `taskStatus(name)` | `taskArmed`, `taskCancelled`, `taskInactive` or `taskUnknown` |
| `stop()` | Graceful: reject new work, drain the pool, drop timers |
| `shutdown()` | Immediate: like `stop` but discard still-queued jobs |
| `halt(delayMs)` | `stop` after `delayMs`; safe from inside jobs/callbacks |
| `close()` | `stop` + join scheduler thread + free resources (idempotent) |
| `isRunning()` / `poolSize()` / `rawPool()` | Status, worker count, underlying pool |

### Roadmap
- [x] Task cancellation by id for immediate jobs (pending queue removal)
- [ ] Task prioritization
- [ ] File logging of task execution
- [ ] More detailed error handling and reporting for task failures

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/supranim/tasks/issues)
- 👋 Wanna help? [Fork it!](https://github.com/supranim/tasks/fork)

### 🎩 License
MIT license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright &copy; 2026 OpenPeeps & Contributors &mdash; All rights reserved.
