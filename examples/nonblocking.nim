# Non-blocking concurrency: jobs overlap in time across workers while
# the scheduler stays responsive, even with every worker saturated.
# Run with: clue build examples/nonblocking.nim --out:/tmp/nonblock && /tmp/nonblock
import std/[locks, os, times]
import supranim_tasks

var lock: Lock
initLock(lock)

var m = newTaskManager(poolSize = 4)

# Part 1: a concurrent batch. Eight jobs with staggered 200-400ms
# sleeps overlap on four workers: wall time tracks the slowest jobs,
# not the sum, and completions arrive out of submission order.
const Batch = 8
var startedAt: array[Batch, float]
var finishedAt: array[Batch, float]
var order: seq[int]
var delivered = 0

proc submitBatchJob(m: TaskManager, v, idx, ms: int, t0: float) =
  ## Enqueue one batch job recording its own start/finish timestamps.
  ## Values arrive as params — fresh per call — so each closure below
  ## captures its own copy. Loop locals would NOT do: closures share
  ## one slot per loop variable and would all see the last iteration.
  let jobCb = proc(res: int) =
    withLock lock:
      order.add(res)
      inc delivered
  discard m.submit(proc(): int =
    withLock lock:
      startedAt[idx] = epochTime()
    sleep(ms)
    withLock lock:
      finishedAt[idx] = epochTime()
    v, jobCb)

template lockedInt(expr: untyped): int =
  ## One lock-guarded read for poll conditions (plain reads race with
  ## the dispatch thread writing under the lock).
  var tmp = 0
  withLock lock:
    tmp = expr
  tmp

let t0 = epochTime()
for i in 1 .. Batch:
  submitBatchJob(m, i, i - 1, 200 + (i mod 3) * 100, t0)

# Part 2: mixed kinds interleaved with the batch — a delayed one-shot
# and a wall-clock one-shot fire while workers are busy.
let lateCb = proc(res: string) =
  withLock lock:
    echo "delayed (100ms): ", res
discard m.submitDelayed(100, proc(): string = "on time", lateCb)
let wallCb = proc(res: string) =
  withLock lock:
    echo "scheduled: ", res
discard m.scheduleAt((getTime() + initDuration(milliseconds = 400)).local(),
  proc(): string = "wall-clock", wallCb)

var waited = 0
while lockedInt(delivered) < Batch and waited < 8000:
  sleep(20)
  inc(waited, 20)
withLock lock:
  echo "batch: ", delivered, "/", Batch, " delivered in ",
    int((epochTime() - t0) * 1000),
    "ms (sleeps sum 2500ms, wall tracks the slowest)"
  echo "delivery order: ", order
  for j in 0 ..< Batch:
    echo "  job ", j + 1, ": started +",
      int((startedAt[j] - t0) * 1000), "ms, ran ",
      int((finishedAt[j] - startedAt[j]) * 1000), "ms"

# Part 3: a repeating tick on freed workers — prompt delivery, so
# cancelling after three ticks stops it cleanly.
var ticks = 0
let tickCb = proc(res: int) =
  withLock lock:
    inc ticks
    echo "repeating tick #", ticks
let every = m.submitRepeating(150, proc(): int = 1, tickCb)
waited = 0
while lockedInt(ticks) < 3 and waited < 5000:
  sleep(20)
  inc(waited, 20)
m.cancelTask(every)
sleep(150) # cancels land at the next heartbeat (~10ms)
echo "repeating cancelled: ", m.taskStatus(every) == taskCancelled
m.removeTask(every)

# Part 4: responsiveness under full saturation. All four workers stuck
# 800ms in sleepers, yet submitting returns at once and a timer still
# fires on schedule (its job queues and runs once a worker frees up).
for w in 1 .. 4:
  assert m.submit(proc(): int =
    sleep(800)
    1, proc(res: int) = discard)
sleep(50) # let the sleepers occupy every worker
let t1 = epochTime()
for i in 1 .. 20:
  let v = i # results discarded below, so sharing one slot is harmless
  assert m.submit(proc(): int = v, proc(res: int) = discard)
echo "20 submits while saturated took ",
  int((epochTime() - t1) * 1000), "ms"
let firedAt = epochTime()
let timerCb = proc(res: int) =
  echo "timer delivered: ", res
let id = m.submitDelayed(100, proc(): int = 7, timerCb)
while m.hasTask(id):
  sleep(10)
echo "timer fired after ", int((epochTime() - firedAt) * 1000),
  "ms (workers busy 800ms)"

m.close()
echo "closed, running=", m.isRunning
