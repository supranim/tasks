# Cancellable immediate jobs: submit returns a JobId, cancelJob drops
# a still-queued job synchronously — it never runs and stays silent.
# Running jobs cannot be preempted: cancelling one returns false and
# it delivers normally.
# Run with: clue build examples/cancellable.nim --out:/tmp/cancel && /tmp/cancel
import std/[locks, os]
import supranim_tasks

var lock: Lock
initLock(lock)

var m = newTaskManager(poolSize = 1)

# Occupy the only worker so the next three submissions queue up.
discard m.submit(proc(): int =
  sleep(500)
  0, proc(res: int) = discard)
sleep(50) # let the slow job occupy the worker

var ran: seq[int]

proc submitTracked(m: TaskManager, v: int): JobId =
  ## Enqueue one job recording its value on start. The value arrives
  ## as a param — fresh per call — so each closure captures its own
  ## copy (loop locals would share one slot).
  let cb = proc(res: int) =
    withLock lock:
      echo "job ", res, " delivered"
  m.submit(proc(): int =
    withLock lock:
      ran.add(v)
    v, cb)

let a = submitTracked(m, 1)
let b = submitTracked(m, 2)
let c = submitTracked(m, 3)
echo "ids valid: ", a.isValid and b.isValid and c.isValid
echo "cancel b (queued): ", m.cancelJob(b)
echo "cancel b again: ", m.cancelJob(b)
sleep(900) # slow job + survivors run; b's slot passes silently
withLock lock:
  echo "ran: ", ran
echo "cancel a (finished): ", m.cancelJob(a)
m.close()
echo "closed, running=", m.isRunning
