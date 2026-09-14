# Basics: immediate, delayed and repeating tasks.
# Run with: clue build examples/basics.nim --out:/tmp/basics && /tmp/basics
import std/[locks, os]
import supranim_tasks

var lock: Lock
initLock(lock)

var m = newTaskManager(poolSize = 2)
echo "running: ", m.isRunning, " workers: ", m.poolSize

# Immediate: runs on a worker, callback fires on the dispatch thread.
discard m.submit(
  proc(): string = "hello",
  proc(res: string) =
    withLock lock:
      echo "immediate: ", res
)

# Delayed one-shot: runs once after 300ms.
discard m.submitDelayed(300,
  proc(): int = 40 + 2,
  proc(res: int) =
    withLock lock:
      echo "delayed: ", res
)

# Repeating: runs every 200ms until cancelled.
let every = m.submitRepeating(200,
  proc(): int = 1,
  proc(res: int) =
    withLock lock:
      echo "tick"
)
sleep(550)
m.cancelTask(every)
echo "cancelled repeating timer ", int(every)

sleep(300)
m.close()
echo "closed, running=", m.isRunning
