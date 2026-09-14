# Scheduled tasks: wall-clock scheduling on std/times (local time).
# Run with: clue build examples/scheduled.nim --out:/tmp/scheduled && /tmp/scheduled
import std/[locks, os, times]
import supranim_tasks

var lock: Lock
initLock(lock)

var m = newTaskManager(poolSize = 2)

# One-shot at a DateTime: fires ~2s from now.
let future = (getTime() + initDuration(seconds = 2)).local()
let atCb = proc(res: string) =
  withLock lock:
    echo "at: ", res
discard m.scheduleAt(future, proc(): string = "on time", atCb,
  name = "meeting")
echo "meeting status: ", m.taskStatus("meeting")

# Past DateTime: never fires, stays tracked as inactive.
let past = (getTime() - initDuration(seconds = 5)).local()
let neverCb = proc(res: int) = echo "must never print"
discard m.scheduleAt(past, proc(): int = 1, neverCb, name = "missed")
sleep(200) # let the registration land on the scheduler thread
echo "missed status: ", m.taskStatus("missed")
m.removeTask("missed")
sleep(150) # removals land at the next heartbeat (~10ms)
echo "missed after remove: ", m.taskStatus("missed")

# Daily at the next wall-clock second: fires once here (next would be
# tomorrow), then cancel stops the chain.
let tickTarget = (getTime() + initDuration(seconds = 2)).local()
let tickCb = proc(res: int) =
  withLock lock:
    echo "daily tick"
discard m.scheduleDaily(tickTarget.hour, tickTarget.minute,
  tickTarget.second, proc(): int = 1, tickCb, name = "tick")
echo "tick armed: ", m.hasTask("tick")

sleep(3000)
m.cancelTask("tick")
sleep(150) # cancels land at the next heartbeat (~10ms)
echo "tick after cancel: ", m.taskStatus("tick")
m.removeTask("tick")

# Weekly preview: when would next Monday 09:00 fire from now?
echo "ms until Monday 09:00: ",
  nextWeeklyDelayMsFrom(getTime(), dMon, 9, 0, 0)

m.close()
echo "closed, running=", m.isRunning
