# Named tasks: cancelTask/removeTask by id or name, hasTask checks.
# Run with: clue build examples/named_tasks.nim --out:/tmp/named && /tmp/named
import std/[locks, os]
import supranim_tasks

var lock: Lock
initLock(lock)

var m = newTaskManager(poolSize = 2)

# Arm a named repeating task.
let beatCb = proc(res: int) =
  withLock lock:
    echo "heartbeat tick"
discard m.submitRepeating(150, proc(): int = 1, beatCb, name = "heartbeat")
echo "heartbeat armed: ", m.hasTask("heartbeat")
sleep(400)

# cancelTask stops the firing but keeps the name reserved.
m.cancelTask("heartbeat")
echo "after cancel, still tracked: ", m.hasTask("heartbeat")
try:
  discard m.submitDelayed(1000, proc(): int = 1,
    proc(res: int) = discard, name = "heartbeat")
except CatchableError as err:
  echo "duplicate name rejected: ", err.msg

# removeTask is strict: no further fires, name freed for reuse.
let cleanupCb = proc(res: int) =
  withLock lock:
    echo "cleanup fired (should not happen)"
discard m.submitDelayed(60_000, proc(): int = 7, cleanupCb, name = "cleanup")
echo "cleanup armed: ", m.hasTask("cleanup")
m.removeTask("cleanup")
sleep(100) # removal lands at the next heartbeat (~10ms)
echo "cleanup removed: ", not m.hasTask("cleanup")

# Unknown ids and names are safe no-ops.
m.cancelTask("missing")
m.removeTask("missing")

m.close()
echo "closed, running=", m.isRunning
