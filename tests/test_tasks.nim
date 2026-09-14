# Unit tests for supranim_tasks (powpow-backed task manager).
#
# Two angles: functionality (results, timers, lifecycle, named tasks)
# and non-blocking behavior (the scheduler event loop stays responsive
# while workers and the dispatch thread are saturated — no call in this
# API waits on job execution).
import std/[unittest, locks, os, times]

import supranim_tasks

var resLock: Lock
initLock(resLock)

template waitUntil(condExpr: untyped, timeoutMs: int): bool =
  ## Spin until condExpr holds (checked under resLock) or timeout.
  var ok = false
  var waited = 0
  while waited < timeoutMs:
    withLock resLock:
      if condExpr:
        ok = true
        break
    sleep(10)
    inc(waited, 10)
  ok

template elapsedMs(t0: float): int =
  int((epochTime() - t0) * 1000)

template checkSubmit(call: untyped) =
  ## `submit` returns a `JobId`: assert it is a real id.
  check call.isValid

suite "task manager functionality":
  test "immediate submit delivers the result":
    var got: seq[int]
    var m = newTaskManager(poolSize = 2)
    check m.isRunning
    check m.poolSize == 2
    checkSubmit m.submit(proc(): int = 40 + 2, proc(res: int) =
      withLock resLock:
        got.add(res))
    check waitUntil(got.len == 1, 2000)
    withLock resLock:
      check got == @[42]
    m.close()
    check not m.isRunning

  test "delayed submit fires once after the delay":
    var got: seq[string]
    var m = newTaskManager(poolSize = 2)
    let t0 = epochTime()
    discard m.submitDelayed(150, proc(): string = "late", proc(res: string) =
      withLock resLock:
        got.add(res))
    check waitUntil(got.len == 1, 3000)
    check epochTime() - t0 >= 0.12
    sleep(250)
    withLock resLock:
      check got.len == 1
    m.close()

  test "repeating submit fires until cancelled":
    var count = 0
    var m = newTaskManager(poolSize = 2)
    let id = m.submitRepeating(100, proc(): int = 1, proc(res: int) =
      withLock resLock:
        inc count)
    check waitUntil(count >= 3, 3000)
    m.cancel(id)
    var frozen = 0
    withLock resLock:
      frozen = count
    sleep(300)
    withLock resLock:
      check count == frozen
    m.close()

  test "job errors go to onError, cb is skipped":
    var oks: seq[int]
    var errs: seq[string]
    var m = newTaskManager(poolSize = 2)
    checkSubmit m.submit(proc(): int = raise newException(ValueError, "boom"),
      proc(res: int) =
        withLock resLock:
          oks.add(res),
      proc(err: ref CatchableError) =
        withLock resLock:
          errs.add(err.msg))
    check waitUntil(errs.len == 1, 2000)
    sleep(200)
    withLock resLock:
      check errs == @["boom"]
      check oks.len == 0
    m.close()

  test "stop rejects new work, close is idempotent":
    var m = newTaskManager(poolSize = 1)
    m.stop()
    check not m.isRunning
    check not m.submit(proc(): int = 1, proc(res: int) = discard).isValid
    expect CatchableError:
      discard m.submitDelayed(50, proc(): int = 1, proc(res: int) = discard)
    m.close()
    m.close()
    check not m.isRunning

  test "halt stops the manager after the delay":
    var m = newTaskManager(poolSize = 1)
    check m.halt(150)
    sleep(600)
    check not m.isRunning
    m.close()

  test "cancelTask by name stops a repeating task":
    var count = 0
    var m = newTaskManager(poolSize = 2)
    let tickCb = proc(res: int) =
      withLock resLock:
        inc count
    discard m.submitRepeating(80, proc(): int = 1, tickCb, name = "ticker")
    check m.hasTask("ticker")
    check waitUntil(count >= 2, 3000)
    m.cancelTask("ticker")
    sleep(100) # cancel lands at the next heartbeat (~10ms)
    # Cancel stops the firing but keeps the name reserved (unlike
    # removeTask): re-registering the name still raises.
    check m.hasTask("ticker")
    expect CatchableError:
      discard m.submitRepeating(80, proc(): int = 1, tickCb,
        name = "ticker")
    var frozen = 0
    withLock resLock:
      frozen = count
    sleep(250)
    withLock resLock:
      check count == frozen
    m.close()

  test "removeTask suppresses a pending one-shot and frees the name":
    var got: seq[int]
    var m = newTaskManager(poolSize = 2)
    let addCb = proc(res: int) =
      withLock resLock:
        got.add(res)
    discard m.submitDelayed(400, proc(): int = 1, addCb, name = "once")
    check m.hasTask("once")
    m.removeTask("once")
    sleep(100) # removal lands at the next heartbeat (~10ms)
    check not m.hasTask("once")
    # Name is reusable immediately, job of the removed task never runs.
    discard m.submitDelayed(400, proc(): int = 2, addCb, name = "once")
    check m.hasTask("once")
    check waitUntil(got.len == 1, 3000)
    sleep(300)
    withLock resLock:
      check got == @[2]
    m.close()

  test "duplicate task names raise":
    var m = newTaskManager(poolSize = 2)
    discard m.submitDelayed(5_000, proc(): int = 1,
      proc(res: int) = discard, name = "dup")
    expect CatchableError:
      discard m.submitDelayed(5_000, proc(): int = 2,
        proc(res: int) = discard, name = "dup")
    m.removeTask("dup")
    m.close()

  test "unknown ids and names are no-ops":
    var m = newTaskManager(poolSize = 1)
    m.cancelTask("missing")
    m.removeTask("missing")
    check not m.hasTask("missing")
    m.close()

suite "task manager never blocks":
  test "submitting returns at once while the worker is busy":
    # One worker stuck 700ms in a job: 100 further submits must return
    # immediately (queueing), not after the worker frees up (~700ms+).
    var m = newTaskManager(poolSize = 1)
    checkSubmit m.submit(proc(): int =
      sleep(700)
      1, proc(res: int) = discard)
    sleep(50) # let the slow job occupy the worker
    let t0 = epochTime()
    for i in 1 .. 100:
      let v = i
      checkSubmit m.submit(proc(): int = v, proc(res: int) = discard)
    check elapsedMs(t0) < 3000
    m.close()

  test "delayed timers fire on schedule while the pool is saturated":
    # Sole worker busy 800ms: a 100ms timer still fires on time (the
    # scheduler loop is independent of workers). The job itself queues
    # and runs once the worker frees up — nothing is dropped.
    var got: seq[int]
    var m = newTaskManager(poolSize = 1)
    checkSubmit m.submit(proc(): int =
      sleep(800)
      1, proc(res: int) = discard)
    sleep(50)
    let addCb = proc(res: int) =
      withLock resLock:
        got.add(res)
    let t0 = epochTime()
    let id = m.submitDelayed(100, proc(): int = 7, addCb)
    check waitUntil(not m.hasTask(id), 3000)
    check elapsedMs(t0) < 600 # fired while the worker was still busy
    check waitUntil(got.len == 1, 5000) # queued job still delivered
    withLock resLock:
      check got == @[7]
    m.close()

  test "repeating timers keep firing during slow jobs":
    # Both workers stuck 600ms: 100ms ticks queue up on the scheduler
    # side and none are lost — they deliver once workers free up.
    var count = 0
    var m = newTaskManager(poolSize = 2)
    let slowJob = proc(): int =
      sleep(600)
      1
    let slowCb = proc(res: int) = discard
    checkSubmit m.submit(slowJob, slowCb)
    checkSubmit m.submit(slowJob, slowCb)
    sleep(50)
    let tickCb = proc(res: int) =
      withLock resLock:
        inc count
    let id = m.submitRepeating(100, proc(): int = 1, tickCb)
    check waitUntil(count >= 4, 5000)
    m.cancelTask(id)
    m.close()

  test "timer jobs start while dispatch callbacks are slow":
    # A 500ms callback blocks the dispatch thread, but the scheduler
    # loop plus a free worker still start timer jobs on time. The job
    # start (worker side) is what we measure, not its delivery.
    var startedAt = 0.0
    var m = newTaskManager(poolSize = 2)
    checkSubmit m.submit(proc(): int = 1, proc(res: int) = sleep(500))
    sleep(50) # fast job done, dispatch now stuck 500ms in its callback
    let t0 = epochTime()
    let markCb = proc(res: int) = discard
    discard m.submitDelayed(100, proc(): int =
      withLock resLock:
        startedAt = epochTime()
      1, markCb)
    check waitUntil(startedAt > 0.0, 3000)
    check int((startedAt - t0) * 1000) < 400
    m.close()

  test "cancel and remove return at once under load":
    # Control ops only enqueue; they never wait on workers, the pool
    # drain, or each other — even with the worker stuck and timers
    # pending, 40 ops complete far below any execution latency.
    var m = newTaskManager(poolSize = 1)
    checkSubmit m.submit(proc(): int =
      sleep(800)
      1, proc(res: int) = discard)
    sleep(50)
    let noopCb = proc(res: int) = discard
    var ids: seq[TimerId]
    for i in 1 .. 20:
      ids.add(m.submitDelayed(5_000, proc(): int = 1, noopCb))
    let t0 = epochTime()
    for id in ids[0 .. 9]:
      m.cancelTask(id)
    for id in ids[10 .. ^1]:
      m.removeTask(id)
    check elapsedMs(t0) < 2000
    m.close()

  test "stop drains queued work instead of dropping it":
    var got: seq[int]
    var m = newTaskManager(poolSize = 2)
    for i in 1 .. 10:
      let v = i
      let addCb = proc(res: int) =
        withLock resLock:
          got.add(res)
      checkSubmit m.submit(proc(): int = v, addCb)
    m.stop() # graceful: every queued job still runs and delivers
    check waitUntil(got.len == 10, 5000)
    m.close()

suite "immediate job cancellation":
  test "cancelJob removes a queued job silently":
    var ran = 0
    var delivered = 0
    var errs: seq[string]
    var slowDone = false
    var m = newTaskManager(poolSize = 1)
    let slowCb = proc(res: int) =
      withLock resLock:
        slowDone = true
    checkSubmit m.submit(proc(): int =
      sleep(600)
      1, slowCb)
    sleep(50) # slow job occupies the only worker
    let fastCb = proc(res: int) =
      withLock resLock:
        inc delivered
    let fastErr = proc(err: ref CatchableError) =
      withLock resLock:
        errs.add(err.msg)
    let fastJob = proc(): int =
      withLock resLock:
        inc ran
      2
    let id = m.submit(fastJob, fastCb, fastErr)
    check id.isValid
    check m.cancelJob(id) # still queued: never runs, stays silent
    check not m.cancelJob(JobId(999_999))
    check not m.cancelJob(JobId(0))
    check waitUntil(slowDone, 3000)
    sleep(300) # the skipped node passes through the worker by now
    withLock resLock:
      check ran == 0
      check delivered == 0
      check errs.len == 0
    check not m.cancelJob(id) # already skipped: unknown
    m.close()

  test "cancelJob on a running job returns false, delivery normal":
    var started = false
    var got: seq[int]
    var m = newTaskManager(poolSize = 1)
    let runCb = proc(res: int) =
      withLock resLock:
        got.add(res)
    let id = m.submit(proc(): int =
      withLock resLock:
        started = true
      sleep(300)
      42, runCb)
    check id.isValid
    check waitUntil(started, 2000)
    check not m.cancelJob(id) # running: cannot preempt
    check waitUntil(got.len == 1, 3000)
    withLock resLock:
      check got == @[42]
    check not m.cancelJob(id) # delivered: unknown
    m.close()

  test "cancelJob from inside a callback":
    # Capturing the manager handle here is safe: close() joins every
    # pool thread before the manager can die — the same basis as the
    # documented halt-from-callback pattern.
    var ranC = 0
    var cbFired = false
    var cancelledFromCb = false
    var idCslot = JobId(0)
    var m = newTaskManager(poolSize = 1)
    checkSubmit m.submit(proc(): int =
      sleep(400)
      0, proc(res: int) = discard)
    sleep(50)
    let cbA = proc(res: int) =
      withLock resLock:
        cbFired = true
        cancelledFromCb = m.cancelJob(idCslot)
    checkSubmit m.submit(proc(): int = 0, cbA)
    let idB = m.submit(proc(): int =
      sleep(300)
      1, proc(res: int) = discard)
    check idB.isValid
    let jobC = proc(): int =
      withLock resLock:
        inc ranC
      2
    let cbC = proc(res: int) = discard
    withLock resLock:
      idCslot = m.submit(jobC, cbC)
    check idCslot.isValid
    check waitUntil(cbFired, 3000) # A ran; C still behind B
    check cancelledFromCb
    sleep(600) # B finishes, C's slot passes silently
    withLock resLock:
      check ranC == 0
    m.close()

  test "cancelJob under saturation, stop still drains":
    var ran = 0
    var m = newTaskManager(poolSize = 1)
    checkSubmit m.submit(proc(): int =
      sleep(500)
      0, proc(res: int) = discard)
    sleep(50)
    var ids: seq[JobId]
    for i in 1 .. 5:
      let v = i
      let cb = proc(res: int) = discard
      ids.add(m.submit(proc(): int =
        withLock resLock:
          inc ran
        v, cb))
    check m.cancelJob(ids[0])
    check m.cancelJob(ids[2])
    check m.cancelJob(ids[4])
    check not m.cancelJob(ids[0]) # already cancelled
    m.stop() # graceful drain: slow job + 2 survivors run
    withLock resLock:
      check ran == 2
    m.close()

  test "submit after stop returns an invalid id":
    var m = newTaskManager(poolSize = 1)
    m.stop()
    let id = m.submit(proc(): int = 1, proc(res: int) = discard)
    check not id.isValid
    check not m.cancelJob(id)
    m.close()

suite "scheduled tasks":
  test "next daily occurrence math":
    let nowDt = dateTime(2026, mSep, 14, 10, 0, 0, 0, local())
    let nowT = nowDt.toTime
    check nextDailyDelayMsFrom(nowT, 11, 0, 0) == 3_600_000
    let past = nextDailyDelayMsFrom(nowT, 9, 0, 0)
    check past > 20 * 3_600_000 # tomorrow, ~23h modulo DST
    check past < 26 * 3_600_000

  test "next weekly occurrence math":
    let nowDt = dateTime(2026, mSep, 14, 10, 0, 0, 0, local())
    let nowT = nowDt.toTime
    let wd = nowDt.weekday
    check nextWeeklyDelayMsFrom(nowT, wd, 11, 0, 0) == 3_600_000
    let nxt = nextWeeklyDelayMsFrom(nowT, wd, 9, 0, 0)
    check nxt > 6 * 24 * 3_600_000 # next week, ~7d minus 1h
    check nxt < 8 * 24 * 3_600_000

  test "same-second echo rolls to the next day":
    # A chain fire landing inside its own target second must not
    # recompute a ~0ms delay and echo the occurrence twice.
    let echoNow = dateTime(2026, mSep, 14, 10, 0, 0, 500_000_000,
      local()).toTime
    check nextDailyDelayMsFrom(echoNow, 10, 0, 0,
      minLeadMs = 60_000) > 20 * 3_600_000
    # ...while an explicit same-second schedule still fires ASAP.
    check nextDailyDelayMsFrom(echoNow, 10, 0, 1,
      minLeadMs = 1) < 2000

  test "scheduleAt future fires once":
    var got: seq[int]
    var m = newTaskManager(poolSize = 2)
    let at = (getTime() + initDuration(milliseconds = 300)).local()
    let id = m.scheduleAt(at, proc(): int = 7, proc(res: int) =
      withLock resLock:
        got.add(res))
    check m.taskStatus(id) == taskArmed
    check waitUntil(got.len == 1, 3000)
    sleep(300)
    withLock resLock:
      check got.len == 1
    check m.taskStatus(id) == taskUnknown # fired one-shots forget
    m.close()

  test "scheduleAt past stays inactive and never fires":
    var got: seq[int]
    var m = newTaskManager(poolSize = 2)
    let collectCb = proc(res: int) =
      withLock resLock:
        got.add(res)
    let past = (getTime() - initDuration(seconds = 5)).local()
    let id = m.scheduleAt(past, proc(): int = 1, collectCb,
      name = "pasty")
    sleep(300) # let the registration land on the scheduler thread
    check m.taskStatus(id) == taskInactive
    check m.taskStatus("pasty") == taskInactive
    check m.hasTask("pasty")
    sleep(500)
    withLock resLock:
      check got.len == 0
    check m.taskStatus(id) == taskInactive
    # Cancel keeps it inactive with the name reserved.
    m.cancelTask("pasty")
    sleep(150)
    check m.taskStatus("pasty") == taskInactive
    expect CatchableError:
      discard m.scheduleAt(
        (getTime() + initDuration(seconds = 60)).local(),
        proc(): int = 2, proc(res: int) = discard, name = "pasty")
    # Remove drops tracking and frees the name for reuse.
    m.removeTask("pasty")
    sleep(150)
    check m.taskStatus("pasty") == taskUnknown
    check not m.hasTask("pasty")
    let id2 = m.scheduleAt(
      (getTime() + initDuration(milliseconds = 200)).local(),
      proc(): int = 3, collectCb, name = "pasty")
    check m.taskStatus(id2) == taskArmed
    check waitUntil(got.len == 1, 3000)
    withLock resLock:
      check got == @[3]
    m.close()

  test "taskStatus matrix":
    var m = newTaskManager(poolSize = 2)
    let id = m.submitDelayed(5_000, proc(): int = 1,
      proc(res: int) = discard, name = "s1")
    sleep(150) # let the registration land
    check m.taskStatus(id) == taskArmed
    check m.taskStatus("s1") == taskArmed
    m.cancelTask(id)
    sleep(150)
    check m.taskStatus(id) == taskCancelled
    check m.taskStatus("s1") == taskCancelled
    check m.taskStatus(TimerId(999_999)) == taskUnknown
    check m.taskStatus("nope") == taskUnknown
    m.removeTask("s1")
    sleep(150)
    check m.taskStatus("s1") == taskUnknown
    m.close()

  test "scheduleDaily fires at the wall-clock second":
    var count = 0
    var m = newTaskManager(poolSize = 2)
    let target = (getTime() + initDuration(seconds = 2)).local()
    let tickCb = proc(res: int) =
      withLock resLock:
        inc count
    discard m.scheduleDaily(target.hour, target.minute, target.second,
      proc(): int = 1, tickCb, name = "daily-tick")
    check m.taskStatus("daily-tick") == taskArmed
    check waitUntil(count >= 1, 8000)
    m.cancelTask("daily-tick")
    sleep(150)
    # Next occurrence would be tomorrow: exactly one fire happened.
    check m.taskStatus("daily-tick") == taskCancelled
    withLock resLock:
      check count == 1
    m.removeTask("daily-tick")
    m.close()

  test "stale chain id still stops the live occurrence":
    var count = 0
    var m = newTaskManager(poolSize = 2)
    let target = (getTime() + initDuration(seconds = 2)).local()
    let tickCb = proc(res: int) =
      withLock resLock:
        inc count
    let first = m.scheduleDaily(target.hour, target.minute,
      target.second, proc(): int = 1, tickCb, name = "chain")
    check waitUntil(count >= 1, 8000) # fired once, re-armed by now
    m.cancelTask(first) # stale id follows supersession to the live one
    sleep(150)
    check m.taskStatus("chain") == taskCancelled
    withLock resLock:
      check count == 1
    m.removeTask("chain")
    m.close()
