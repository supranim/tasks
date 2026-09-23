# Fast, framework-agnostic background task scheduling and execution.
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/supranim/tasks
#
## A tiny task manager built on powpow: a `ThreadPool` for execution
## plus a private scheduler `Loop` (own thread) for delayed one-shot
## and repeating tasks.
##
## - Immediate work goes straight to the pool: jobs run on worker
##   threads, `cb`/`onError` fire serialized on the pool dispatch
##   thread (never on the caller's thread — lock shared state there).
##   `submit` returns a `JobId`; `cancelJob` removes a still-queued
##   job synchronously (running jobs cannot be preempted).
## - Everything for the scheduler thread (timer registration, cancels,
##   removals, halts, loop stop) crosses as a `SchedOp`: one proc plus
##   one unmanaged arg. A heartbeat callback drains the pending ops on
##   the scheduler thread — no managed closure is ever posted across
##   threads, so the per-thread cycle tables stay consistent.
## - Delayed/repeating tasks may carry a `name`: `cancelTask` drops the
##   timer, `removeTask` additionally frees the name for reuse and
##   guarantees no further fires. Unnamed tasks are tracked by id only.
## - Wall-clock scheduling on `std/times` (local time): `scheduleAt`
##   runs once at a `DateTime` (past times stay tracked as
##   `taskInactive` and never fire); `scheduleDaily`/`scheduleWeekly`
##   re-arm per occurrence. `taskStatus` reports
##   `taskArmed`/`taskCancelled`/`taskInactive`/`taskUnknown` per id or
##   name.
## - Framework-agnostic: this module imports only `pkg/powpow` and
##   the standard library. No C libraries, no event loop to drive.
##
## Threading contract: Nim ORC tracks cycle candidates per thread, so
## a managed cell released last on a thread other than the one that
## shared it corrupts that table. Hence: job/cb closures must not
## capture `ref` objects — directly or nested (e.g. `seq[SomeRef]`) —
## across threads. Capture values, strings, seqs of values, locks,
## atomics and raw pointers instead. This matches powpow's
## `submitWork` envelope ("single-owner movable `T`"): closures
## capturing true refs are equally unsafe there.
##
## Lifecycle: `newTaskManager` starts the pool and the scheduler
## thread. `stop`/`shutdown` reject new work and tear down the pool
## (graceful drain vs discard-queued); `close` additionally joins the
## scheduler thread and frees the loop. Teardown is idempotent, but
## `close` must finish on the creating thread: called from inside a
## pool/scheduler callback it stops the work and skips the join, and
## a later `close` from the creating thread completes the teardown.
## `stop`, `shutdown` and `close` join pool threads, so they must run
## outside pool jobs/callbacks — from inside one, call `halt`.

import std/[locks, atomics, tables]
import std/typedthreads

import pkg/powpow/loop
import pkg/powpow/threadpool

export threadpool.ThreadPoolError

from pkg/powpow/types import TimerId
export TimerId

from std/times import DateTime, WeekDay, Time, toTime,
  getTime, local, dateTime, fromUnix, initDuration, inMilliseconds,
  year, month, monthday, weekday, `+`, `-`
export DateTime, WeekDay, Time

const
  HeartbeatMs = 10
    ## Scheduler-loop cadence. Doubles as the op pump, so control
    ## operations take effect within ~10ms, and guarantees poll
    ## iterations even with no timers armed (same reason powpow's pool
    ## loop has one). The heartbeat environment is one raw pointer.

type
  TaskManager* = ref object
    pool: ThreadPool
      # Worker pool. Created, pinned and destroyed on the creating
      # thread (see `newTaskManager`); other threads only ever take
      # transient call copies, exactly like powpow's own dispatch
      # thread does with its loop.
    sched: Loop
      # Private scheduler loop. Created on the constructing thread,
      # driven on the scheduler thread (via a borrowed handle that is
      # never reference-counted there), closed on the constructing
      # thread after the join in `close`.
    ctl: ptr CtlBlock
      # Unmanaged control block shared with the scheduler thread.
    schedThread: Thread[pointer]
    workerCount: int
    closed: Atomic[bool]
      # Full teardown completed (join + loop close + deinit). Atomic
      # so post-close calls bail out without touching freed memory.

  SchedProc* = proc(ctl: ptr CtlBlock, arg: pointer) {.nimcall, gcsafe.}
    ## One control operation, run on the scheduler thread by the drain
    ## (`run`) — or on the creating thread for leftover ops at `close`
    ## (`abort`). Args are unmanaged (`allocShared0`): crossing them
    ## touches no cycle table on either thread.

  SchedOp* = tuple[run, abort: SchedProc, arg: pointer]

  TaskStatus* = enum
    ## Lifecycle state of a delayed/repeating task, per id or name.
    taskArmed      ## Timer live, will fire (or re-arm, for chained tasks).
    taskCancelled  ## `cancelTask` stopped it; the name stays reserved.
    taskInactive   ## Past `scheduleAt`: tracked but never armed, never fires.
    taskUnknown    ## No such task: fired, removed, or never existed.

  JobId* = distinct int
    ## Handle for one immediate (`submit`) job. `JobId(0)` is invalid
    ## (submission rejected while stopping/closed).

  JobState = enum
    ## Immediate-job lifecycle. `jsQueued` is the only cancellable
    ## state; both transitions out of it are serialized under
    ## `ctl.lock`, so exactly one of (run) or (cancel) wins.
    jsQueued, jsRunning, jsCancelled

  JobCell = ptr JobCellObj
  JobCellObj = object
    ## Unmanaged per-job cell: a plain state machine, no managed
    ## fields, so any thread may touch it under `ctl.lock`.
    ## Allocated on the submitting thread; freed on the dispatch
    ## thread after delivery/skip, or by the post-join `reapJobs`
    ## walk. Lifetime is covered by the `jobs` entry: removal and
    ## free happen together under the lock.
    state: JobState

  JobSkipped = object of CatchableError
    ## Private signal: a cancelled immediate job reaching a worker.
    ## Swallowed by the dispatch wrapper — user callbacks never see
    ## it (cancellation is silent, like dropping a timer).

  SchedKind = enum
    ## How a payload's timer is (re-)armed. `skPlain` is the classic
    ## delay/interval path; `skAt` arms once at a Unix-ms target (or
    ## goes inactive when past); `skDaily`/`skWeekly` chain one-shots,
    ## recomputing the next wall-clock occurrence after every fire.
    skPlain, skAt, skDaily, skWeekly

  TaskRec = object
    # One tracked timer, armed or not. The payload pointer's first
    # field is its `dead` flag (see `TimerPayloadObj`); `name` is
    # empty for unnamed tasks. Cancelled entries stay until `close`
    # (powpow drops timer nodes lazily, with no hook); inactive
    # entries own no timer node at all.
    payload: pointer
    free: SchedProc
    name: string
    isInterval: bool
    armed: bool
    cancelled: bool

  CtlBlock = object
    lock: Lock
      ## Guards everything below. Held briefly; op bodies never block
      ## on another thread while holding it.
    startedCond: Cond
    started: bool
    schedId: int
    stopping: bool
    schedRaw: pointer     ## Borrowed `Loop`; see `sched`
    poolRaw: pointer      ## Borrowed `ThreadPool`; see `pool`
    pending: seq[SchedOp]
      ## Ops waiting for the drain. Elements are raw procs/pointers,
      ## so crossing the seq touches no cycle table.
    byId: Table[int, TaskRec]
      ## Armed timers by timer id. Entries hold no `ref`s (pointer,
      ## proc, string, bool) — lock-guarded cross-thread count ops on
      ## the table stay in acyclic cells only.
    byName: Table[string, int]
      ## Named task index into `byId`.
    superseded: Table[int, int]
      ## Chained re-arm hops, old timer id to current one. Lets a
      ## stale id still reach the live occurrence at cancel/remove
      ## time. One small entry per fire; reclaimed at `close`.
    nextInactiveId: int
      ## Synthetic-id source for past `scheduleAt` tasks (no powpow
      ## timer node exists for them). Counts down from -1; powpow ids
      ## are positive, so the ranges never collide.
    jobs: Table[int, JobCell]
      ## Immediate jobs by id. Entries hold one unmanaged cell
      ## pointer each — lock-guarded count ops stay acyclic. An entry
      ## lives from `submit` until dispatch delivery/skip, or until
      ## the post-join `reapJobs` walk (`shutdown` discards queued
      ## nodes whose dispatch never runs).
    jobSeq: int
      ## Immediate-job id source. Counts up from 1; `0` stays the
      ## invalid sentinel.

  TimerPayload[T] = ptr TimerPayloadObj[T]
  TimerPayloadObj[T] = object
    ## One delayed/repeating registration. Unmanaged (`allocShared0`):
    ## crossing it touches no cycle table. `dead` is the first field
    ## so `removeTask` can mark any payload through an untyped pointer.
    ## The job closures ride as struct fields with strictly sequenced
    ## ownership — written on the caller thread before enqueueing,
    ## extracted on the scheduler thread in the drain or reclaimed by
    ## `close` post-join (in-contract payloads are acyclic, so these
    ## are plain count ops; ref-capturing payloads are out of contract,
    ## same as with raw `submitWork`).
    dead: Atomic[bool]
    lock: Lock
    cond: Cond
    done: bool
    err: string
    isInterval: bool
    delayMs: int
    id: TimerId
    name: string
    kind: SchedKind
      ## Wall-clock scheduling flavor. Only raw ints cross threads —
      ## the target instant (`targetMs`), clock fields and weekday —
      ## never a `DateTime`/`Timezone` (managed refs stay on the
      ## thread that builds them).
    targetMs: int64   ## `skAt`: target instant as Unix milliseconds.
    hour, minute, second: int ## `skDaily`/`skWeekly`: local clock time.
    weekdayOrd: int   ## `skWeekly`: `ord(WeekDay)`, Monday = 0.
    ctlRaw: pointer
    poolRaw: pointer
    schedRaw: pointer
    job: proc(): T {.closure.}
    cb: proc(res: T) {.closure.}
    onError: proc(err: ref CatchableError) {.closure.}

  TimerArg = ptr TimerArgObj
  TimerArgObj = object
    ## Unmanaged op arg carrying one timer id (`doCancel`/`doRemove`).
    id: TimerId

  HaltArg = ptr HaltArgObj
  HaltArgObj = object
    ## Unmanaged op arg carrying the halt delay (`doHalt`).
    delayMs: int

  NameArg = ptr NameArgObj
  NameArgObj = object
    ## Unmanaged op arg carrying a task name (`doCancelByName` /
    ## `doRemoveByName`). The string is written on the caller thread,
    ## read and cleared on the scheduler thread — the same crossing
    ## the payload `name` field already performs.
    name: string

proc `==`*(a, b: JobId): bool {.borrow.}
  ## Job ids compare by value; `JobId(0)` is the invalid sentinel.

proc isValid*(id: JobId): bool {.inline.} =
  ## True for a real submission id (anything but `JobId(0)`).
  id != JobId(0)

proc forgetJob(ctl: ptr CtlBlock, id: int) =
  ## Drop one immediate job's tracking entry and free its cell. Takes
  ## `ctl.lock`; used on the dispatch thread after delivery/skip and
  ## on the submitting thread when the pool rejects the wrapped job.
  withLock(ctl.lock):
    if ctl.jobs.hasKey(id):
      deallocShared(ctl.jobs[id])
      ctl.jobs.del(id)

proc reapJobs(ctl: ptr CtlBlock) =
  ## Free cells of immediate jobs that never reached dispatch
  ## (queued-and-discarded at `shutdown`, or strays). Runs only where
  ## pool threads are provably joined by this thread — after
  ## `closeThreadPool`/`shutdownThreadPool` here, or in the `close`
  ## walk — so no worker or dispatch callback can touch them.
  withLock(ctl.lock):
    for _, cell in ctl.jobs:
      deallocShared(cell)
    ctl.jobs.clear()

proc wrapJob[T](ctl: ptr CtlBlock, cell: JobCell,
    job: proc(): T {.closure.}): proc(): T {.closure.} =
  ## Worker-side gate for one immediate job. The queued-to-running
  ## transition is serialized with `cancelJob` under `ctl.lock`:
  ## exactly one wins. Losers raise `JobSkipped`, which the pool
  ## routes to the dispatch wrapper for a silent drop. Captures are
  ## two raw pointers plus the user closure — acyclic, safe to post.
  result = proc(): T =
    var run = false
    withLock(ctl.lock):
      if cell.state == jsQueued:
        cell.state = jsRunning
        run = true
    if not run:
      raise newException(JobSkipped, "job cancelled while queued")
    job()

proc wrapCb[T](ctl: ptr CtlBlock, id: int,
    cb: proc(res: T) {.closure.}): proc(res: T) {.closure.} =
  ## Dispatch-side delivery: forget tracking first (safe even if the
  ## user callback raises), then deliver.
  result = proc(res: T) =
    forgetJob(ctl, id)
    cb(res)

proc wrapOnError[T](ctl: ptr CtlBlock, id: int,
    onError: proc(err: ref CatchableError) {.closure.}):
    proc(err: ref CatchableError) {.closure.} =
  ## Dispatch-side failure path: skips stay silent, real failures go
  ## to the user handler. Tracking is forgotten first, as in `wrapCb`.
  result = proc(err: ref CatchableError) =
    forgetJob(ctl, id)
    if err of JobSkipped:
      return
    if onError != nil:
      onError(err)

proc markDead(p: pointer) {.inline.} =
  ## Flag a payload dead through its first field (see `TimerPayloadObj`).
  cast[ptr Atomic[bool]](p)[].store(true)

proc isDead(p: pointer): bool {.inline.} =
  cast[ptr Atomic[bool]](p)[].load

proc freePayload[T](ctl: ptr CtlBlock, raw: pointer) {.nimcall, gcsafe.} =
  ## Reclaim one payload: drop its closure fields, then free it. Runs
  ## on the scheduler thread (dead fired payload) or the creating
  ## thread post-join (`close` walk): each field's counts were taken
  ## on the caller thread and die here, strictly sequenced.
  let p = cast[TimerPayload[T]](raw)
  p.job = nil
  p.cb = nil
  p.onError = nil
  deinitCond(p.cond)
  deinitLock(p.lock)
  deallocShared(p)

proc forgetTask(ctl: ptr CtlBlock, id: TimerId) =
  ## Drop a fired payload from the tracking maps (caller holds no
  ## locks; takes `ctl.lock` briefly). Missing entries are fine — a
  ## removed-then-spuriously-fired payload is already gone.
  withLock(ctl.lock):
    if ctl.byId.hasKey(int(id)):
      let rec = ctl.byId[int(id)]
      if rec.name.len > 0:
        ctl.byName.del(rec.name)
      ctl.byId.del(int(id))

proc nextDailyDelayMsFrom*(nowT: Time, hour, minute, second: int,
    minLeadMs = 1000): int =
  ## Milliseconds from `nowT` to the next local `hour:minute:second`
  ## at least `minLeadMs` out (today when still future enough, else
  ## tomorrow, stepping whole days so DST stays calendar-correct).
  ## Public so callers can preview when a daily task will fire. The
  ## lead exists for chains: a fire landing inside its own target
  ## second would otherwise recompute a ~0ms delay and echo the same
  ## occurrence twice.
  let nowDt = nowT.local()
  var target = dateTime(nowDt.year, nowDt.month, nowDt.monthday,
    hour, minute, second, 0, local())
  var diffMs = inMilliseconds(target.toTime - nowT)
  while diffMs < minLeadMs:
    target = target + initDuration(days = 1)
    diffMs = inMilliseconds(target.toTime - nowT)
  int(diffMs)

proc nextDailyDelayMs(hour, minute, second: int,
    minLeadMs = 1000): int =
  nextDailyDelayMsFrom(getTime(), hour, minute, second, minLeadMs)

proc nextWeeklyDelayMsFrom*(nowT: Time, weekday: WeekDay,
    hour, minute, second: int, minLeadMs = 1000): int =
  ## Milliseconds from `nowT` to the next local `weekday` +
  ## `hour:minute:second` at least `minLeadMs` out (this week when
  ## still future enough, else next week, stepping whole weeks).
  ## Same echo protection as `nextDailyDelayMsFrom`.
  let nowDt = nowT.local()
  var target = dateTime(nowDt.year, nowDt.month, nowDt.monthday,
    hour, minute, second, 0, local())
  let daysAhead = (ord(weekday) - ord(nowDt.weekday) + 7) mod 7
  target = target + initDuration(days = daysAhead)
  var diffMs = inMilliseconds(target.toTime - nowT)
  while diffMs < minLeadMs:
    target = target + initDuration(days = 7)
    diffMs = inMilliseconds(target.toTime - nowT)
  int(diffMs)

proc nextWeeklyDelayMs(weekdayOrd, hour, minute, second: int,
    minLeadMs = 1000): int =
  nextWeeklyDelayMsFrom(getTime(), WeekDay(weekdayOrd),
    hour, minute, second, minLeadMs)

proc firePayload[T](p: TimerPayload[T])

proc fireChain[T](p: TimerPayload[T], ctl: ptr CtlBlock,
    pool: ThreadPool) =
  ## Chained wall-clock fire (scheduler thread): submit this
  ## occurrence, then re-arm for the next one, recomputed from local
  ## `now()` so DST shifts are absorbed (one 23h/25h day). The maps
  ## move atomically to the fresh id (`byName` never flickers) and the
  ## hop is recorded for stale-id cancels. Cancelled/stopped chains
  ## are dropped without re-arming — the entry stays for `close`, like
  ## a plain cancelled timer, so `taskStatus` keeps reporting it.
  var sched = cast[Loop](p.schedRaw)
  # Chains demand a 60s lead: anything nearer is the same-second echo
  # of the occurrence just fired (see the helpers), never a legit
  # next day/week (those sit ~23h/7d out).
  let delayMs = if p.kind == skDaily:
    nextDailyDelayMs(p.hour, p.minute, p.second, 60_000)
  else:
    nextWeeklyDelayMs(p.weekdayOrd, p.hour, p.minute, p.second,
      60_000)
  let oldId = int(p.id)
  discard pool.submitWork(p.job, p.cb, p.onError)
  withLock(ctl.lock):
    if ctl.stopping:
      return
    if not ctl.byId.hasKey(oldId):
      return
    if ctl.byId[oldId].cancelled:
      return
    let fire: TimerCallback = proc(id: int) =
      firePayload(p)
    p.id = sched.addTimer(delayMs, fire)
    var rec = ctl.byId[oldId]
    ctl.byId.del(oldId)
    ctl.byId[int(p.id)] = rec
    ctl.superseded[oldId] = int(p.id)
    if rec.name.len > 0:
      ctl.byName[rec.name] = int(p.id)
  wasMoved(sched)

proc firePayload[T](p: TimerPayload[T]) =
  ## Timer-fire body (scheduler thread). Dead payloads (removed) are
  ## dropped silently — this is what makes `removeTask` strict. Live
  ## one-shot payloads are consumed: closures extracted, tracking
  ## dropped, payload freed. Repeating payloads stay armed: submit
  ## copies and keep ownership until `cancelTask`/`removeTask`/`close`.
  ## Chained (`skDaily`/`skWeekly`) payloads submit and re-arm.
  let ctl = cast[ptr CtlBlock](p.ctlRaw)
  var pool = cast[ThreadPool](p.poolRaw)
  if isDead(cast[pointer](p)):
    forgetTask(ctl, p.id)
    freePayload[T](ctl, cast[pointer](p))
    wasMoved(pool)
    return
  if p.kind == skDaily or p.kind == skWeekly:
    fireChain(p, ctl, pool)
    wasMoved(pool) # borrowed: the submit copy is the pool's own pattern
    return
  if p.isInterval:
    let ok = pool.submitWork(p.job, p.cb, p.onError)
    wasMoved(pool) # borrowed: the submit copy is the pool's own pattern
    discard ok
    return
  let j = p.job
  let c = p.cb
  let e = p.onError
  p.job = nil
  p.cb = nil
  p.onError = nil
  forgetTask(ctl, p.id)
  let ok = pool.submitWork(j, c, e)
  wasMoved(pool) # borrowed: the submit copy is the pool's own pattern
  deinitCond(p.cond)
  deinitLock(p.lock)
  deallocShared(p)
  discard ok

proc doRegister[T](ctl: ptr CtlBlock, raw: pointer) {.nimcall, gcsafe.} =
  ## Arm one payload's timer, then wake the registering caller with
  ## the id (or a duplicate-name error). The `fire` closure is built
  ## here so its environment is born, lives and dies on the scheduler
  ## thread — or harmlessly at loop close, holding no managed refs.
  ## `skAt` with a past target goes inactive instead: tracked under a
  ## synthetic id, but owning no timer node, so it never fires.
  let p = cast[TimerPayload[T]](raw)
  var sched = cast[Loop](p.schedRaw)
  withLock(ctl.lock):
    if p.name.len > 0 and ctl.byName.hasKey(p.name):
      withLock(p.lock):
        p.err = "duplicate task name: " & p.name
        signal(p.cond)
    elif ctl.stopping:
      withLock(p.lock):
        p.err = "TaskManager is stopping"
        signal(p.cond)
    else:
      if p.kind == skAt:
        let delayMs = int(p.targetMs -
          inMilliseconds(getTime() - fromUnix(0)))
        if delayMs > 0:
          let fire: TimerCallback = proc(id: int) =
            firePayload(p)
          p.id = sched.addTimer(delayMs, fire)
          ctl.byId[int(p.id)] = TaskRec(payload: raw,
            free: freePayload[T], name: p.name, armed: true)
          if p.name.len > 0:
            ctl.byName[p.name] = int(p.id)
        else:
          p.id = TimerId(ctl.nextInactiveId)
          dec ctl.nextInactiveId
          ctl.byId[int(p.id)] = TaskRec(payload: raw,
            free: freePayload[T], name: p.name, armed: false)
          if p.name.len > 0:
            ctl.byName[p.name] = int(p.id)
      else:
        let fire: TimerCallback = proc(id: int) =
          firePayload(p)
        if p.kind == skPlain and p.isInterval:
          p.id = sched.addInterval(p.delayMs, fire)
        elif p.kind == skPlain:
          p.id = sched.addTimer(p.delayMs, fire)
        elif p.kind == skDaily:
          # Explicit schedule: fire ASAP (lead 1ms). The echo guard
          # lives in the chain re-arm (`fireChain`), not here.
          p.id = sched.addTimer(
            nextDailyDelayMs(p.hour, p.minute, p.second, 1), fire)
        else:
          p.id = sched.addTimer(
            nextWeeklyDelayMs(p.weekdayOrd, p.hour, p.minute,
              p.second, 1), fire)
        ctl.byId[int(p.id)] = TaskRec(payload: raw,
          free: freePayload[T], name: p.name,
          isInterval: p.isInterval, armed: true)
        if p.name.len > 0:
          ctl.byName[p.name] = int(p.id)
      withLock(p.lock):
        p.done = true
        signal(p.cond)
  wasMoved(sched)

proc abortRegister[T](ctl: ptr CtlBlock, raw: pointer) {.nimcall, gcsafe.} =
  ## Wake a registration waiter with a closed error (leftover pending
  ## op at `close`). The caller frees its own payload after raising.
  let p = cast[TimerPayload[T]](raw)
  withLock(p.lock):
    p.err = "TaskManager is closed"
    signal(p.cond)

proc resolveCurrentId(ctl: ptr CtlBlock, id: int): int =
  ## Follow chained re-arm hops to the live occurrence id. Caller
  ## holds `ctl.lock`.
  result = id
  while ctl.superseded.hasKey(result):
    result = ctl.superseded[result]

proc cancelIdLocked(ctl: ptr CtlBlock, sched: var Loop, id: int) =
  ## Mark one tracked id cancelled and drop its timer node. Caller
  ## holds `ctl.lock`; `sched` stays borrowed (no count ops).
  let cur = ctl.resolveCurrentId(id)
  if ctl.byId.hasKey(cur) and ctl.byId[cur].armed:
    sched.cancelTimer(TimerId(cur))
    ctl.byId[cur].cancelled = true

proc removeIdLocked(ctl: ptr CtlBlock, sched: var Loop, id: int) =
  ## Strict removal of one tracked id: drop the timer node (when
  ## armed), flag the payload dead and free the name. Memory is
  ## reclaimed by the fire path or the `close` walk. Caller holds
  ## `ctl.lock`; `sched` stays borrowed.
  let cur = ctl.resolveCurrentId(id)
  if ctl.byId.hasKey(cur):
    let rec = ctl.byId[cur]
    if rec.armed:
      sched.cancelTimer(TimerId(cur))
    markDead(rec.payload)
    if rec.name.len > 0:
      ctl.byName.del(rec.name)

proc doCancel(ctl: ptr CtlBlock, raw: pointer) {.nimcall, gcsafe.} =
  ## Drop a timer by id. Unknown or already-fired ids are ignored;
  ## inactive ids are a no-op (nothing armed). Stale chained ids
  ## follow supersession to the live occurrence. Takes effect at the
  ## next heartbeat (~10ms); a lazily-dropped repeating timer may fire
  ## once more first.
  let arg = cast[TimerArg](raw)
  var sched = cast[Loop](ctl.schedRaw)
  withLock(ctl.lock):
    if ctl.byId.hasKey(int(arg.id)) or
        ctl.superseded.hasKey(int(arg.id)):
      ctl.cancelIdLocked(sched, int(arg.id))
    elif int(arg.id) >= 0:
      sched.cancelTimer(arg.id)
  wasMoved(sched)
  deallocShared(arg)

proc abortFreed(ctl: ptr CtlBlock, raw: pointer) {.nimcall, gcsafe.} =
  ## Leftover tiny-arg op at `close`: nothing to run, just free the arg.
  if raw != nil:
    deallocShared(raw)

proc abortNameArg(ctl: ptr CtlBlock, raw: pointer) {.nimcall, gcsafe.} =
  ## Leftover name-arg op at `close`: drop the managed name field
  ## first (plain `deallocShared` would leak its cell), then free.
  if raw != nil:
    let arg = cast[NameArg](raw)
    arg.name = ""
    deallocShared(arg)

proc doCancelByName(ctl: ptr CtlBlock, raw: pointer) {.nimcall, gcsafe.} =
  ## Drop a named task. The name resolves here — on the scheduler
  ## thread, serialized with fires — so a chain re-arming between the
  ## call and the op still stops. Unknown names are ignored.
  let arg = cast[NameArg](raw)
  var sched = cast[Loop](ctl.schedRaw)
  withLock(ctl.lock):
    if ctl.byName.hasKey(arg.name):
      ctl.cancelIdLocked(sched, ctl.byName[arg.name])
  wasMoved(sched)
  arg.name = ""
  deallocShared(arg)

proc doRemoveByName(ctl: ptr CtlBlock, raw: pointer) {.nimcall, gcsafe.} =
  ## Strict removal by name, resolved on the scheduler thread like
  ## `doCancelByName`. Unknown names are ignored.
  let arg = cast[NameArg](raw)
  var sched = cast[Loop](ctl.schedRaw)
  withLock(ctl.lock):
    if ctl.byName.hasKey(arg.name):
      ctl.removeIdLocked(sched, ctl.byName[arg.name])
  wasMoved(sched)
  arg.name = ""
  deallocShared(arg)

proc doRemove(ctl: ptr CtlBlock, raw: pointer) {.nimcall, gcsafe.} =
  ## Strict removal by id: drop the timer, flag the payload dead (a
  ## lazy spurious fire drops it silently instead of running the job)
  ## and free the name for reuse. Stale chained ids follow
  ## supersession to the live occurrence. Memory is reclaimed by the
  ## fire path or the `close` walk. Unknown ids are ignored.
  let arg = cast[TimerArg](raw)
  var sched = cast[Loop](ctl.schedRaw)
  withLock(ctl.lock):
    if ctl.byId.hasKey(int(arg.id)) or
        ctl.superseded.hasKey(int(arg.id)):
      ctl.removeIdLocked(sched, int(arg.id))
    elif int(arg.id) >= 0:
      sched.cancelTimer(arg.id)
  wasMoved(sched)
  deallocShared(arg)

proc haltFire(ctlRaw: pointer): TimerCallback =
  ## Build the halt timer callback. Assembled on the scheduler thread
  ## only: the environment holds one raw pointer, and everything it
  ## touches stays borrowed — safe on whichever thread destroys it.
  result = proc(id: int) =
    let ctl = cast[ptr CtlBlock](ctlRaw)
    withLock(ctl.lock):
      ctl.stopping = true
    var sched = cast[Loop](ctl.schedRaw)
    sched.stop()
    wasMoved(sched)
    var pool = cast[ThreadPool](ctl.poolRaw)
    closeThreadPool(pool)
    wasMoved(pool)
    ctl.reapJobs() # drained: only dispatch-removed entries are gone

proc doHalt(ctl: ptr CtlBlock, raw: pointer) {.nimcall, gcsafe.} =
  ## Arm the delayed stop timer.
  let arg = cast[HaltArg](raw)
  var sched = cast[Loop](ctl.schedRaw)
  discard sched.addTimer(arg.delayMs, haltFire(cast[pointer](ctl)))
  wasMoved(sched)
  deallocShared(arg)

proc doStop(ctl: ptr CtlBlock, raw: pointer) {.nimcall, gcsafe.} =
  ## Stop the scheduler loop now. Posted by `stop`/`shutdown`; the
  ## pool teardown runs on the caller, the loop stop here.
  var sched = cast[Loop](ctl.schedRaw)
  sched.stop()
  wasMoved(sched)

proc abortNoop(ctl: ptr CtlBlock, raw: pointer) {.nimcall, gcsafe.} =
  discard

proc enqueueOp(ctl: ptr CtlBlock, run, abort: SchedProc, arg: pointer) =
  ## Append an op (any thread). The drain picks it up within one
  ## heartbeat. Single lock, no queue discipline beyond FIFO.
  withLock(ctl.lock):
    ctl.pending.add((run, abort, arg))

proc drainCtl(ctlRaw: pointer) =
  ## Op pump (scheduler thread, from the heartbeat). Swaps the pending
  ## list under the lock, then runs each op with no lock held, so
  ## producers never block on op bodies.
  let ctl = cast[ptr CtlBlock](ctlRaw)
  var batch: seq[SchedOp]
  withLock(ctl.lock):
    batch = ctl.pending
    ctl.pending.setLen(0)
  for op in batch:
    op.run(ctl, op.arg)

proc schedMain(arg: pointer) {.thread.} =
  ## Scheduler thread entry. No managed cell crosses here — `arg` is
  ## a raw control-block pointer and the loop is used borrowed.
  let ctl = cast[ptr CtlBlock](arg)
  withLock(ctl.lock):
    ctl.schedId = getThreadId()
    ctl.started = true
    signal(ctl.startedCond)
  # Borrowed, not owned: no reference-count op on exit — the same
  # discipline as powpow's own dispatch thread entry.
  var sched = cast[Loop](ctl.schedRaw)
  {.cast(gcsafe).}:
    sched.run()
  wasMoved(sched)

proc isSchedulerThread(m: TaskManager): bool {.inline.} =
  ## True when called on the scheduler thread. Reads control-block
  ## value fields only; callers check `closed` first, and the block is
  ## freed only at the end of `close` on the creating thread.
  let ctl = cast[ptr CtlBlock](m.ctl)
  ctl.started and getThreadId() == ctl.schedId

proc newTaskManager*(poolSize = 4): TaskManager =
  ## Create a manager with `poolSize` pool workers and start the
  ## private scheduler thread. Blocks briefly until the scheduler
  ## loop is running, so the first delayed submit never races startup.
  result = TaskManager()
  result.pool = newThreadPool(poolSize)
  result.workerCount = poolSize
  result.sched = newLoop()
  result.ctl = cast[ptr CtlBlock](allocShared0(sizeof(CtlBlock)))
  let ctl = result.ctl
  initLock(ctl.lock)
  initCond(ctl.startedCond)
  ctl.nextInactiveId = -1
  ctl.schedRaw = cast[pointer](result.sched)
  ctl.poolRaw = cast[pointer](result.pool)
  # Pin the pool cell's cycle slot to this thread: the first non-last
  # release wins `rootIdx`, so later call-copy releases on scheduler
  # and dispatch threads stay consistent with the destroy here in
  # `close` (via the manager field). Without this, a first submit
  # from another thread would pin the slot there and the destroy
  # would unregister on an empty table.
  block:
    let poolPin = result.pool
    discard poolPin.isNil
  # Heartbeat doubles as the op pump: the drain environment is one raw
  # pointer (acyclic), and the loop never sits in an empty wait, so
  # ops are picked up promptly without cross-thread posting (same
  # reason powpow's pool loop has a heartbeat).
  let ctlRaw = cast[pointer](ctl)
  discard result.sched.addInterval(HeartbeatMs,
    proc(id: int) = drainCtl(ctlRaw))
  createThread(result.schedThread, schedMain, ctlRaw)
  withLock(ctl.lock):
    while not ctl.started:
      wait(ctl.startedCond, ctl.lock)

proc rawPool*(m: TaskManager): ThreadPool =
  ## The underlying powpow pool, for advanced use (e.g. submitting
  ## with `submitWork` directly). Prefer the `submit` helpers.
  m.pool

proc poolSize*(m: TaskManager): int =
  ## Worker count the pool was created with.
  m.workerCount

proc isRunning*(m: TaskManager): bool =
  ## False once `stop`/`shutdown` began or `close` completed.
  if m.closed.load:
    return false
  let ctl = cast[ptr CtlBlock](m.ctl)
  withLock(ctl.lock):
    result = not ctl.stopping

proc hasTask*(m: TaskManager, name: string): bool =
  ## True while a named delayed/repeating task is armed. Removed,
  ## fired or unknown names return false.
  if m.closed.load:
    return false
  let ctl = cast[ptr CtlBlock](m.ctl)
  withLock(ctl.lock):
    result = ctl.byName.hasKey(name)

proc hasTask*(m: TaskManager, id: TimerId): bool =
  ## True while the timer id is armed (named or not).
  if m.closed.load:
    return false
  let ctl = cast[ptr CtlBlock](m.ctl)
  withLock(ctl.lock):
    result = ctl.byId.hasKey(int(id))

proc submit*[T](m: TaskManager,
    job: proc(): T {.closure.},
    cb: proc(res: T) {.closure.},
    onError: proc(err: ref CatchableError) {.closure.} = nil): JobId =
  ## Queue `job` for immediate execution on a pool worker. `cb(res)`
  ## fires on the pool dispatch thread; `onError(err)` instead when
  ## the job raises. Returns a `JobId` for `cancelJob`, or `JobId(0)`
  ## when stopping/closed or the pool is already torn down — then
  ## neither callback fires.
  ##
  ## Thread-safe: may be called from any thread, including from inside
  ## callbacks. See the module contract about captured references.
  if m.closed.load:
    return JobId(0)
  let ctl = cast[ptr CtlBlock](m.ctl)
  var cell = cast[JobCell](allocShared0(sizeof(JobCellObj)))
  cell.state = jsQueued
  var id = 0
  withLock(ctl.lock):
    if ctl.stopping:
      deallocShared(cell)
      return JobId(0)
    inc ctl.jobSeq
    id = ctl.jobSeq
    ctl.jobs[id] = cell
  if not m.pool.submitWork(wrapJob[T](ctl, cell, job),
      wrapCb[T](ctl, id, cb), wrapOnError[T](ctl, id, onError)):
    ctl.forgetJob(id)
    return JobId(0)
  JobId(id)

proc cancelJob*(m: TaskManager, id: JobId): bool =
  ## Cancel one immediate job by id. Returns true iff the job was
  ## still queued: it will never run and neither callback fires. A
  ## running, finished or unknown id returns false — a running job
  ## runs to completion and delivers normally (jobs cannot be
  ## preempted). Takes effect synchronously (lock-guarded, no
  ## heartbeat delay), so a true return is authoritative at call time.
  ## Thread-safe, including from inside callbacks.
  if m.closed.load:
    return false
  let ctl = cast[ptr CtlBlock](m.ctl)
  withLock(ctl.lock):
    if ctl.jobs.hasKey(int(id)) and
        ctl.jobs[int(id)].state == jsQueued:
      ctl.jobs[int(id)].state = jsCancelled
      return true
  false

proc schedule[T](m: TaskManager, isInterval: bool, delayMs: int,
    name: string, job: proc(): T {.closure.},
    cb: proc(res: T) {.closure.},
    onError: proc(err: ref CatchableError),
    kind = skPlain, targetMs = 0'i64,
    hour = 0, minute = 0, second = 0, weekdayOrd = 0): TimerId =
  ## Shared delayed/repeating/wall-clock registration. From foreign
  ## threads this is synchronous: it blocks until the drain owns the
  ## timer (one heartbeat). From the scheduler thread itself it
  ## registers directly — queueing would deadlock the waiter against
  ## its own drain. Duplicate non-empty names raise `CatchableError`.
  ## Only raw ints cross into the payload (see `TimerPayloadObj`).
  if m.closed.load:
    raise newException(CatchableError, "TaskManager is closed")
  let ctl = cast[ptr CtlBlock](m.ctl)
  withLock(ctl.lock):
    if ctl.stopping:
      raise newException(CatchableError, "TaskManager is stopping")
  var p = cast[TimerPayload[T]](allocShared0(sizeof(TimerPayloadObj[T])))
  initLock(p.lock)
  initCond(p.cond)
  p.isInterval = isInterval
  p.delayMs = delayMs
  p.name = name
  p.kind = kind
  p.targetMs = targetMs
  p.hour = hour
  p.minute = minute
  p.second = second
  p.weekdayOrd = weekdayOrd
  p.ctlRaw = cast[pointer](ctl)
  p.poolRaw = cast[pointer](m.pool)
  p.schedRaw = cast[pointer](m.sched)
  p.job = job
  p.cb = cb
  p.onError = onError
  if m.isSchedulerThread:
    doRegister[T](ctl, cast[pointer](p))
  else:
    enqueueOp(ctl, doRegister[T], abortRegister[T], cast[pointer](p))
    withLock(p.lock):
      while not p.done and p.err.len == 0:
        wait(p.cond, p.lock)
  var msg = ""
  withLock(p.lock):
    if p.err.len > 0:
      msg = p.err
      p.err = ""
  if msg.len > 0:
    freePayload[T](nil, cast[pointer](p))
    raise newException(CatchableError, msg)
  result = p.id

proc submitDelayed*[T](m: TaskManager, delayMs: int,
    job: proc(): T {.closure.},
    cb: proc(res: T) {.closure.},
    onError: proc(err: ref CatchableError) {.closure.} = nil,
    name = ""): TimerId =
  ## Run `job` once after `delayMs` milliseconds (then like `submit`).
  ## Returns the timer id for `cancelTask`/`removeTask`. An optional
  ## `name` arms it as a named task (duplicate names raise
  ## `CatchableError`). Raises `CatchableError` when stopping/closed.
  m.schedule(false, delayMs, name, job, cb, onError)

proc submitRepeating*[T](m: TaskManager, intervalMs: int,
    job: proc(): T {.closure.},
    cb: proc(res: T) {.closure.},
    onError: proc(err: ref CatchableError) {.closure.} = nil,
    name = ""): TimerId =
  ## Run `job` every `intervalMs` milliseconds until cancelled (then
  ## like `submit` per fire). Returns the timer id. An optional `name`
  ## arms it as a named task (duplicate names raise `CatchableError`).
  ## Raises `CatchableError` when stopping/closed. Stopping the
  ## manager drops all repeating timers. Cancelled payloads are
  ## reclaimed at `close` (powpow drops timer nodes lazily, with no
  ## hook).
  m.schedule(true, intervalMs, name, job, cb, onError)

proc scheduleAt*[T](m: TaskManager, at: DateTime,
    job: proc(): T {.closure.},
    cb: proc(res: T) {.closure.},
    onError: proc(err: ref CatchableError) {.closure.} = nil,
    name = ""): TimerId {.discardable.} =
  ## Run `job` once at wall-clock `at` (local time, then like
  ## `submit`). Returns the timer id for `cancelTask`/`removeTask` and
  ## `taskStatus`. A past `at` never fires: the task stays tracked as
  ## `taskInactive` (no warning, name still reserved). An optional
  ## `name` arms it as a named task (duplicate names raise
  ## `CatchableError`). Raises `CatchableError` when stopping/closed.
  m.schedule(false, 0, name, job, cb, onError,
    kind = skAt, targetMs = inMilliseconds(at.toTime - fromUnix(0)))

proc scheduleDaily*[T](m: TaskManager, hour, minute: int, second = 0,
    job: proc(): T {.closure.},
    cb: proc(res: T) {.closure.},
    onError: proc(err: ref CatchableError) {.closure.} = nil,
    name = ""): TimerId  {.discardable.} =
  ## Run `job` every day at local `hour:minute:second` (then like
  ## `submit` per fire). Returns the first timer id; each occurrence
  ## re-arms for the next day, recomputed from local `now()` so DST
  ## shifts land on one 23h/25h day. `cancelTask`/`removeTask` by name
  ## stop the chain; a stale id still reaches the live occurrence.
  ## Raises `CatchableError` on out-of-range times or stopping/closed.
  if hour < 0 or hour > 23 or minute < 0 or minute > 59 or
      second < 0 or second > 59:
    raise newException(CatchableError,
      "scheduleDaily: time out of range")
  m.schedule(false, 0, name, job, cb, onError,
    kind = skDaily, hour = hour, minute = minute, second = second)

proc scheduleWeekly*[T](m: TaskManager, weekday: WeekDay,
    hour, minute: int, second = 0,
    job: proc(): T {.closure.},
    cb: proc(res: T) {.closure.},
    onError: proc(err: ref CatchableError) {.closure.} = nil,
    name = ""): TimerId  {.discardable.} =
  ## Run `job` every week on `weekday` at local `hour:minute:second`
  ## (then like `submit` per fire). Same chaining, cancellation and
  ## error rules as `scheduleDaily`.
  if hour < 0 or hour > 23 or minute < 0 or minute > 59 or
      second < 0 or second > 59:
    raise newException(CatchableError,
      "scheduleWeekly: time out of range")
  m.schedule(false, 0, name, job, cb, onError,
    kind = skWeekly, weekdayOrd = ord(weekday),
    hour = hour, minute = minute, second = second)

proc statusOf(ctl: ptr CtlBlock, id: int): TaskStatus =
  ## Derive one tracked id's status. Caller holds `ctl.lock`; the
  ## payload outlives its entry (freed only by the fire path, which
  ## forgets first, or the `close` walk, after which `closed` bails
  ## out early), so the dead-flag read is safe.
  if not ctl.byId.hasKey(id):
    return taskUnknown
  let rec = ctl.byId[id]
  if not rec.armed:
    return taskInactive
  if isDead(rec.payload):
    return taskUnknown
  if rec.cancelled:
    return taskCancelled
  taskArmed

proc taskStatus*(m: TaskManager, id: TimerId): TaskStatus =
  ## Lifecycle state of one delayed/repeating/scheduled task by id.
  ## Thread-safe, including from inside callbacks.
  if m.closed.load:
    return taskUnknown
  let ctl = cast[ptr CtlBlock](m.ctl)
  withLock(ctl.lock):
    result = ctl.statusOf(int(id))

proc taskStatus*(m: TaskManager, name: string): TaskStatus =
  ## Lifecycle state of one named task. Unknown names are
  ## `taskUnknown`. Thread-safe, including from inside callbacks.
  if m.closed.load:
    return taskUnknown
  let ctl = cast[ptr CtlBlock](m.ctl)
  withLock(ctl.lock):
    if not ctl.byName.hasKey(name):
      result = taskUnknown
    else:
      result = ctl.statusOf(ctl.byName[name])

proc cancelTask*(m: TaskManager, id: TimerId) =
  ## Drop a delayed/repeating/scheduled timer by id. Unknown or
  ## already-fired ids are ignored; inactive ids are a no-op; a stale
  ## chained id still reaches the live occurrence. Takes effect at the
  ## next heartbeat (~10ms); a repeating timer may fire once more
  ## first (powpow drops timer nodes lazily). Thread-safe, including
  ## from inside callbacks.
  if m.closed.load:
    return
  let ctl = cast[ptr CtlBlock](m.ctl)
  var arg = cast[TimerArg](allocShared0(sizeof(TimerArgObj)))
  arg.id = id
  enqueueOp(ctl, doCancel, abortFreed, cast[pointer](arg))

proc cancelTask*(m: TaskManager, name: string) =
  ## Drop a named delayed/repeating/scheduled task. Unknown names are
  ## ignored; otherwise identical to `cancelTask` by id. The name
  ## resolves on the scheduler thread, so a chain re-arming between
  ## the call and the op still stops.
  if m.closed.load:
    return
  let ctl = cast[ptr CtlBlock](m.ctl)
  var arg = cast[NameArg](allocShared0(sizeof(NameArgObj)))
  arg.name = name
  enqueueOp(ctl, doCancelByName, abortNameArg, cast[pointer](arg))

proc cancel*(m: TaskManager, id: TimerId) =
  ## Alias of `cancelTask` by id.
  m.cancelTask(id)

proc removeTask*(m: TaskManager, id: TimerId) =
  ## Strict removal by id: drop the timer, flag the payload dead (a
  ## lazy spurious fire drops it silently instead of running the job)
  ## and free a taken name for reuse. Payload memory is reclaimed by
  ## the fire path or the `close` walk. Unknown ids are ignored.
  ## Takes effect at the next heartbeat (~10ms). Thread-safe,
  ## including from inside callbacks.
  if m.closed.load:
    return
  let ctl = cast[ptr CtlBlock](m.ctl)
  var arg = cast[TimerArg](allocShared0(sizeof(TimerArgObj)))
  arg.id = id
  enqueueOp(ctl, doRemove, abortFreed, cast[pointer](arg))

proc removeTask*(m: TaskManager, name: string) =
  ## Strict removal by name. Unknown names are ignored; otherwise
  ## identical to `removeTask` by id. Resolves on the scheduler
  ## thread, like `cancelTask` by name.
  if m.closed.load:
    return
  let ctl = cast[ptr CtlBlock](m.ctl)
  var arg = cast[NameArg](allocShared0(sizeof(NameArgObj)))
  arg.name = name
  enqueueOp(ctl, doRemoveByName, abortNameArg, cast[pointer](arg))

proc beginStop(m: TaskManager): bool =
  ## Shared `stop`/`shutdown` prologue: flip `stopping` once and post
  ## the loop stop. Returns false when stopping/closed (or already
  ## stopping) so callers can bail out idempotently.
  if m.closed.load:
    return false
  let ctl = cast[ptr CtlBlock](m.ctl)
  withLock(ctl.lock):
    if ctl.stopping:
      return false
    ctl.stopping = true
  enqueueOp(ctl, doStop, abortNoop, nil)
  true

proc stop*(m: TaskManager) =
  ## Graceful stop: reject new submissions, drop pending timers, let
  ## the pool drain queued jobs (every queued job still runs and
  ## delivers, cancelled immediates stay silent). Blocks until the
  ## pool is torn down. Idempotent. Must run outside pool
  ## jobs/callbacks (it joins pool threads).
  if m.beginStop:
    closeThreadPool(m.pool)
    let ctl = cast[ptr CtlBlock](m.ctl)
    ctl.reapJobs()

proc shutdown*(m: TaskManager) =
  ## Immediate stop: like `stop`, but jobs still queued (never
  ## started) are discarded — their callbacks never fire, and their
  ## job cells are reaped here. In-flight jobs finish and deliver.
  ## Blocks. Idempotent. Must run outside pool jobs/callbacks (it
  ## joins pool threads).
  if m.beginStop:
    shutdownThreadPool(m.pool)
    let ctl = cast[ptr CtlBlock](m.ctl)
    ctl.reapJobs()

proc halt*(m: TaskManager, delayMs: int): bool {.discardable.} =
  ## Gracefully `stop` the manager after `delayMs` milliseconds
  ## (plus one heartbeat for the op to land). Returns false when
  ## stopping/closed. Only enqueues and returns, so it is safe from
  ## inside a job/callback — the recommended way to stop from there.
  if m.closed.load:
    return false
  let ctl = cast[ptr CtlBlock](m.ctl)
  withLock(ctl.lock):
    if ctl.stopping:
      return false
  var arg = cast[HaltArg](allocShared0(sizeof(HaltArgObj)))
  arg.delayMs = delayMs
  enqueueOp(ctl, doHalt, abortFreed, cast[pointer](arg))
  true

proc close*(m: TaskManager) =
  ## `stop`, then join the scheduler thread, abort leftover ops
  ## (registration waiters wake with a closed error), reclaim armed
  ## timer payloads, and free the loop and control block. Idempotent.
  ## When called from inside a pool/scheduler callback the join and
  ## loop teardown are skipped (they would deadlock); call `close`
  ## again from the creating thread to finish.
  if m.closed.load:
    return
  m.stop()
  if m.isSchedulerThread:
    return
  joinThread(m.schedThread)
  let ctl = cast[ptr CtlBlock](m.ctl)
  # The scheduler thread is dead and the pool drained: nothing fires
  # anymore. Abort whatever never ran, then reclaim every payload
  # still tracked — all on the creating thread that produced the
  # counts. Table keys die here too (plain strings, acyclic).
  withLock(ctl.lock):
    for op in ctl.pending:
      op.abort(ctl, op.arg)
    ctl.pending.setLen(0)
    for id, rec in ctl.byId:
      rec.free(ctl, rec.payload)
    ctl.byId.clear()
    ctl.byName.clear()
    ctl.superseded.clear()
    for _, cell in ctl.jobs:
      deallocShared(cell)
    ctl.jobs.clear()
  m.sched.close()
  deinitCond(ctl.startedCond)
  deinitLock(ctl.lock)
  deallocShared(ctl)
  m.closed.store(true)

when isMainModule:
  import std/os
  var m = newTaskManager(poolSize = 2)
  assert m.isRunning
  assert m.submit(proc(): string = "hello", proc(res: string) =
    echo "immediate: ", res).isValid
  let once = m.submitDelayed(100, proc(): int = 40 + 2, proc(res: int) =
    echo "delayed: ", res)
  echo "scheduled one-shot: ", int(once)
  let every = m.submitRepeating(200, proc(): int = 1, proc(res: int) =
    echo "repeating tick")
  sleep(550)
  m.cancelTask(every)
  echo "cancelled repeating: ", int(every)
  let namedCb = proc(res: int) = echo "named fired (should not happen)"
  let named = m.submitDelayed(5_000, proc(): int = 7, namedCb,
    name = "cleanup")
  echo "named armed: ", m.hasTask("cleanup"), " id armed: ",
    m.hasTask(named), " id: ", int(named)
  m.removeTask("cleanup")
  sleep(100) # removal lands at the next heartbeat (~10ms)
  echo "named removed: ", not m.hasTask("cleanup")
  sleep(200)
  m.close()
  echo "closed, running=", m.isRunning
