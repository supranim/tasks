# A simple task manager built on top of libevent
# for scheduling and executing tasks with optional delays, using an event loop.
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/supranim/tasks

import std/[deques, times, threadpool, options]
import pkg/libevent/bindings/event

## This library provides a simple task manager that allows you to schedule tasks to be executed
## immediately or after a specified delay. It uses libevent for efficient event handling and supports
## cooperative multitasking by allowing tasks to yield control back to the event loop, ensuring that
## long-running tasks do not block the execution of other tasks.

type
  TaskProc* = proc() {.gcsafe, nimcall.}
    ## A procedure type representing the work to be done for a task. This is a simple no-argument
    ## procedure that will be executed when the task runs

  Task* = object
    id*: string
      ## An optional identifier for the task, useful for debugging or tracking purposes.
    work*: TaskProc
      ## The procedure to execute for this task. The `TaskProc` will run in a threadpool
      ## to allow cooperative multitasking. Tasks should yield control back to the event loop if they

  ScheduledTask* = object
    dueAt: Time
      # The absolute time when the task is due to run. This is calculated as the current
      # time plus any specified delay.
    task: Task
      # The task to be executed.
    repeating: bool
    repeatEvery: Duration

  TaskManager* = ref object
    base*: ptr event_base
      ## The libevent base used for managing events and the event loop.
    tickEv: ptr event
      # The periodic timer event that checks for due tasks and executes pending tasks.
    tickTv: Timeval
      # The interval for the periodic tick event (e.g., 10ms).
    pending: Deque[Task]
      # A queue of tasks that are ready to run on the next tick. Tasks are moved
      # here from the scheduled list when they become due.
    scheduled: seq[ScheduledTask]
      # A sequence of tasks that are scheduled to run at specific times. Each entry
      # includes the absolute due time and the task to execute.
    stopping: bool
      # A flag indicating whether the manager is in the process of stopping. This
      # is used to prevent new tasks from being added and to signal the event loop
      # to exit

proc nowMs(): int64 =
  int64(epochTime() * 1000.0)

proc isPositiveDuration(d: Duration): bool =
  d.inMilliseconds > 0

proc runTask(taskProc: TaskProc) {.thread.} =
  taskProc()

proc beautifyDuration(d: Duration): string =
  if d.inSeconds < 60:
    result = "in " & $d.inSeconds & " seconds"
  elif d.inMinutes < 60:
    result = "in " & $d.inMinutes & " minutes"
  elif d.inHours < 24:
    result = "in " & $d.inHours & " hours"
  else:
    result = "in " & $d.inDays & " days"

proc onTick(fd: cint, events: cshort, arg: pointer) {.cdecl.} =
  # Libevent callback for the periodic tick event. This is where we check for due
  # scheduled tasks and execute pending tasks.
  let manager = cast[TaskManager](arg)
  if manager.stopping:
    return

  let rightNow = getTime()

  # Move due scheduled tasks to pending
  var i = manager.scheduled.high
  while i >= 0:
    if manager.scheduled[i].dueAt <= rightNow:
      let s = manager.scheduled[i]
      manager.pending.addLast(s.task)

      if s.repeating and isPositiveDuration(s.repeatEvery):
        # Advance next run; catch up if we missed multiple intervals.
        manager.scheduled[i].dueAt = manager.scheduled[i].dueAt + s.repeatEvery
        while manager.scheduled[i].dueAt <= rightNow:
          manager.scheduled[i].dueAt = manager.scheduled[i].dueAt + s.repeatEvery
      else:
        manager.scheduled.delete(i)
    dec i

  # Execute all pending tasks on the event loop thread
  while manager.pending.len > 0:
    spawn runTask(manager.pending.popFirst().work)

proc newTaskManager*(tickMs = 10): TaskManager =
  ## Initializes the TaskManager. This sets up the libevent base and a periodic timer event that
  ## checks for scheduled tasks and executes pending tasks. The tickMs parameter controls how often
  ## the manager checks for due tasks (default is 10ms).
  result = TaskManager()

  result.base = event_base_new()
  if result.base.isNil:
    raise newException(CatchableError, "event_base_new() failed")

  result.pending = initDeque[Task]()
  result.tickTv.tv_sec = clong(tickMs div 1000)
  result.tickTv.tv_usec = clong((tickMs mod 1000) * 1000)

  result.tickEv = event_new(
    result.base,
    -1,
    (EV_TIMEOUT or EV_PERSIST).cushort,
    onTick,
    cast[pointer](result)
  )
  if result.tickEv.isNil:
    event_base_free(result.base)
    result.base = nil
    raise newException(CatchableError, "event_new() failed")

  if event_add(result.tickEv, addr result.tickTv) != 0:
    event_free(result.tickEv)
    result.tickEv = nil
    event_base_free(result.base)
    result.base = nil
    raise newException(CatchableError, "event_add() failed")

proc submitTask*(manager: TaskManager, task: Task): bool =
  ## Submits a task for immediate execution on the next tick. Returns
  ## false if the manager is stopping or nil.
  if manager.isNil or manager.stopping:
    return false
  manager.pending.addLast(task)
  true

proc submitTask*(manager: TaskManager, delay: Duration, task: Task, repeatTask = false) =
  ## Schedules a one-shot task after `delay`. If `repeatTask` is true, the task will be
  ## rescheduled to run repeatedly at the specified interval.
  if manager.isNil or manager.stopping:
    return

  if not isPositiveDuration(delay):
    manager.pending.addLast(task)
    return
  
  let scheduleTask = ScheduledTask(
    dueAt: getTime() + delay,
    task: task,
    repeating: repeatTask,
    repeatEvery: initDuration()
  )

  manager.scheduled.add(scheduleTask)
  
  echo "Scheduled task '#", task.id, "' to run in ", beautifyDuration(delay)

proc submitNewTask*(manager: TaskManager, id: string, work: TaskProc): bool {.discardable.} =
  ## A convenience overload for submitting a new task without needing to create a Task object first.
  manager.submitTask(Task(id: id, work: work))

proc submitRepeatingTask*(manager: TaskManager, every: Duration, task: Task,
                startIn: Option[Duration] = none(Duration)): bool =
  ## Schedules a repeating task that runs at a specified interval. The first execution can be
  ## delayed by `startIn`, which defaults to the same as `every` if not provided or non-positive.
  ## - every: repeat interval (must be > 0)
  ## - startIn: optional first delay; if <= 0, uses `every`
  if manager.isNil or manager.stopping:
    return false

  let firstDelay =
    if startIn.isSome:
      startIn.get()
    else:
      every

  manager.scheduled.add(ScheduledTask(
    dueAt: getTime() + firstDelay,
    task: task,
    repeating: true,
    repeatEvery: every
  ))
  true

proc run*(manager: TaskManager) =
  ## Starts the event loop to process tasks. This will block until the
  ## manager is stopped. Each task run in a threadpool to allow cooperative multitasking.
  if manager.isNil or manager.base.isNil: return
  discard event_base_dispatch(manager.base)

proc stop*(manager: TaskManager) =
  ## Signals the manager to stop. This will cause the event loop to
  ## exit after the current tick completes.
  if manager.isNil or manager.stopping:
    return

  manager.stopping = true

  if not manager.tickEv.isNil:
    discard event_del(manager.tickEv)

  if not manager.base.isNil:
    discard event_base_loopbreak(manager.base)

proc close*(manager: var TaskManager) =
  ## Cleans up resources used by the TaskManager. This should be called
  ## after stopping the manager to free resources.
  if manager.isNil:
    return

  manager.stop()

  if not manager.tickEv.isNil:
    event_free(manager.tickEv)
    manager.tickEv = nil

  if not manager.base.isNil:
    event_base_free(manager.base)
    manager.base = nil

  manager.scheduled.setLen(0)
  manager.pending = initDeque[Task]()

proc halt*(manager: TaskManager, delay: Duration): bool =
  ## Schedules the manager to stop after a specified delay. This is a convenient way to
  ## automatically stop the manager after a certain amount of time has passed.
  ## Returns false if the manager is nil or already stopping.
  if manager.isNil or manager.base.isNil:
    return false

  var tv: Timeval
  tv.tv_sec = clong(delay.inMilliseconds div 1000)
  tv.tv_usec = clong((delay.inMilliseconds mod 1000) * 1000)
  result = event_base_loopexit(manager.base, addr tv) == 0

