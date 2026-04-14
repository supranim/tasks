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
- Schedule tasks to run at specific times or after certain delays
- Based on Libevent for efficient event-driven task management
- Supports both one-time and repeating tasks
- Thread-safe task submission and execution
- Simple API for submitting and managing tasks

>[!NOTE]
> This task manager can also be used in other Nim projects that need background task processing. Ensure you have Libevent installed and properly linked when using this package.


### Example usage

```nim
import std/[os, times]
import pkg/supranim_tasks

var manager = newTaskManager(tickMs = 10)

manager.submitTask(Task(
  id: "task1",
  work: proc() =
    echo "Hello from task1"
    while true:
      sleep(1000) # Simulate long-running task
))

manager.submitTask(
  initDuration(days = 2),
  Task(id: "once-after-2-days", work: proc() = echo "run once")
)

manager.submitRepeatingTask(
  every = initDuration(minutes = 5),
  task = Task(id: "repeat-5-minutes",
    work: proc() =
      echo "This runs every 5 minutes"
  )
)

manager.run()
manager.close()
```

### Roadmap
- [ ] Add support for task cancellation and rescheduling
- [ ] Implement task prioritization
- [ ] Add support for file logging of task execution
- [ ] Provide more detailed error handling and reporting for task failures

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/supranim/tasks/issues)
- 👋 Wanna help? [Fork it!](https://github.com/supranim/tasks/fork)
- 😎 [Get €20 in cloud credits from Hetzner](https://hetzner.cloud/?ref=Hm0mYGM9NxZ4)

### 🎩 License
MIT license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright &copy; 2025 OpenPeeps & Contributors &mdash; All rights reserved.