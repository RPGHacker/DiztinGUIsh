using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Diz.Core.util
{
    public class WorkerTaskManager : IWorkerTaskManager
    {
        private readonly List<Task> tasks = [];
        private readonly object taskLock = new();
        private readonly ManualResetEvent notFinishing = new(false);
        
        private volatile bool finished;
        private Timer timer;
        private readonly object timerLock = new();

        private TaskManagerState state = TaskManagerState.NotStarted;
        private TaskManagerResult result = TaskManagerResult.Succeeded;
        private object? resultContext = null;

        public void Start()
        {
            this.state = TaskManagerState.NotStarted;
            var oneSecond = TimeSpan.FromSeconds(1);
            timer = new Timer(_ => Update(), null, oneSecond, oneSecond);
        }

        public void StartFinishing(TaskManagerResult result, object? resultContext = null)
        {
            this.state = TaskManagerState.Finished;
            this.result = result;
            this.resultContext = resultContext;
            notFinishing.Set();
        }
        public TaskManagerState GetState()
        {
            return state;
        }

        public TaskManagerResult GetResult()
        {
            return result;
        }

        public object? GetResultContext()
        {
            return resultContext;
        }

        private void Update()
        {
            lock (timerLock)
            {
                if (timer == null)
                    return;

                if (finished)
                {
                    timer.Change(-1, -1);
                    timer.Dispose();
                    timer = null;
                }

                lock (taskLock)
                {
                    Debug.Assert(!tasks.Contains(null));
                    tasks.RemoveAll(task => task.IsCompleted);
                    Debug.Assert(!tasks.Contains(null));
                }
            }
        }

        public Task Run(Action action, CancellationToken cancelToken)
        {
            return StartTask(new Task(action, cancelToken));
        }
        
        public Task Run(Action action)
        {
            return StartTask(new Task(action));
        }

        private Task StartTask(Task task)
        {
            Debug.Assert(task != null);

            lock (taskLock)
            {
                tasks.Add(task);
                Debug.Assert(!tasks.Contains(null));
            }

            task.Start();

            return task;
        }

        // blocking method
        public void WaitForAllTasksToComplete()
        {
            // wait until we've been told we should be finishing
            notFinishing.WaitOne();

            while (true)
            {
                List<Task> tasksCopy;
                lock (taskLock)
                {
                    // Remove completed tasks to prevent deadlock.
                    while (tasks.Count > 0 && tasks[^1].Status == TaskStatus.RanToCompletion)
                        tasks.RemoveAt(tasks.Count - 1);

                    if (tasks.Count == 0)
                        break;

                    Debug.Assert(!tasks.Contains(null));
                    tasksCopy = new List<Task>(tasks);
                    Debug.Assert(!tasksCopy.Contains(null));
                }

                // anything in our original list, let it run to completion
                Task.WaitAll(tasksCopy.ToArray());
            }

            finished = true;
        }
    }

    // reference implementation that runs synchronously. mostly for benchmarking/etc.
    public class WorkerTaskManagerSynchronous : IWorkerTaskManager
    {
        private TaskManagerState state = TaskManagerState.NotStarted;
        private TaskManagerResult result = TaskManagerResult.Succeeded;
        private object? resultContext = null;

        public void Start()
        {
            state = TaskManagerState.Running;
        }

        public Task Run(Action action, CancellationToken cancelToken)
        {
            var syncTask = new Task(action);
            syncTask.RunSynchronously();
            return syncTask;
        }

        public Task Run(Action action)
        {
            return Run(action, new CancellationToken());
        }

        public void WaitForAllTasksToComplete() { }
        public void StartFinishing(TaskManagerResult result, object? resultContext = null)
        {
            this.state = TaskManagerState.Finished;
            this.result = result;
            this.resultContext = resultContext;
        }

        // With all these getters, it might be better to turn this whole interface
        // into an abstract class instead...
        public TaskManagerState GetState()
        {
            return state;
        }

        public TaskManagerResult GetResult()
        {
            return result;
        }

        public object? GetResultContext()
        {
            return resultContext;
        }

    }

    public enum TaskManagerState
    {
        NotStarted,
        Running,
        Finished,
    }

    public enum TaskManagerResult
    {
        Failed,
        Cancelled,
        Succeeded,
    }

    public interface IWorkerTaskManager
    {
        void Start();
        
        void WaitForAllTasksToComplete();
        
        Task Run(Action action, CancellationToken cancelToken);
        Task Run(Action action);

        void StartFinishing(TaskManagerResult result, object? resultContext = null);

        // With all these getters, it might be better to turn this whole interface
        // into an abstract class instead...
        TaskManagerState GetState();
        TaskManagerResult GetResult();
        object? GetResultContext();
    }
}