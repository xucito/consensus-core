using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Domain.SystemCommands.ShardMetadata;
using ConsensusCore.Domain.SystemCommands.Tasks;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Services.Data;
using ConsensusCore.Node.Services.Raft;
using ConsensusCore.Node.SystemTasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services.Tasks
{
    public class TaskService<State> : ITaskService where State : BaseState, new()
    {
        private readonly ILogger _logger;
        private readonly ClusterClient _clusterClient;
        private readonly IStateMachine<State> _stateMachine;
        private readonly ClusterOptions _clusterOptions;
        private readonly NodeStateService _nodeStateService;
        private readonly IDataService _dataService;

        //Internal Componetns
        private readonly Monitor<State> _monitor;
        private ConcurrentDictionary<Guid, NodeTaskMetadata> _nodeTasks { get; set; } = new ConcurrentDictionary<Guid, NodeTaskMetadata>();

        //Background processes
        private readonly Task _scanTasks;

        public TaskService(
            ILoggerFactory loggerFactory,
            IStateMachine<State> stateMachine,
            NodeStateService nodeStateService,
            ClusterClient clusterClient,
            IOptions<ClusterOptions> clusterOptions,
            IDataService dataService)
        {
            _logger = loggerFactory.CreateLogger<TaskService<State>>();
            _stateMachine = stateMachine;
            _clusterOptions = clusterOptions.Value;
            _clusterClient = clusterClient;
            _nodeStateService = nodeStateService;
            _dataService = dataService;
            _monitor = new Monitor<State>(stateMachine, loggerFactory.CreateLogger<Monitor<State>>(), nodeStateService);
            _scanTasks = new Task(async () => await ScanTasks());
            _scanTasks.Start();
        }

        private async Task ScanTasks()
        {
            while (true)
            {
                if ((_nodeStateService.Role == NodeState.Follower || _nodeStateService.Role == NodeState.Leader) && _nodeStateService.IsBootstrapped && _nodeStateService.InCluster)
                {
                    _logger.LogDebug(_nodeStateService.GetNodeLogId() + "Starting task watch.");
                    //Check tasks assigned to this node
                    var tasks = _stateMachine.CurrentState.GetNodeClusterTasks(new ClusterTaskStatuses[] { ClusterTaskStatuses.Created }, _nodeStateService.Id).ToList();
                    var currentTasksNo = _nodeTasks.Where(t => !t.Value.Task.IsCompleted).Count();
                    var numberOfTasksToAssign = (tasks.Count() > (_clusterOptions.ConcurrentTasks - currentTasksNo)) ? (_clusterOptions.ConcurrentTasks - currentTasksNo) : tasks.Count();

                    _logger.LogDebug(_nodeStateService.GetNodeLogId() + numberOfTasksToAssign + "tasks to run. || " + currentTasksNo);
                    if (numberOfTasksToAssign > 0)
                    {
                        await _clusterClient.Send(new ExecuteCommands()
                        {
                            Commands = new List<BaseCommand>()
                                {
                                    new UpdateClusterTasks()
                                    {
                                        TasksToUpdate = tasks.GetRange(0, numberOfTasksToAssign).Select(t => new TaskUpdate(){
                                              Status = ClusterTaskStatuses.InProgress,
                                              CompletedOn = DateTime.UtcNow,
                                              TaskId = t.Id
                                        }).ToList()
                                    }
                                },
                            WaitForCommits = true
                        });

                        //Create a thread for each task
                        for (var i = 0; i < numberOfTasksToAssign; i++)
                        {
                            _logger.LogDebug(_nodeStateService.GetNodeLogId() + " is starting task " + tasks[i].ToString());
                            try
                            {
                                var newTask = StartNodeTask(tasks[i]);
                                _nodeTasks.TryAdd(tasks[i].Id
                                    , new NodeTaskMetadata()
                                    {
                                        Id = tasks[i].Id,
                                        Task = Task.Run(() => newTask)
                                    });
                            }
                            catch (Exception e)
                            {
                                _logger.LogCritical(_nodeStateService.GetNodeLogId() + "Failed to fail step " + tasks[i].Id + " gracefully.");
                            }
                        }
                    }
                }
                Thread.Sleep(1000);
            }
        }

        public async Task StartNodeTask(BaseTask task)
        {
            try
            {
                _logger.LogInformation(_nodeStateService.Id + "Starting task " + task.Id);
                switch (task)
                {
                    case ResyncShard t:
                        var result = await _dataService.Handle(new RequestShardSync()
                        {
                            Type = t.Type,
                            ShardId = t.ShardId
                        });

                        if (result.IsSuccessful)
                        {
                            await _clusterClient.Send(new ExecuteCommands()
                            {
                                Commands = new List<BaseCommand>()
                                {
                                    new UpdateShardMetadataAllocations(){
                                        ShardId = t.ShardId,
                                        Type = t.Type,
                                        InsyncAllocationsToAdd = new HashSet<Guid>(){ _nodeStateService.Id },
                                        StaleAllocationsToRemove = new HashSet<Guid>(){ _nodeStateService.Id }
                                    },
                                    new UpdateClusterTasks()
                                    {
                                        TasksToUpdate = new List<TaskUpdate>()
                                        {
                                            new TaskUpdate()
                                            {
                                                Status = ClusterTaskStatuses.Successful,
                                                CompletedOn = DateTime.UtcNow,
                                            TaskId = task.Id
                                            }
                                        }
                                    }
                                },
                                WaitForCommits = true
                            });
                        }
                        break;
                }

                _logger.LogInformation("Completing task " + task.Id);
            }
            catch (Exception e)
            {
                _logger.LogError(_nodeStateService.GetNodeLogId() + "Failed to complete task " + task.Id + " with error " + e.Message + Environment.NewLine + e.StackTrace);
                await _clusterClient.Send(new ExecuteCommands()
                {
                    Commands = new List<BaseCommand>()
                            {
                                new UpdateClusterTasks()
                                {
                                    TasksToUpdate = new List<TaskUpdate>()
                                    {
                                        new TaskUpdate()
                                        {
                                            Status = ClusterTaskStatuses.Error,
                                            CompletedOn = DateTime.UtcNow,
                                            TaskId = task.Id,
                                            ErrorMessage = e.Message + ": " +  e.StackTrace

                                        }
                                    }
                                }
                            },
                    WaitForCommits = true
                });
            }
        }
    }
}
