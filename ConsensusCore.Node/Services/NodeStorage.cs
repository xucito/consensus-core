using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConsensusCore.Node.Services
{
    public class NodeStorage<TCommand> : INodeStorage<TCommand>
        where TCommand : BaseCommand
    {
        public Guid Id { get; set; } = Guid.NewGuid();
        public string Name { get; set; }
        public double Version { get; set; } = 1.0;
        public int CurrentTerm { get; set; } = 0;
        public Guid? VotedFor { get; set; } = null;
        public List<LogEntry<TCommand>> Logs { get; set; } = new List<LogEntry<TCommand>>();

        public int GetLogCount()
        {
            return Logs.Count() + 1;
        }

        public int GetLastLogTerm()
        {
            var lastLog = Logs.LastOrDefault();
            if (lastLog != null)
            {
                return lastLog.Term;
            }
            return 0;
        }

        public int GetLastLogIndex()
        {
            var lastLog = Logs.LastOrDefault();
            if (lastLog != null)
            {
                return lastLog.Index;
            }
            return 0;
        }

        public LogEntry<TCommand> GetLogAtIndex(int logIndex)
        {
            return Logs.Where(l => l.Index == logIndex).FirstOrDefault();
        }

        public void AddLog(LogEntry<TCommand> log)
        {
            if (Logs.Where(l => l.Index == log.Index && l.Term == log.Term).Count() == 0)
            {
                Logs.Add(log);
            }
        }

        public void AddLogs(List<LogEntry<TCommand>> logs)
        {
            foreach (var log in logs)
            {
                if (Logs.Where(l => l.Index == log.Index && l.Term == log.Term).Count() == 0)
                {
                    Logs.Add(log);
                }
            }
        }
    }

    public class NodeStorage<TCommand, TRepository> : NodeStorage<TCommand>, INodeStorage<TCommand, TRepository>
        where TCommand : BaseCommand
        where TRepository : BaseRepository<TCommand>
    {
        private TRepository _repository;

        public NodeStorage(TRepository repository)
        {
            _repository = repository;
            var storedData = _repository.LoadNodeData();
            Id = storedData.Id;
            Name = storedData.Name;
            Version = storedData.Version;
            CurrentTerm = storedData.CurrentTerm;
            VotedFor = storedData.VotedFor;
        }

        public new void AddLogs(List<LogEntry<TCommand>> logs)
        {
            foreach (var log in logs)
            {
                if (Logs.Where(l => l.Index == log.Index && l.Term == log.Term).Count() == 0)
                {
                    Logs.Add(log);
                }
            }
            _repository.SaveNodeData();
        }

        public new void AddLog(LogEntry<TCommand> log)
        {
            if (Logs.Where(l => l.Index == log.Index && l.Term == log.Term).Count() == 0)
            {
                Logs.Add(log);
            }
            _repository.SaveNodeData();
        }
    }
}
