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
        public readonly object _locker = new object();

        public int GetLogCount()
        {
            return Logs.Count();
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
            if(logIndex == 0 || Logs.Count() < logIndex)
            {
                return null;
            }
            var log = Logs[logIndex - 1];
            if (log.Index == logIndex)
            {
                return log;
            }
            else
            {
                return Logs.Where(l => l.Index == logIndex).FirstOrDefault();
            }
        }

        public int AddLog(List<TCommand> commands, int term)
        {
            int index;
            lock (_locker)
            {
                index = Logs.Count() + 1;
                Logs.Add(new LogEntry<TCommand>()
                {
                    Commands = commands,
                    Term = term,
                    Index = index
                });
            }
            return index;
        }

        public void UpdateCurrentTerm(int newterm)
        {
            CurrentTerm = newterm;
        }

        public void SetVotedFor(Guid candidateId)
        {
            VotedFor = candidateId;
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

        public new int AddLog(List<TCommand> commands, int term)
        {
            int index;
            lock (_locker)
            {
                index = Logs.Count() + 1;
                Logs.Add(new LogEntry<TCommand>()
                {
                    Commands = commands,
                    Term = term,
                    Index = index
                });
            }
            _repository.SaveNodeData();
            return index;
        }

        public new void UpdateCurrentTerm(int newterm)
        {
            CurrentTerm = newterm;
            _repository.SaveNodeData();
        }

        public new void SetVotedFor(Guid candidateId)
        {
            VotedFor = candidateId;
            _repository.SaveNodeData();
        }
    }
}
