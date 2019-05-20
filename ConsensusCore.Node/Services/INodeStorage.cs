using System;
using System.Collections.Generic;
using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Models;

namespace ConsensusCore.Node.Services
{
    public interface INodeStorage<TCommand> where TCommand : BaseCommand
    {
        int CurrentTerm { get; }
        Guid Id { get; }
        List<LogEntry<TCommand>> Logs { get; }
        string Name { get; }
        double Version { get; }
        Guid? VotedFor { get; }

        int AddLog(List<TCommand> commands, int term);
        int GetLastLogTerm();
        LogEntry<TCommand> GetLogAtIndex(int logIndex);
        int GetLogCount();
    }

    public interface INodeStorage<TCommand, TRepository> 
        where TCommand : BaseCommand
         where TRepository : BaseRepository<TCommand>
    {
        int CurrentTerm { get; }
        Guid Id { get; }
        List<LogEntry<TCommand>> Logs { get; }
        string Name { get; }
        double Version { get; }
        Guid? VotedFor { get; }

        int AddLog(List<TCommand> commands, int term);
        int GetLastLogTerm();
        LogEntry<TCommand> GetLogAtIndex(int logIndex);
        int GetLogCount();
    }
}