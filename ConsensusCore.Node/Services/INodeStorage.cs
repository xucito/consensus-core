using System;
using System.Collections.Generic;
using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Models;

namespace ConsensusCore.Node.Services
{
    public interface INodeStorage
    {
        int CurrentTerm { get; }
        Guid Id { get; }
        List<LogEntry> Logs { get; }
        string Name { get; }
        double Version { get; }
        Guid? VotedFor { get; }

        int AddLog(List<BaseCommand> commands, int term);
        int GetLastLogTerm();
        LogEntry GetLogAtIndex(int logIndex);
        int GetLogCount();
        void DeleteLogsFromIndex(int index);
    }
}