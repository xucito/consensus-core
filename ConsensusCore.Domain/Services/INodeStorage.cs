using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Models;

namespace ConsensusCore.Domain.Services
{
    public interface INodeStorage<State> where State : BaseState, new()
    {
        State LastSnapshot { get; set; }
        void SetLastSnapshot(State snapshot, int lastIncludedIndex, int lastIncludedTerm);
        Guid Id { get; set; }
        int CurrentTerm { get; }
        int LastSnapshotIncludedIndex { get; set; }
        int LastSnapshotIncludedTerm { get; }
        SortedList<int, LogEntry> Logs { get; set; }
        string Name { get; set; }
        double Version { get; set; }
        Guid? VotedFor { get; set; }
        int AddCommands(BaseCommand[] commands, int term);
        void AddLog(LogEntry entry);
        void AddLogs(List<LogEntry> entries);
        void CreateSnapshot(int indexIncludedTo, int trailingLogCount);
        void DeleteLogsFromIndex(int fromIndex, int? toIndex = null);
        int GetLastLogIndex();
        int GetLastLogTerm();
        LogEntry GetLogAtIndex(int logIndex);
        List<LogEntry> GetLogRange(int startIndexToSendFrom, int endIndexToSendTo);
        int GetTotalLogCount();
        bool LogExists(int logIndex);
        void Save();
        Task<bool> SaveThread();
        void SetCurrentTerm(int newTerm);
        void SetVotedFor(Guid? id);
    }
}