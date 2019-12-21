# ConsensusCore
Consensus Algorithm based on Raft that allows for .NET Core applications to implement a clustered configuration.

## Design
### Redirection
- All nodes should be able to respond to requests from clients
- Follower nodes will forward requests to the leader
- Candidate nodes will cache requests until a timeout is reached or the node becomes a leader or follower


# Differences from paper

RequestVote => VoteRequest

R1

## Snapshotting

The raft paper proposed to send snapshots from leader to followers in chunks however the implementation in this framework makes the following assumtions 
- Snapshots are always starting from 0 to x (x being the lastIncludedIndex) 
- The snapshot can be transferred via a single network transaction.

Two Cluster options relate to the snapshot trailing
- SnapshottingInterval: How frequently to create snapshots in terms of commited logs i.e. 10 = every 10 commits it will create a snapshot
- SnapshottingTrailingLogCount: How many recent logs to exclude from the snapshot, this is used so that you do not create snapshots (and also delete) the more recent logs that may be in transient transactions. Generally the higher this number the lower likelihood of any race conditions occuring i.e. After evaluating that an AppendEntryRPC is to be sent from the leader, the logs are removed from the index.

Example: if Snapshotting interval is set to 50 and snapshottingtraillogcount is set to 10, every 50 commits, it will create a snapshot up to x (x being the commit index) - 10

# Add Extensions

```csharp
services.AddConsensusCore<CindiClusterState, NodeInMemoryRepository>();
```

Implement your state

```csharp
public class CindiClusterState : BaseState
{
	public override void ApplyCommandToState(BaseCommand command)
	{
		throw new NotImplementedException();
	}
}
```

## Design
### Redirection
- All nodes should be able to respond to requests from clients
- Follower nodes will forward requests to the leader
- Candidate nodes will cache requests until a timeout is reached or the node becomes a leader or follower


## Metrics
- Metrics are pushed by the Event handler. 
- If you write back the metric back to the cluster make sure you set Metric = false in the WriteData request to prevent circular record generation