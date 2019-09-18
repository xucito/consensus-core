# ConsensusCore
Consensus Algorithm based on Raft that allows for .NET Core applications to implement a clustered configuration.


# Differences from paper

RequestVote => VoteRequest

R1

## Snapshotting

The raft paper proposed to send snapshots from leader to followers in chunks however the implementation in this framework makes the following assumtions 
- Snapshots are always starting from 0 to x (x being the lastIncludedIndex) 
- The snapshot can be transferred via a single network transaction.



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
