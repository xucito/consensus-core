# ConsensusCore
Consensus Algorithm based on Raft that allows for .NET Core applications to implement a clustered configuration.


# Differences from paper

RequestVote => VoteRequest

R1



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