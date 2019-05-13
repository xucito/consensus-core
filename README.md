# ConsensusCore
Consensus Algorithm based on Raft that allows for .NET Core applications to implement a clustered configuration.

## Design
### Redirection
- All nodes should be able to respond to requests from clients
- Follower nodes will forward requests to the leader
- Candidate nodes will cache requests until a timeout is reached or the node becomes a leader or follower
