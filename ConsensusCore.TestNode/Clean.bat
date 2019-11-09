RMDIR C:\Users\TNguy\.nuget\packages\consensuscore.domain /S /Q
RMDIR C:\Users\TNguy\.nuget\packages\consensuscore.node /S /Q

CD C:\Users\TNguy\OneDrive\Documents\Repositories\consensus-core\ConsensusCore.Domain
dotnet clean
dotnet pack

COPY /Y C:\Users\TNguy\OneDrive\Documents\Repositories\consensus-core\ConsensusCore.Domain\bin\Debug C:\Users\TNguy\OneDrive\Documents\LocalNuget

CD C:\Users\TNguy\OneDrive\Documents\Repositories\consensus-core\ConsensusCore.Node
dotnet clean
dotnet pack
COPY /Y C:\Users\TNguy\OneDrive\Documents\Repositories\consensus-core\ConsensusCore.Node\bin\Debug C:\Users\TNguy\OneDrive\Documents\LocalNuget

PAUSE