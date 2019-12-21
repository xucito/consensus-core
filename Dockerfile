FROM mcr.microsoft.com/dotnet/core/sdk:2.2 AS build-env
WORKDIR /app

COPY ./ConsensusCore.TestNode/ConsensusCore.TestNode.csproj ./
COPY ./ConsensusCore.TestNode/. ./
COPY ./ConsensusCore.Node/. /ConsensusCore.Node/
COPY ./ConsensusCore.Domain/. /ConsensusCore.Domain/

RUN dotnet restore
RUN dotnet build ConsensusCore.TestNode.csproj

RUN dotnet publish -c Release -o out ConsensusCore.TestNode.csproj

FROM base AS final
FROM mcr.microsoft.com/dotnet/core/aspnet:2.2

COPY --from=build /app/out .

ENTRYPOINT ["dotnet", "ConsensusCore.TestNode.dll"]