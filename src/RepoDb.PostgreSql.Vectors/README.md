[![MSBuild-CI](https://github.com/AmpScm/RepoDB/actions/workflows/build.yml/badge.svg)](https://github.com/AmpScm/RepoDB/actions/workflows/build.yml)
[![Version](https://img.shields.io/nuget/v/AmpScm.RepoDb.PostgreSql.Vectors?&logo=nuget)](https://www.nuget.org/packages/AmpScm.RepoDb.PostgreSql.Vectors)
[![GitterChat](https://img.shields.io/gitter/room/mikependon/RepoDb?&logo=gitter&color=48B293)](https://gitter.im/RepoDb/community)

# RepoDb.PostgreSql.Vectors

An extension library that extends RepoDB's PostgreSQL support with pgvector's Vector integration.

> **Note**: pgvector documentation is not yet included in this repository. See the [PostgreSQL Guide](/docs/getstarted/postgresql.md) for general PostgreSQL usage.

## Installation

At the Package Manager Console, write the command below.

```csharp
> Install-Package AmpScm.RepoDb.PostgreSql.Vectors
```

## Setup

First, ensure RepoDB and PostgreSQL support are configured.

```csharp
using RepoDb;
using Npgsql;

GlobalConfiguration.Setup().UsePostgreSql().UsePostgreSqlVectors();;
```

The pgvector extension is automatically recognized in PostgreSQL after calling UsePostgreSqlVectors().

Make sure to have the pgvector extension installed and enabled in your PostgreSQL database for the vector functionalities to work.

## Community Engagements

- [GitHub](https://github.com/AmpScm/RepoDb/issues) - for any issues, requests and problems.
- [StackOverflow](https://stackoverflow.com/search?q=RepoDB) - for any technical questions.
- [Gitter Chat](https://gitter.im/RepoDb/community) - for direct and live Q&A.

## License

[Apache-2.0](http://apache.org/licenses/LICENSE-2.0.html)
- Copyright © 2019 - 2024 [Michael Camara Pendon](https://github.com/mikependon)
- Copyright © 2025 - now [Bert Huijben](https://github.com/rhuijben)
