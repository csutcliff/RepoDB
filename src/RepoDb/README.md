[![MSBuild-CI](https://github.com/AmpScm/RepoDb/actions/workflows/build.yml/badge.svg)](https://github.com/AmpScm/RepoDb/actions/workflows/build.yml)
[![Version](https://img.shields.io/nuget/v/AmpScm.RepoDb?&logo=nuget)](https://www.nuget.org/packages/AmpScm.RepoDb)
[![GitterChat](https://img.shields.io/gitter/room/mikependon/RepoDb?&logo=gitter&color=48B293)](https://gitter.im/RepoDb/community)

# RepoDB - a hybrid ORM library for .NET.

RepoDB is an open-source .NET ORM library that bridges the gaps of micro-ORMs and full-ORMs. It helps you simplify the switch-over of when to use the BASIC and ADVANCE operations during the development.

## Important Pages

- [GitHub Home Page](https://github.com/AmpScm/RepoDb) - to learn more about the core library and fork improvements.
- [Documentation](https://github.com/AmpScm/RepoDb/blob/main/docs/README.md) - comprehensive guides, features, and references in the repository.

## Core Features

- Batch Operations - Execute multiple operations in a single batch
- Bulk Operations - Perform high-performance bulk insert, update, delete, and merge operations
- Caching - Built-in query result caching to improve performance
- Class Handlers - Custom handlers for property value transformations
- Class Mapping - Map entity classes to database tables with full control
- Dynamics - Work with dynamic objects seamlessly
- Connection Persistency - Manage database connections efficiently
- Enumeration - Work with nullable enum types
- Expression Trees - Build complex queries using C# expressions
- Hints - Provide database-specific hints for query optimization
- Implicit Mapping - Automatic property to column mapping
- Multiple Query - Execute multiple queries efficiently
- Property Handlers - Custom handlers for individual property transformations
- Repositories - Generic repository pattern implementation
- Targeted Operations - Query and manipulate specific columns
- Tracing - Built-in tracing and debugging support
- Transaction - Full transaction support with isolation levels
- Type Mapping - Comprehensive data type mapping across databases

## Community Engagements

- [GitHub](https://github.com/AmpScm/RepoDb/issues) - for any issues, requests and problems.
- [StackOverflow](https://stackoverflow.com/search?q=RepoDB) - for any technical questions.
- [Gitter Chat](https://gitter.im/RepoDb/community) - for direct and live Q&A.

## License

[Apache-2.0](http://apache.org/licenses/LICENSE-2.0.html)
- Copyright © 2019 - 2024 [Michael Camara Pendon](https://github.com/mikependon)
- Copyright © 2025 - now [Bert Huijben](https://github.com/rhuijben)

--------

## Installation

At the Package Manager Console, write the command below.

```
Install-Package AmpScm.RepoDb
```

## Documentation

For detailed documentation, see:

- [Getting Started Guide](/docs/getstarted/getstarted.md)
- [Installation Guide](/docs/getstarted/installation.md)
- [Features Guide](/docs/features/features.md)
- [Operations Reference](/docs/operations/operations.md)
- [All Documentation](/docs)

## Database-Specific Packages

This is the core library. For database-specific implementations, install the appropriate extension package:

- **SQL Server**: `AmpScm.RepoDb.SqlServer`
- **PostgreSQL**: `AmpScm.RepoDb.PostgreSql`
- **MySQL**: `AmpScm.RepoDb.MySql` (MySql.Data) or `AmpScm.RepoDb.MySqlConnector` (MySqlConnector)
- **Oracle**: `AmpScm.RepoDb.Oracle`
- **SQLite**: `AmpScm.RepoDb.Sqlite.Microsoft` (Microsoft.Data.Sqlite) or `AmpScm.RepoDb.SQLite.System` (System.Data.SQLite)

For more details on getting started with a specific database, refer to the appropriate database guide in the [documentation](/docs/getstarted).
