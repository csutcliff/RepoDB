# Installation

The packages can be installed using the Package Manager Console window.

## Raw SQLs

If you wish to work only with raw-SQLs.

```csharp
> Install-Package AmpScm.RepoDb
```

> It supports all kinds of RDBMS data providers.

## SQL Server

If you wish to work with [SQL Server](https://www.nuget.org/packages/AmpScm.RepoDb.SqlServer).

```csharp
> Install-Package AmpScm.RepoDb.SqlServer
```

Once installed, call the globalized setup method to initialize all the dependencies for [SQL Server](https://www.nuget.org/packages/AmpScm.RepoDb.SqlServer).

```csharp
GlobalConfiguration
	.Setup()
	.UseSqlServer();
```

For the users prior the version 1.13.0, use the bootstrapper code below.

```csharp
RepoDb.SqlServerBootstrap.Initialize();
```

Or, if you are to work with the bulk operations.

```csharp
> Install-Package AmpScm.RepoDb.SqlServer.BulkOperations
```

#### System.Data.SqlClient

If you are working with this package, you are required to bootstrap the connection object on the startup.

```csharp
var dbSetting = new SqlServerDbSetting();

DbSettingMapper
	.Add<System.Data.SqlClient.SqlConnection>(dbSetting, true);
DbHelperMapper
	.Add<System.Data.SqlClient.SqlConnection>(new SqlServerDbHelper(), true);
StatementBuilderMapper
	.Add<System.Data.SqlClient.SqlConnection>(new SqlServerStatementBuilder(dbSetting), true);
```

Or, you can replicate the actual [SqlServerBootstrap](/src/RepoDb.SqlServer/RepoDb.SqlServer/SqlServerBootstrap.cs) class implementation and attach it to your solution. Then, call the local class initializer method explicitly.

## PostgreSQL

If you wish to work with [PostgreSQL](https://www.nuget.org/packages/AmpScm.RepoDb.PostgreSql).

```csharp
> Install-Package AmpScm.RepoDb.PostgreSql
```

Once installed, call the globalized setup method to initialize all the dependencies for PostgreSql.

```csharp
GlobalConfiguration
	.Setup()
	.UsePostgreSql();
```

For the users prior the version 1.13.0, use the bootstrapper code below.

```csharp
RepoDb.PostgreSqlBootstrap.Initialize();
```

Or, if you are to work with the bulk operations.

```csharp
> Install-Package AmpScm.RepoDb.PostgreSql.BulkOperations
```

## MySQL

There are 2 packages available for MySQL.

#### MySql.Data

If you wish to work with [RepoDb.MySql](https://www.nuget.org/packages/AmpScm.RepoDb.MySql).

```csharp
> Install-Package AmpScm.RepoDb.MySql
```

Once installed, call the globalized setup method to initialize all the dependencies for MySQL.

```csharp
GlobalConfiguration
	.Setup()
	.UseMySql();
```

#### MySqlConnector

If you wish to work with [RepoDb.MySqlConnector](https://www.nuget.org/packages/AmpScm.RepoDb.MySqlConnector).

```csharp
> Install-Package AmpScm.RepoDb.MySqlConnector;
```

Once installed, call the globalized setup method to initialize all the dependencies for MySQL.

```csharp
GlobalConfiguration
	.Setup()
	.UseMySqlConector();
```

## SQLite

There are 2 packages available for SQLite.

#### System.Data.SQLite.Core

If you wish to work with [RepoDb.SQLite.System](https://www.nuget.org/packages/AmpScm.RepoDb.SQLite.System).

```csharp
> Install-Package AmpScm.RepoDb.SQLite.System
```

Once installed, call the globalized setup method to initialize all the dependencies for SQLite.

```csharp
GlobalConfiguration
	.Setup()
	.UseSQLite();
```

#### Microsoft.Data.Sqlite

If you wish to work with [RepoDb.Sqlite.Microsoft](https://www.nuget.org/packages/AmpScm.RepoDb.Sqlite.Microsoft).

```csharp
> Install-Package AmpScm.RepoDb.Sqlite.Microsoft
```

Once installed, call the globalized setup method to initialize all the dependencies for SQLite.

```csharp
GlobalConfiguration
	.Setup()
	.UseSqlite();
```

> Please visit our [documentation](/docs) page to learn more about this library.
