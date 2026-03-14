[![MSBuild-CI](https://github.com/AmpScm/RepoDb/actions/workflows/build.yml/badge.svg)](https://github.com/AmpScm/RepoDb/actions/workflows/build.yml)
[![Version](https://img.shields.io/nuget/v/AmpScm.RepoDb.Oracle?&logo=nuget)](https://www.nuget.org/packages/AmpScm.RepoDb.Oracle)
[![GitterChat](https://img.shields.io/gitter/room/mikependon/RepoDb?&logo=gitter&color=48B293)](https://gitter.im/RepoDb/community)

# RepoDb.Oracle - a hybrid .NET ORM library for Oracle.

RepoDB is an open-source .NET ORM library that bridges the gaps of micro-ORMs and full-ORMs. It helps you simplify the switch-over of when to use the BASIC and ADVANCE operations during the development.

## Important Pages

- [GitHub Home Page](https://github.com/AmpScm/RepoDb) - to learn more about the core library.
- [Oracle Guide](/docs/getstarted/oracle.md) - getting started with Oracle
- [All Documentation](/docs) - comprehensive guides and references

## Community Engagements

- [GitHub](https://github.com/AmpScm/RepoDb/issues) - for any issues, requests and problems.
- [StackOverflow](https://stackoverflow.com/search?q=RepoDB) - for any technical questions.
- [Gitter Chat](https://gitter.im/RepoDb/community) - for direct and live Q&A.

## Dependencies

- [Oracle.ManagedDataAccess.Core](https://www.nuget.org/packages/Oracle.ManagedDataAccess.Core/) - the data provider used for Oracle.
- [RepoDb](https://www.nuget.org/packages/AmpScm.RepoDb/) - the core library of RepoDB.

## License

[Apache-2.0](http://apache.org/licenses/LICENSE-2.0.html)
- Copyright © 2025 - now [Bert Huijben](https://github.com/rhuijben)


--------

## Installation

At the Package Manager Console, write the command below.

```csharp
> Install-Package AmpScm.RepoDb.Oracle
```

See the [documentation](/docs) for more information.

## Get Started

First, RepoDB must be configured and Oracle support loaded.

```csharp
using RepoDb;
using Oracle.ManagedDataAccess.Client;

GlobalConfiguration.Setup().UseOracle();
```

**Note:** This call must be done once during application startup.

After the bootstrap initialization, any library operation can then be called.

### Query

```csharp
using (var connection = new OracleConnection(ConnectionString))
{
	var customer = connection.Query<Customer>(c => c.Id == 10045);
}
```

### Insert

```csharp
var customer = new Customer
{
	FirstName = "John",
	LastName = "Doe",
	IsActive = true
};

using (var connection = new OracleConnection(ConnectionString))
{
	var id = connection.Insert<Customer>(customer);
}
```

### Update

```csharp
using (var connection = new OracleConnection(ConnectionString))
{
	var customer = connection.Query<Customer>(10045);
	customer.FirstName = "John";
	customer.LastUpdatedUtc = DateTime.UtcNow;
	var affectedRows = connection.Update<Customer>(customer);
}
```

### Delete

```csharp
using (var connection = new OracleConnection(ConnectionString))
{
	var customer = connection.Query<Customer>(10045);
	var deletedCount = connection.Delete<Customer>(customer);
}
```
