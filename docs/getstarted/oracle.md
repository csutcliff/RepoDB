# Get Started for Oracle

---

RepoDB is a hybrid .NET ORM library for [Oracle](https://www.nuget.org/packages/AmpScm.RepoDb.Oracle) RDBMS. The project is hosted at [GitHub](https://github.com/AmpScm/RepoDb/tree/main/src/RepoDb.Oracle) and is licensed with [Apache 2.0](http://apache.org/licenses/LICENSE-2.0.html).

## Installation

The library can be installed via NuGet. In your Package Manager Console, type the command below.

```csharp
> Install-Package AmpScm.RepoDb.Oracle
```

Once installed, call the globalized setup method to initialize all the dependencies for [Oracle](https://www.nuget.org/packages/AmpScm.RepoDb.Oracle).

```csharp
using RepoDb;
using Oracle.ManagedDataAccess.Client;

GlobalConfiguration
    .Setup()
    .UseOracle();
```

## Create a Table

Let us say you have this table on your database.

```sql
CREATE TABLE Person
(
    Id NUMBER PRIMARY KEY,
    Name VARCHAR2(128) NOT NULL,
    Age NUMBER(3) NOT NULL,
    CreatedDateUtc TIMESTAMP NOT NULL
);

CREATE SEQUENCE Person_Seq
    START WITH 1
    INCREMENT BY 1
    NOCYCLE;
```

## Create a Model

And you have this model on your application.

```csharp
using RepoDb.Attributes;

public class Person
{
    [Primary]
    public long Id { get; set; }

    public string Name { get; set; }

    public int Age { get; set; }

    public DateTime CreatedDateUtc { get; set; }
}
```

## Creating a Record

To create a row, use the [Insert](/docs/operations/insert.md) method.

```csharp
var person = new Person
{
    Name = "John Doe",
    Age = 54,
    CreatedDateUtc = DateTime.UtcNow
};

using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var id = connection.Insert(person);
}
```

To insert multiple rows, use the [InsertAll](/docs/operations/insertall.md) operation.

```csharp
var people = GetPeople(100);

using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var rowsInserted = connection.InsertAll(people);
}
```

> **Note:** The [Insert](/docs/operations/insert.md) method returns the value of identity/primary column, while the [InsertAll](/docs/operations/insertall.md) method returns the number of rows inserted. Both methods are automatically setting back the value of the primary property to the entity model (if present).

## Querying a Record

To query a row, use the [Query](/docs/operations/query.md) method.

```csharp
using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var person = connection.Query<Person>(e => e.Id == 10045);
    /* Do the stuffs for person here */
}
```

To query all the rows, use the [QueryAll](/docs/operations/queryall.md) method.

```csharp
using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var people = connection.QueryAll<Person>();
    /* Do the stuffs for the people here */
}
```

## Merging a Record

To merge a row, use the [Merge](/docs/operations/merge.md) method.

```csharp
var person = new Person
{
    Id = 1,
    Name = "John Doe",
    Age = 57,
    CreatedDateUtc = DateTime.UtcNow
};

using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var id = connection.Merge(person);
}
```

By default, the primary column is used as a qualifier, but you can customize the qualifiers with other columns.

```csharp
var person = new Person
{
    Name = "John Doe",
    Age = 57,
    CreatedDateUtc = DateTime.UtcNow
};

using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var id = connection.Merge(person, qualifiers: (p => new { p.Name }));
}
```

To merge all the rows, use the [MergeAll](/docs/operations/mergeall.md) method.

```csharp
var people = GetPeople(100).AsList();
people
    .ForEach(p => p.Name = $"{p.Name} (Merged)");

using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var affectedRecords = connection.MergeAll<Person>(people);
}
```

> **Note:** The [Merge](/docs/operations/merge.md) method returns the value of the primary column while the [MergeAll](/docs/operations/mergeall.md) method returns the number of rows affected. Both methods are automatically setting back the value of the primary property to the entity model (if present).

## Deleting a Record

To delete a row, use the [Delete](/docs/operations/delete.md) method.

```csharp
using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var deletedRows = connection.Delete<Person>(10045);
}
```

By default, it uses the primary column as a qualifier, but you can also use other columns like below.

```csharp
using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var deletedRows = connection.Delete<Person>(p => p.Name == "John Doe");
}
```

To delete all the rows, use the [DeleteAll](/docs/operations/deleteall.md) method.

```csharp
using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var deletedRows = connection.DeleteAll<Person>();
}
```

You can also pass the list of primary keys or models to be deleted.

```csharp
using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var primaryKeys = new [] { 10045, 11001, ..., 12011 };
    var deletedRows = connection.DeleteAll<Person>(primaryKeys);
}
```

> **Note:** Both the [Delete](/docs/operations/delete.md) and [DeleteAll](/docs/operations/deleteall.md) methods return the number of rows affected during the deletion.

## Updating a Record

To update a row, use the [Update](/docs/operations/update.md) method.

```csharp
var person = new Person
{
    Id = 1,
    Name = "James Doe",
    Age = 55,
    CreatedDateUtc = DateTime.UtcNow
};

using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var updatedRows = connection.Update<Person>(person);
}
```

You can also dynamically update by targeting certain columns.

```csharp
using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var updatedRows = connection.Update("Person", new { Id = 1, Name = "James Doe" });
}
```

To update all the rows, use the [UpdateAll](/docs/operations/updateall.md) method.

```csharp
var people = GetPeople(100).AsList();
people
    .ForEach(p => p.Age = p.Age + 1);

using (var connection = new OracleConnection(ConnectionString))
{
    connection.Open();
    var affectedRecords = connection.UpdateAll<Person>(people);
}
```

> **Note:** Both the [Update](/docs/operations/update.md) and [UpdateAll](/docs/operations/updateall.md) methods return the number of rows affected during the update.

## Next Steps

- For more advanced features, visit the [Features](/docs/features/features.md) guide.
- Explore [Operations](/docs/operations/README.md) for more detailed method references.
- Check out [Attributes](/docs/attributes/attributes.md) for entity mapping customization.
