
# DataEntityDataReader

---

This class is used to convert the [IEnumerable<T>](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1?view=net-7.0) object into a [DbDataReader](https://learn.microsoft.com/en-us/dotnet/api/system.data.common.dbdatareader?view=net-6.0) object.

Let us say you have a method named `GetPeople()` that creates an enumerable of `Person` model.

```csharp
private void IEnumerable<Person> GetPeople(int count = 10000)
{
    for (var i = 0; i < count; i++)
    {
        yield return new Person
        {
            Name = $"Person-{i}",
            SSN = Guid.NewGuid.ToString(),
            DateInsertedUtc = DateTime.UtcNow
        };
    }
}
```

Then, you can extract it to be a [DbDataReader](https://learn.microsoft.com/en-us/dotnet/api/system.data.common.dbdatareader?view=net-6.0) object via this class.

```csharp
var people = GetPeople();
using (var reader = new DataEntityDataReader<Person>(people))
{
    // Do the stuffs here
}
```

Then, you can use it like a normal data reader.

```csharp
while (reader.Read())
{
    // Extract the properties here
}
```

Or use it in the bulk operations.

```csharp
using (var connection = new SqlConnection(connectionString))
{
    var people = GetPeople();
    using (var reader = new DataEntityDataReader<Person>(people))
    {
        var insertedRows = connection.BulkInsert("[dbo].[Person]", reader);
    }
}
```
> This class is useful if you are tying to use the bulk operations (i.e.: [BulkDelete](/docs/operations/bulkdelete.md), [BulkInsert](/docs/operations/bulkinsert.md), [BulkMerge](/docs/operations/bulkmerge.md) and [BulkUpdate](/docs/operations/bulkupdate.md)) with [DbDataReader](https://learn.microsoft.com/en-us/dotnet/api/system.data.common.dbdatareader?view=net-6.0) object sourced by [IEnumerable<T>](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1?view=net-7.0).
