
# BulkImportPseudoTableType

---

This enum is being used to define the type of pseudo-temporary table to be created during the bulk-import operations. This enumeration is only used for [PostgreSQL](https://www.nuget.org/packages/AmpScm.RepoDb.PostgreSql.BulkOperations) RDBMS.

It is used by the following bulk import operations.

- [BinaryBulkDelete](/docs/operations/binarybulkdelete.md)
- [BinaryBulkDeleteByKey](/docs/operations/binarybulkdeletebykey.md)
- [BinaryBulkInsert](/docs/operations/binarybulkinsert.md)
- [BinaryBulkMerge](/docs/operations/binarybulkmerge.md)
- [BinaryBulkUpdate](/docs/operations/binarybulkupdate.md)

## Enum Values

Below is the list of enum values.

| Name | Description |
|:-----|:------------|
| Temporary | A temporary pseudo-table will be created. The table is dedicated to the session of the connection and is automatically being destroyed once the connection is closed/disposed. Use this if you are working within an asynchronous environment.
| Physical | A physical pseudo-table will be created. The table is shared to any other connections. Use this if you prefer performance and is not working within an asynchronous environment. |

## Usability

Simply pass value on the `pseudoTableType` argument.

Below is for [BinaryBulkDelete](/docs/operations/binarybulkdelete.md) operation.

```csharp
using (var connection = new NpgsqlConnection(connectionString))
{
    var people = GetPeople(1000);
    var deletedRows = connection.BinaryBulkDelete(people,
        pseudoTableType: BulkImportPseudoTableType.Physical);
}
```

Below is for [BinaryBulkDeleteByKey](/docs/operations/binarybulkdeletebykey.md) operation.

```csharp
using (var connection = new NpgsqlConnection(connectionString))
{
    var primaryKeys = GetPeople(1000).Select(e => e.Id);
    var deletedRows = connection.BinaryBulkDeleteByKey(primaryKeys,
        pseudoTableType: BulkImportPseudoTableType.Physical);
}
```

Below is for [BinaryBulkInsert](/docs/operations/binarybulkinsert.md) operation.

```csharp
using (var connection = new NpgsqlConnection(connectionString))
{
    var people = GetPeople(1000);
    var insertedRows = connection.BinaryBulkInsert(people,
        pseudoTableType: BulkImportPseudoTableType.Physical);
}
```

Below is for [BinaryBulkMerge](/docs/operations/binarybulkmerge.md) operation.

```csharp
using (var connection = new NpgsqlConnection(connectionString))
{
    var people = GetPeople(1000);
    var mergedRows = connection.BinaryBulkMerge(people,
        pseudoTableType: BulkImportPseudoTableType.Physical);
}
```

Below is for [BinaryBulkUpdate](/docs/operations/binarybulkupdate.md) operation.

```csharp
using (var connection = new NpgsqlConnection(connectionString))
{
    var people = GetPeople(1000);
    var updatedRows = connection.BinaryBulkUpdate(people,
        pseudoTableType: BulkImportPseudoTableType.Physical);
}
```
> By default, the `Temporary` is used and it is thread-safe in nature. The pseudo-temporary table that is being created is localized to the instance of the connection.