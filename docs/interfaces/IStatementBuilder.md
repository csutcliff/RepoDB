
# IStatementBuilder

---

This interface is used to mark a class to be a statement builder object. I provides necessary methods for your to be able to override the way how the SQL statements are being constructed.

## Methods

Below is the list of methods.

| Name | Description |
|:-----|:------------|
| CreateAverage | Used to create a SQL statement for the [Average](/docs/operations/average.md) operation. |
| CreateAverageAll | Used to create a SQL statement for the [AverageAll](/docs/operations/averageall.md) operation. |
| CreateBatchQuery | Used to create a SQL statement for the [BatchQuery](/docs/operations/batchquery.md) operation. |
| CreateCount | Used to create a SQL statement for the [Count](/docs/operations/count.md) operation. |
| CreateCountAll | Used to create a SQL statement for the [Average](/docs/operations/countall.md) operation. |
| CreateDelete | Used to create a SQL statement for the [Delete](/docs/operations/delete.md) operation. |
| CreateDeleteAll | Used to create a SQL statement for the [DeleteAll](/docs/operations/deleteall.md) operation. |
| CreateExists | Used to create a SQL statement for the [Exists](/docs/operations/exists.md) operation. |
| CreateInsert | Used to create a SQL statement for the [Insert](/docs/operations/insert.md) operation. |
| CreateInsertAll | Used to create a SQL statement for the [InsertAll](/docs/operations/insertall.md) operation. |
| CreateMax | Used to create a SQL statement for the [Max](/docs/operations/max.md) operation. |
| CreateMaxAll | Used to create a SQL statement for the [MaxAll](/docs/operations/maxall.md) operation. |
| CreateMerge | Used to create a SQL statement for the [Merge](/docs/operations/merge.md) operation. |
| CreateMergeAll | Used to create a SQL statement for the [MergeAll](/docs/operations/mergeall.md) operation. |
| CreateMin | Used to create a SQL statement for the [Min](/docs/operations/min.md) operation. |
| CreateMinAll | Used to create a SQL statement for the [MinAll](/docs/operations/minall.md) operation. |
| CreateQuery | Used to create a SQL statement for the [Query](/docs/operations/query.md) operation. |
| CreateQueryAll | Used to create a SQL statement for the [QueryAll](/docs/operations/queryall.md) operation. |
| CreateSum | Used to create a SQL statement for the [Sum](/docs/operations/sum.md) operation. |
| CreateSumAll | Used to create a SQL statement for the [SumAll](/docs/operations/sumall.md) operation. |
| CreateTruncate | Used to create a SQL statement for the [Truncate](/docs/operations/truncate.md) operation. |
| CreateUpdate | Used to create a SQL statement for the [Update](/docs/operations/update.md) operation. |
| CreateUpdateAll | Used to create a SQL statement for the [UpdateAll](/docs/operations/updateall.md) operation. |

## Use-Cases

This is very useful if you wish to override the existing statement builder of the library, or wish to support the other RDBMS database providers.

Please visit the [Statement Builder](/extensibility/statementbuilder) to learn more about the statement builder.

## How to Implement?

You have to manually create a class that implements this interface.

```csharp
public class OptimizedSqlServerStatementBuilder : IStatementBuilder
{
    private IDbSetting _dbSetting = new SqlServerDbSetting();

    public string CreateAverage(QueryBuilder queryBuilder,
        string tableName,
        Field field,
        QueryGroup where = null,
        string hints = null)
    {
        // Initialize the builder
        var builder = queryBuilder ?? new QueryBuilder();

        // Build the query
        builder.Clear()
            .Select()
            .Average(field, DbSetting, ConvertFieldResolver)
            .WriteText($"AS {"AverageValue".AsQuoted(DbSetting)}")
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .End();

        // Return the query
        return builder.GetString();
    }

    ...

    public string CreateQuery(QueryBuilder queryBuilder,
        string tableName,
        IEnumerable<Field> fields,
        QueryGroup where = null,
        IEnumerable<OrderField> orderBy = null,
        int? top = null,
        string hints = null)
    {
        // Initialize the builder
        var builder = queryBuilder ?? new QueryBuilder();

        // Build the query
        builder.Clear()
            .Select()
            .TopFrom(top)
            .FieldsFrom(fields, DbSetting)
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .OrderByFrom(orderBy, DbSetting)
            .End();

        // Return the query
        return builder.GetString();
    }

    ...
}
```

## Usability

You can instantiate a new instance and pass it when you are calling any [fluent methods](/links/fluent-methods).

```csharp
var statementBuilder = new OptimizedSqlServerStatementBuilder();
using (var connection = new SqlConnection(connectionString))
{
    var people = connection.QueryAll<Person>(statementBuilder: statementBuilder);
}
```

Or, you can pass it on the constructor of [BaseRepository](/class/baserepository) or [DbRepository](/class/dbrepository).

```csharp
// Repository class implementation
public class PersonRepository : BaseRepository<Person, SqlConnection>
{
    public PersonRepository(string connectionString)
        : base(connectionString, new OptimizedSqlServerStatementBuilder())
    { }
}

// Repository class usability
using (var repository = new PersonRepository(connectionString))
{
    var people = connection.QueryAll();
}
```

Or, you can use the [StatementBuilderMapper](/mapper/statementbuildermapper) class to map it with specific RDBMS data provider.

```csharp
StatementBuilderMapper.Map(typeof(SqlConnection), new OptimizedSqlServerStatementBuilder(), true);
```
> By using the [StatementBuilderMapper](/mapper/statementbuildermapper), the library will automatically use the mapped statement builder when calling the `DbConnection`, [BaseRepository](/class/baserepository) or [DbRepository](/class/dbrepository) methods. It will vary on the type of the `DbConnection` object you used.