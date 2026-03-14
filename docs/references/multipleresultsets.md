
# Multiple Resultsets

---

This page contains the recommended way of querying the records with multiple resultsets.

## Single Parent with Multiple Children

For raw-SQL, call the [ExecuteQueryMultiple](/docs/operations/executequerymultiple.md) method.

```csharp
using (var connection = new SqlConnection(connectionString))
{
    using (var extractor = connection.ExecuteQueryMultiple(@"SELECT [Id], [Name] FROM [dbo].[Customer] WITH (NOLOCK) WHERE [Id] = @CustomerId;
        SELECT [Id], [CustomerId], [ProductId], [Price], [Quantity], [OrderDateUtc] FROM [dbo].[Order] WITH (NOLOCK) WHERE [CustomerId] = @CustomerId",
        new { CustomerId = 10045 }))
    {
        var customer = extractor.Extract<Customer>().FirstOrDefault();
        customer.Orders = extractor.Extract<Order>().AsList();
        // Process the 'customer' here
    }
}
```

For fluent-method, call the [QueryMultiple](/docs/operations/querymultiple.md) method.

```csharp
using (var connection = new SqlConnection(connectionString))
{
    var customerId = 10045;
    var tuple = connection.QueryMultiple<Customer, Order>(c => c.Id == customerId,
        o => o.CustomerId == customerId,
        hints1: SqlServerTableHints.NoLock,
        hints2: SqlServerTableHints.NoLock);
    var customer = tuple.Item1.FirstOrDefault();
    customer.Orders = tuple.Item2.AsList();
    // Process the 'customer' here
}
```

## Multiple Parent with Multiple Children

For raw-SQL, call the [ExecuteQueryMultiple](/docs/operations/executequerymultiple.md) method.

```csharp
using (var connection = new SqlConnection(connectionString))
{
    using (var extractor = connection.ExecuteQueryMultiple(@"SELECT [Id], [Name] FROM [dbo].[Customer] WITH (NOLOCK) WHERE [Id] IN (@Keys);
        SELECT [Id], [CustomerId], [ProductId], [Price], [Quantity], [OrderDateUtc] FROM [dbo].[Order] WITH (NOLOCK) WHERE [CustomerId] IN (@Keys);",
        new { Keys = new [] { 10045, ..., 11211 }))
    {
        var customers = extractor.Extract<Customer>().AsList();
        var orders = extractor.Extract<Order>();

        // Iterate the customers and map all the orders
        customers.ForEach(
            c => c.Orders = orders.Where(o => o.CustomerId == c.Id).AsList());

        // Process the 'customers' here
    }
}
```

For fluent-method, call the [QueryMultiple](/docs/operations/querymultiple.md) method.

```csharp
using (var connection = new SqlConnection(connectionString))
{
    var keys = new [] { 10045, ..., 11211 };
    var tuple = connection.QueryMultiple<Customer, Order>(c => keys.Contains(c.Id),
        o => keys.Contains(o.CustomerId),
        hints1: SqlServerTableHints.NoLock,
        hints2: SqlServerTableHints.NoLock);
    var customers = tuple.Item1.AsList();

    // Iterate the customers and map all the orders
    customers.AsList().ForEach(
        c => c.Orders = tuple.Item2.Where(o => o.CustomerId == c.Id).AsList());

    // Process the 'customers' here
}
```

> You can also visit our [Multiple Query](/docs/features/multiplequery.md) feature to get more information.

## Key Take-aways

- The method [QueryMultiple](/docs/operations/querymultiple.md) is pre-extracting all the resultsets, whereas [ExecuteQueryMultiple](/docs/operations/executequerymultiple.md) is extracting as per call-basis (via `Extract()` or `Scalar()` method).
- Ensure to always wrap the calls to [ExecuteQueryMultiple](/docs/operations/executequerymultiple.md) with `using` keyword.
- You can have a lot of controls with [ExecuteQueryMultiple](/docs/operations/executequerymultiple.md) over [QueryMultiple](/docs/operations/querymultiple.md) method.