
# Order

---

This enum is used to define an ordering of the query results. It is usually used in [Query](/docs/operations/query.md), [QueryAll](/docs/operations/queryall.md) and [BatchQuery](/docs/operations/batchquery.md) operations.

### Enum Values

Below is the list of enum values.

| Name | Description |
|:-----|:------------|
| Ascending | Is used to define an ascending order. This is the default setting. |
| Descending | Is used to define a descending order. |

### Usability

Simply use the value when creating an [OrderField](/class/orderfield) object.

```csharp
var orderBy = new OrderField("LastName", Order.Ascending);
```

Or, when parsing a dynamic or class object via [OrderField](/class/orderfield) object.

```csharp
var orderBy = OrderField.Parse(new
{
    LastName = Order.Ascending,
    FirstName = Order.Descending
});
```

Or via `Ascending` or `Descending` method of the [OrderField](/class/orderfield) object.

```csharp
var orderBy = OrderField.Descending<Person>(p => p.LastName);
```