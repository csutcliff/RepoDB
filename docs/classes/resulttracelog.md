
# ResultTraceLog

---

A trace-logging class that holds the reuslt of the trace operation. It derives from [TraceLog](/class/tracelog) class.

## Properties

Below is the list of properties.

| Name | Description |
|:-----|:------------|
| ExecutionTime | Handles the total elapsed time of the actual execution. |
| Result | Handles the actual execution result. |
| CancellableTraceLog | handles the actual reference to the associated [CancellableTracelog](/class/cancellabletracelog) object. |

## Implementation

Let us say you have a custom trace class named `MyCustomTrace`. Then, you pass this object when you call the [Insert](/docs/operations/insert.md) operation.

```csharp
using (var connection = new SqlConnection(connectionString))
{
    connection.Insert<Person>(person, trace: new MyCustomTrace());
}
```

You can set a breakpoint at your `MyCustomTrace.AfterExecution()` and identify the actual results.

```csharp
public void AfterExecution<TResult>(ResultTraceLog<TResult> log)
{
    Console.WriteLine($"AfterExecution: {log.Statement}, TotalTime: {log.ExecutionTime.TotalSeconds} second(s)");
}
```
> By setting the `throwException` argument to `true`, an exception will be thrown back to the actual operation.
