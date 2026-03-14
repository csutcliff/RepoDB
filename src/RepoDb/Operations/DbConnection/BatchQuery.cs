using System.Data;
using System.Linq.Expressions;
using RepoDb.Interfaces;

namespace RepoDb;

/// <summary>
/// Contains the extension methods for <see cref="IDbConnection"/> object.
/// </summary>
public static partial class DbConnectionExtension
{
    #region BatchQuery<TEntity>

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="fields">The list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static IEnumerable<TEntity> BatchQuery<TEntity>(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<TEntity>(connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: (QueryGroup?)null,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The dynamic expression to be used.</param>
    /// <param name="fields">The list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static IEnumerable<TEntity> BatchQuery<TEntity>(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        object? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);

        return QueryInternal<TEntity>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static IEnumerable<TEntity> BatchQuery<TEntity>(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        Expression<Func<TEntity, bool>>? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<TEntity>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: connection.ToQueryGroup(where, transaction, tableName),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static IEnumerable<TEntity> BatchQuery<TEntity>(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        QueryField? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<TEntity>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static IEnumerable<TEntity> BatchQuery<TEntity>(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        IEnumerable<QueryField>? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<TEntity>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static IEnumerable<TEntity> BatchQuery<TEntity>(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        QueryGroup? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<TEntity>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: where,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static IEnumerable<TEntity> BatchQuery<TEntity>(this IDbConnection connection,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<TEntity>(connection,
            tableName: ClassMappedNameCache.Get<TEntity>() ?? throw new ArgumentException($"Can't map {typeof(TEntity)} to tablename"),
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: (QueryGroup?)null,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The dynamic expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static IEnumerable<TEntity> BatchQuery<TEntity>(this IDbConnection connection,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        object? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<TEntity>(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>() ?? throw new ArgumentException($"Can't map {typeof(TEntity)} to tablename"),
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static IEnumerable<TEntity> BatchQuery<TEntity>(this IDbConnection connection,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        Expression<Func<TEntity, bool>>? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<TEntity>(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>() ?? throw new ArgumentException($"Can't map {typeof(TEntity)} to tablename"),
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: connection.ToQueryGroup(where, transaction),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static IEnumerable<TEntity> BatchQuery<TEntity>(this IDbConnection connection,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        QueryField? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<TEntity>(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>() ?? throw new ArgumentException($"Can't map {typeof(TEntity)} to tablename"),
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static IEnumerable<TEntity> BatchQuery<TEntity>(this IDbConnection connection,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        IEnumerable<QueryField>? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<TEntity>(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>() ?? throw new ArgumentException($"Can't map {typeof(TEntity)} to tablename"),
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static IEnumerable<TEntity> BatchQuery<TEntity>(this IDbConnection connection,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        QueryGroup? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<TEntity>(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>() ?? throw new ArgumentException($"Can't map {typeof(TEntity)} to tablename"),
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: where,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    #endregion

    #region BatchQueryAsync<TEntity>

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="fields">The list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static async Task<IEnumerable<TEntity>> BatchQueryAsync<TEntity>(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<TEntity>(connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: (QueryGroup?)null,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The dynamic expression to be used.</param>
    /// <param name="fields">The list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static async Task<IEnumerable<TEntity>> BatchQueryAsync<TEntity>(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        object? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<TEntity>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static async Task<IEnumerable<TEntity>> BatchQueryAsync<TEntity>(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        Expression<Func<TEntity, bool>>? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<TEntity>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: connection.ToQueryGroup(where, transaction, tableName),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static async Task<IEnumerable<TEntity>> BatchQueryAsync<TEntity>(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        QueryField? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<TEntity>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static async Task<IEnumerable<TEntity>> BatchQueryAsync<TEntity>(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        IEnumerable<QueryField>? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<TEntity>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static async Task<IEnumerable<TEntity>> BatchQueryAsync<TEntity>(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        QueryGroup? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<TEntity>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: where,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static async Task<IEnumerable<TEntity>> BatchQueryAsync<TEntity>(this IDbConnection connection,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<TEntity>(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>() ?? throw new ArgumentException($"Can't map {typeof(TEntity)} to tablename"),
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: (QueryGroup?)null,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The dynamic expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static async Task<IEnumerable<TEntity>> BatchQueryAsync<TEntity>(this IDbConnection connection,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        object? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<TEntity>(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>() ?? throw new ArgumentException($"Can't map {typeof(TEntity)} to tablename"),
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static async Task<IEnumerable<TEntity>> BatchQueryAsync<TEntity>(this IDbConnection connection,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        Expression<Func<TEntity, bool>>? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<TEntity>(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>() ?? throw new ArgumentException($"Can't map {typeof(TEntity)} to tablename"),
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: connection.ToQueryGroup(where, transaction),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static async Task<IEnumerable<TEntity>> BatchQueryAsync<TEntity>(this IDbConnection connection,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        QueryField? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<TEntity>(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>() ?? throw new ArgumentException($"Can't map {typeof(TEntity)} to tablename"),
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static async Task<IEnumerable<TEntity>> BatchQueryAsync<TEntity>(this IDbConnection connection,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        IEnumerable<QueryField>? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<TEntity>(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>() ?? throw new ArgumentException($"Can't map {typeof(TEntity)} to tablename"),
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of data entity objects.</returns>
    public static async Task<IEnumerable<TEntity>> BatchQueryAsync<TEntity>(this IDbConnection connection,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        QueryGroup? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<TEntity>(connection: connection,
            tableName: ClassMappedNameCache.Get<TEntity>() ?? throw new ArgumentException($"Can't map {typeof(TEntity)} to tablename"),
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            where: where,
            fields: fields,
            orderBy: orderBy,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    #endregion

    #region BatchQuery(TableName)

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of dynamic objects.</returns>
    public static IEnumerable<dynamic> BatchQuery(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<dynamic>(connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            fields: fields,
            where: (QueryGroup?)null,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The dynamic expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of dynamic objects.</returns>
    public static IEnumerable<dynamic> BatchQuery(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        object? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<dynamic>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of dynamic objects.</returns>
    public static IEnumerable<dynamic> BatchQuery(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        QueryField? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<dynamic>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of dynamic objects.</returns>
    public static IEnumerable<dynamic> BatchQuery(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        IEnumerable<QueryField>? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<dynamic>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    /// <summary>
    /// Query the rows from the database by batch.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An enumerable list of dynamic objects.</returns>
    public static IEnumerable<dynamic> BatchQuery(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        QueryGroup? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return QueryInternal<dynamic>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: where,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    #endregion

    #region BatchQueryAsync(TableName)

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of dynamic objects.</returns>
    public static async Task<IEnumerable<dynamic>> BatchQueryAsync(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<dynamic>(connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            fields: fields,
            where: (QueryGroup?)null,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The dynamic expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of dynamic objects.</returns>
    public static async Task<IEnumerable<dynamic>> BatchQueryAsync(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        object? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);

        return await QueryInternalAsync<dynamic>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of dynamic objects.</returns>
    public static async Task<IEnumerable<dynamic>> BatchQueryAsync(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        QueryField? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<dynamic>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of dynamic objects.</returns>
    public static async Task<IEnumerable<dynamic>> BatchQueryAsync(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        IEnumerable<QueryField>? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<dynamic>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: ToQueryGroup(where),
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Query the rows from the database by batch in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="page">The page of the batch to be used. This is a zero-based index (the first page is 0).</param>
    /// <param name="rowsPerBatch">The number of data per batch to be returned.</param>
    /// <param name="orderBy">The order definition of the fields to be used.</param>
    /// <param name="where">The query expression to be used.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An enumerable list of dynamic objects.</returns>
    public static async Task<IEnumerable<dynamic>> BatchQueryAsync(this IDbConnection connection,
        string tableName,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy,
        QueryGroup? where = null,
        IEnumerable<Field>? fields = null,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.BatchQuery,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 0);
        return await QueryInternalAsync<dynamic>(connection: connection,
            tableName: tableName,
            offset: page * rowsPerBatch,
            top: rowsPerBatch > 0 ? rowsPerBatch : 0,
            orderBy: orderBy,
            where: where,
            fields: fields,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    #endregion
}
