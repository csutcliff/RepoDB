using System.Data;
using RepoDb.Interfaces;

namespace RepoDb;

/// <summary>
/// Contains the extension methods for <see cref="IDbConnection"/> object.
/// </summary>
public static partial class DbConnectionExtension
{
    #region CountAll<TEntity>

    /// <summary>
    /// Count the number of rows from the table.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An integer value that holds the number of rows from the table.</returns>
    public static long CountAll<TEntity>(this IDbConnection connection,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.CountAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
        where TEntity : class
    {
        return CountInternal<TEntity>(connection: connection,
            where: null,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }
    #endregion

    #region CountAllAsync<TEntity>

    /// <summary>
    /// Count the number of rows from the table in an asynchronous way.
    /// </summary>
    /// <typeparam name="TEntity">The type of the data entity.</typeparam>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An integer value that holds the number of rows from the table.</returns>
    public static async Task<long> CountAllAsync<TEntity>(this IDbConnection connection,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.CountAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return await CountInternalAsync<TEntity>(connection: connection,
            where: null,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    #endregion

    #region CountAll(TableName)

    /// <summary>
    /// Count the number of rows from the table.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <returns>An integer value that holds the number of rows from the table.</returns>
    public static long CountAll(this IDbConnection connection,
        string tableName,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.CountAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null)
    {
        return CountInternal(connection: connection,
            tableName: tableName,
            where: null,
            hints: hints,
            commandTimeout: commandTimeout,
            traceKey: traceKey,
            transaction: transaction,
            trace: trace,
            statementBuilder: statementBuilder);
    }

    #endregion

    #region CountAllAsync(TableName)

    /// <summary>
    /// Count the number of rows from the table in an asynchronous way.
    /// </summary>
    /// <param name="connection">The connection object to be used.</param>
    /// <param name="tableName">The name of the target table to be used.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <param name="traceKey">The tracing key to be used.</param>
    /// <param name="commandTimeout">The command timeout in seconds to be used.</param>
    /// <param name="transaction">The transaction to be used.</param>
    /// <param name="trace">The trace object to be used.</param>
    /// <param name="statementBuilder">The statement builder object to be used.</param>
    /// <param name="cancellationToken">The <see cref="CancellationToken"/> object to be used during the asynchronous operation.</param>
    /// <returns>An integer value that holds the number of rows from the table.</returns>
    public static async Task<long> CountAllAsync(this IDbConnection connection,
        string tableName,
        string? hints = null,
        int commandTimeout = 0,
        string? traceKey = TraceKeys.CountAll,
        IDbTransaction? transaction = null,
        ITrace? trace = null,
        IStatementBuilder? statementBuilder = null,
        CancellationToken cancellationToken = default)
    {
        return await CountInternalAsync(connection: connection,
            tableName: tableName,
            where: null,
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
