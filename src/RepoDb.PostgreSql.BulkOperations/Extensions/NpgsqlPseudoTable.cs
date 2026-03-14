using Npgsql;
using RepoDb.Enumerations.PostgreSql;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.PostgreSql.BulkOperations;

namespace RepoDb;

public static partial class NpgsqlConnectionExtension
{
    private static void CreatePseudoTable(NpgsqlConnection connection,
        string tableName,
        string pseudoTableName,
        IEnumerable<NpgsqlBulkInsertMapItem> mappings,
        int bulkCopyTimeout = 0,
        BulkImportIdentityBehavior identityBehavior = default,
        BulkImportPseudoTableType pseudoTableType = default,
        IDbSetting? dbSetting = null,
        NpgsqlTransaction? transaction = null)
    {
        var commandText = pseudoTableType == BulkImportPseudoTableType.Physical ?
            GetCreatePseudoTableCommandText(tableName, pseudoTableName, mappings, identityBehavior, dbSetting) :
            GetCreatePseudoTemporaryTableCommandText(tableName, pseudoTableName, mappings, identityBehavior, dbSetting);

        connection.ExecuteNonQuery(commandText,
            bulkCopyTimeout,
            transaction: transaction);
    }

    private static async Task CreatePseudoTableAsync(NpgsqlConnection connection,
        string tableName,
        string pseudoTableName,
        IEnumerable<NpgsqlBulkInsertMapItem> mappings,
        int bulkCopyTimeout = 0,
        BulkImportIdentityBehavior identityBehavior = default,
        BulkImportPseudoTableType pseudoTableType = default,
        IDbSetting? dbSetting = null,
        NpgsqlTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        var commandText = pseudoTableType == BulkImportPseudoTableType.Physical ?
            GetCreatePseudoTableCommandText(tableName, pseudoTableName, mappings, identityBehavior, dbSetting) :
            GetCreatePseudoTemporaryTableCommandText(tableName, pseudoTableName, mappings, identityBehavior, dbSetting);

        await connection.ExecuteNonQueryAsync(commandText,
            bulkCopyTimeout,
            transaction: transaction,
            cancellationToken: cancellationToken);
    }

    private static IEnumerable<IdentityResult> MergeToPseudoTableWithIdentityResults(NpgsqlConnection connection,
        Func<DbField, string> getMergeToPseudoCommandText,
        DbField identityField,
        int bulkCopyTimeout = 0,
        NpgsqlTransaction? transaction = null)
    {
        var commandText = getMergeToPseudoCommandText(identityField);

        return connection.ExecuteQuery<IdentityResult>(commandText,
            bulkCopyTimeout,
            transaction: transaction);
    }

    private static async Task<IEnumerable<IdentityResult>> MergeToPseudoTableWithIdentityResultsAsync(NpgsqlConnection connection,
        Func<DbField, string> getMergeToPseudoCommandText,
        DbField identityField,
        int bulkCopyTimeout = 0,
        NpgsqlTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        var commandText = getMergeToPseudoCommandText(identityField);

        return await connection.ExecuteQueryAsync<IdentityResult>(commandText,
            bulkCopyTimeout,
            transaction: transaction,
            cancellationToken: cancellationToken);
    }

    private static void DropPseudoTable(NpgsqlConnection connection,
        string tableName,
        int bulkCopyTimeout = 0,
        NpgsqlTransaction? transaction = null)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            return;
        }

        var dbSetting = connection.GetDbSetting();
        var commandText = GetDropPseudoTemporaryTableCommandText(tableName, dbSetting);

        connection.ExecuteNonQuery(commandText,
            bulkCopyTimeout,
            transaction: transaction);
    }

    private static async Task DropPseudoTableAsync(NpgsqlConnection connection,
        string tableName,
        int bulkCopyTimeout = 0,
        NpgsqlTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            return;
        }

        var dbSetting = connection.GetDbSetting();
        var commandText = GetDropPseudoTemporaryTableCommandText(tableName, dbSetting);

        await connection.ExecuteNonQueryAsync(commandText,
            bulkCopyTimeout,
            transaction: transaction,
            cancellationToken: cancellationToken);
    }

    private static void CreatePseudoTableIndex(NpgsqlConnection connection,
        string tableName,
        IEnumerable<Field>? fields,
        int bulkCopyTimeout = 0,
        IDbSetting? dbSetting = null,
        NpgsqlTransaction? transaction = null)
    {
        if (fields?.Any() != true)
        {
            return;
        }

        var commandText = GetCreatePseudoTableIndexCommandText(tableName, fields, dbSetting);

        connection.ExecuteNonQuery(commandText,
            bulkCopyTimeout,
            transaction: transaction);
    }

    private static async Task CreatePseudoTableIndexAsync(NpgsqlConnection connection,
        string tableName,
        IEnumerable<Field>? fields,
        int bulkCopyTimeout = 0,
        IDbSetting? dbSetting = null,
        NpgsqlTransaction? transaction = null,
        CancellationToken cancellationToken = default)
    {
        if (fields?.Any() != true)
        {
            return;
        }

        var commandText = GetCreatePseudoTableIndexCommandText(tableName, fields, dbSetting);

        await connection.ExecuteNonQueryAsync(commandText,
            bulkCopyTimeout,
            transaction: transaction,
            cancellationToken: cancellationToken);
    }

    private static string GetCreatePseudoTableIndexCommandText(string tableName,
        IEnumerable<Field> fields,
        IDbSetting? dbSetting)
    {
        var indexName = $"{tableName}_{fields.Select(field => field.FieldName).Join("")}_IDX".AsQuoted(true, dbSetting);
        var columns = fields.Select(field => field.FieldName.AsQuoted(true, dbSetting)).Join(", ");

        return $"CREATE INDEX IF NOT EXISTS {indexName} " +
            $"ON {tableName.AsQuoted(true, dbSetting)} ({columns}); ";
    }

    private static string GetCreatePseudoTableCommandText(string tableName,
        string pseudoTableName,
        IEnumerable<NpgsqlBulkInsertMapItem> mappings,
        BulkImportIdentityBehavior identityBehavior,
        IDbSetting? dbSetting) =>
        $"SELECT {GetCreatePseudoTableQueryColumns(mappings, identityBehavior, dbSetting)} " +
        $"INTO {pseudoTableName.AsQuoted(true, dbSetting)} " +
        $"FROM {tableName.AsQuoted(true, dbSetting)} " +
        $"WHERE (1 = 0);";

    private static string GetCreatePseudoTemporaryTableCommandText(string tableName,
        string pseudoTableName,
        IEnumerable<NpgsqlBulkInsertMapItem> mappings,
        BulkImportIdentityBehavior identityBehavior,
        IDbSetting? dbSetting) =>
        $"SELECT {GetCreatePseudoTableQueryColumns(mappings, identityBehavior, dbSetting)} " +
        $"INTO TEMPORARY {pseudoTableName.AsQuoted(true, dbSetting)} " +
        $"FROM {tableName.AsQuoted(true, dbSetting)} " +
        $"WHERE (1 = 0);";

    private static string GetCreatePseudoTableQueryColumns(IEnumerable<NpgsqlBulkInsertMapItem> mappings,
        BulkImportIdentityBehavior identityBehavior,
        IDbSetting? dbSetting) =>
        identityBehavior != BulkImportIdentityBehavior.ReturnIdentity ?
            mappings.Select(field => field.DestinationColumn.AsQuoted(true, dbSetting)).Join(", ") :
            $"0 AS {"__RepoDb_OrderColumn".AsQuoted(dbSetting)}, " +
                $"{mappings.Select(field => field.DestinationColumn.AsQuoted(true, dbSetting)).Join(", ")}";

    private static string GetDropPseudoTemporaryTableCommandText(string tableName,
        IDbSetting? dbSetting) =>
        $"DROP TABLE IF EXISTS {tableName.AsQuoted(true, dbSetting)};";
}
