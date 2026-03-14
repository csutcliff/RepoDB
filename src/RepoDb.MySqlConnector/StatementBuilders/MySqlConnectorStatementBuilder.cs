#if MYSQLPLAIN
using MySql.Data.MySqlClient;
#else
using System.Reflection;
using MySqlConnector;
#endif
using RepoDb.Exceptions;
using RepoDb.Extensions;
using RepoDb.Interfaces;

namespace RepoDb.StatementBuilders;

/// <summary>
/// A class that is being used to build a SQL Statement for MySql.
/// </summary>
#if MYSQLPLAIN
public sealed class MySqlStatementBuilder : BaseStatementBuilder
#else
public sealed class MySqlConnectorStatementBuilder : BaseStatementBuilder
#endif
{
#if MYSQLPLAIN
    public MySqlStatementBuilder()
        : this(DbSettingMapper.Get<MySqlConnection>()!,
              null,
              null)
    { }

    /// <summary>
    /// Creates a new instance of <see cref="MySqlStatementBuilder"/> class.
    /// </summary>
    /// <param name="dbSetting">The database settings object currently in used.</param>
    /// <param name="convertFieldResolver">The resolver used when converting a field in the database layer.</param>
    /// <param name="averageableClientTypeResolver">The resolver used to identity the type for average.</param>
    public MySqlStatementBuilder(IDbSetting dbSetting,
        IResolver<Field, IDbSetting, string?>? convertFieldResolver = null,
        IResolver<Type, Type?>? averageableClientTypeResolver = null)
        : base(dbSetting,
              convertFieldResolver,
              averageableClientTypeResolver)
    { }
#else
    /// <summary>
    /// Creates a new instance of <see cref="MySqlStatementBuilder"/> object.
    /// </summary>
    public MySqlConnectorStatementBuilder()
        : this(DbSettingMapper.Get<MySqlConnection>()!,
              null,
              null)
    { }

    /// <summary>
    /// Creates a new instance of <see cref="MySqlConnectorStatementBuilder"/> class.
    /// </summary>
    /// <param name="dbSetting">The database settings object currently in used.</param>
    /// <param name="convertFieldResolver">The resolver used when converting a field in the database layer.</param>
    /// <param name="averageableClientTypeResolver">The resolver used to identity the type for average.</param>
    public MySqlConnectorStatementBuilder(IDbSetting dbSetting,
        IResolver<Field, IDbSetting, string?>? convertFieldResolver = null,
        IResolver<Type, Type?>? averageableClientTypeResolver = null)
        : base(dbSetting,
              convertFieldResolver,
              averageableClientTypeResolver)
    { }
#endif

    #region CreateExists

    /// <summary>
    /// Creates a SQL Statement for exists operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for exists operation.</returns>
    public override string CreateExists(string tableName,
        QueryGroup? where = null,
        string? hints = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        // Validate the hints
        GuardHints(hints);

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder
            .Select()
            .WriteText("1")
            .As("ExistsValue", DbSetting)
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .Limit(1)
            .End(DbSetting);

        // Return the query
        return builder.ToString();
    }

    #endregion

    #region CreateInsert

    /// <summary>
    /// Creates a SQL Statement for insert operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be inserted.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for insert operation.</returns>
    public override string CreateInsert(string tableName,
        IEnumerable<Field> fields,
        IEnumerable<DbField> keyFields,
        string? hints = null)
    {
        // Initialize the builder
        var builder = new QueryBuilder();

        // Call the base
        builder.WriteText(
            base.CreateInsert(tableName,
                fields,
                keyFields,
                hints));

        if (keyFields.Any())
        {
            builder
                .Select();

            bool firstField = true;
            foreach (var kf in keyFields)
            {
                if (firstField)
                    firstField = false;
                else
                    builder.Comma();

                if (kf.IsIdentity)
                {
                    builder
                        .WriteText("LAST_INSERT_ID()");
                }
                else
                {
                    builder
                        .WriteText(kf.FieldName.AsParameter(DbSetting));
                }

                builder.As(kf.FieldName, DbSetting);
            }
            builder
                .End(DbSetting);
        }

        // Return the query
        return builder.ToString();
    }

    #endregion

    #region CreateInsertAll

    /// <summary>
    /// Creates a SQL Statement for insert-all operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be inserted.</param>
    /// <param name="batchSize">The batch size of the operation.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for insert operation.</returns>
    public override string CreateInsertAll(string tableName,
        IEnumerable<Field>? fields,
        int batchSize,
        IEnumerable<DbField> keyFields,
        string? hints = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        GuardHints(hints);

        // Validate the multiple statement execution
        ValidateMultipleStatementExecution(batchSize);

        // Verify the fields
        if (fields?.Any() != true)
        {
            throw new EmptyException(nameof(fields), "The list of fields cannot be null or empty.");
        }

        // Insertable fields
        var insertableFields = fields
            .Where(f => keyFields.GetByFieldName(f.FieldName) is not { } x || !(x.IsGenerated || x.IsIdentity));

        // Initialize the builder
        var builder = new QueryBuilder();

        // Compose
        builder
            .Insert()
            .Into()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .OpenParen()
            .FieldsFrom(insertableFields, DbSetting)
            .CloseParen()
            .Values();

        // Iterate the indexes
        for (var index = 0; index < batchSize; index++)
        {
            if (index > 0)
                builder.Comma();

            builder
                .Row()
                .OpenParen()
                .ParametersFrom(insertableFields, index, DbSetting)
                .CloseParen();
        }

        // Close
        builder
            .End(DbSetting);

        if (keyFields.Any())
        {
            builder
                .Select()
                .WriteText("*")
                .From()
                .OpenParen()
                .Values();

            for (var index = 0; index < batchSize; index++)
            {
                if (index > 0)
                    builder.Comma();

                builder
                    .Row()
                    .OpenParen();

                bool firstField = true;
                foreach (var kf in keyFields)
                {
                    if (firstField)
                        firstField = false;
                    else
                        builder.Comma();

                    if (kf.IsIdentity)
                    {
                        builder
                            .WriteText("LAST_INSERT_ID() +")
                            .WriteText($"{index}");
                    }
                    else
                    {
                        builder
                            .WriteText(kf.FieldName.AsParameter(index, DbSetting));
                    }
                }

                builder.CloseParen();
            }

            builder
                .CloseParen()
                .As()
                .WriteText("RESULT")
                .OpenParen()
                .FieldsFrom(keyFields, DbSetting)
                .CloseParen()
                .End(DbSetting);
        }

        return builder.ToString();
    }

    #endregion

    #region CreateMerge

    /// <summary>
    /// Creates a SQL Statement for merge operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be merged.</param>
    /// <param name="qualifiers">The list of the qualifier <see cref="Field"/> objects.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for merge operation.</returns>
    public override string CreateMerge(string tableName,
        IEnumerable<Field> fields,
        IEnumerable<Field>? noUpdateFields,
        IEnumerable<DbField> keyFields,
        IEnumerable<Field> qualifiers, string? hints = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        GuardHints(hints);

        // Verify the fields
        if (fields?.Any() != true)
        {
            throw new ArgumentNullException(nameof(fields), $"The list of fields cannot be null or empty.");
        }

        // Set the qualifiers
        if (qualifiers?.Any() != true)
            qualifiers = keyFields;

        // Validate the qualifiers
        if (qualifiers?.Any() != true)
        {
            var primaryField = keyFields.FirstOrDefault(f => f.IsPrimary);

            if (primaryField is null)
            {
                throw new PrimaryFieldNotFoundException($"The is no primary field from the table '{tableName}' that can be used as qualifier.");
            }
            else
            {
                throw new InvalidQualifiersException($"There are no defined qualifier fields.");
            }
        }

        // Initialize the builder
        var builder = new QueryBuilder();

        // Remove the qualifiers from the fields
        var insertableFields = fields;
        var updatableFields = EnumerableExtension.AsList(fields.Where(f => qualifiers.GetByFieldName(f.FieldName) is null && noUpdateFields?.GetByFieldName(f.FieldName) is null && keyFields.GetByFieldName(f.FieldName) is not { IsIdentity: true }));

        bool insertingIdentity = qualifiers.Any(x => keyFields.GetByFieldName(x.FieldName) is { IsIdentity: true }) && fields.Any(f => keyFields.GetByFieldName(f.FieldName) is { IsIdentity: true });

        if (!insertingIdentity)
        {
            insertableFields = fields.Where(f => keyFields.GetByFieldName(f.FieldName) is not { IsIdentity: true }).AsFieldSet();
        }

        // Build the query
        builder
            .Insert();

        if (updatableFields.Count == 0)
            builder.Ignore();

        builder
            .Into()
            .TableNameFrom(tableName, DbSetting)
            .OpenParen()
            .FieldsFrom(insertableFields, DbSetting)
            .CloseParen()
            .Values()
            .OpenParen()
            .ParametersFrom(insertableFields, 0, DbSetting)
            .CloseParen();

        var identityField = keyFields.FirstOrDefault(x => x.IsIdentity);
        if (updatableFields.Count > 0)
        {
            builder
                .WriteText("ON DUPLICATE KEY")
                .Update();

            IdentityFieldsAndParametersFrom(builder, updatableFields, 0, identityField);
        }
        builder
            .End(DbSetting);

        if (keyFields.Any())
        {
            builder
                .Select();

            bool firstField = true;
            foreach (var kf in keyFields)
            {
                if (firstField)
                    firstField = false;
                else
                    builder.Comma();

                if (kf.IsIdentity)
                {
                    if (insertingIdentity)
                        builder.Case().When().WriteText(identityField!.FieldName.AsParameter(DbSetting)).WriteText("IS NULL").Then();

                    builder
                        .WriteText("LAST_INSERT_ID()");
                    if (insertingIdentity)
                        builder.Else().WriteText(identityField!.FieldName.AsParameter(DbSetting)).WriteText("END");
                }
                else
                {
                    builder
                        .WriteText(kf.FieldName.AsParameter(DbSetting));
                }

                builder.As(kf.FieldName, DbSetting);
            }

            builder
                .End(DbSetting);
        }

        return builder.ToString();
    }

    #endregion

    #region CreateMergeAll

    /// <summary>
    /// Creates a SQL Statement for merge-all operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be merged.</param>
    /// <param name="qualifiers">The list of the qualifier <see cref="Field"/> objects.</param>
    /// <param name="batchSize">The batch size of the operation.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for merge operation.</returns>
    public override string CreateMergeAll(string tableName,
        IEnumerable<Field> fields,
        IEnumerable<Field>? noUpdateFields,
        IEnumerable<Field> qualifiers,
        int batchSize,
        IEnumerable<DbField> keyFields, string? hints = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        GuardHints(hints);

        // Verify the fields
        if (fields?.Any() != true)
        {
            throw new ArgumentNullException(nameof(fields), $"The list of fields cannot be null or empty.");
        }

        // Set the qualifiers
        if (qualifiers?.Any() != true)
            qualifiers = keyFields;

        // Validate the qualifiers
        if (qualifiers?.Any() != true)
        {
            var primaryField = keyFields.FirstOrDefault(f => f.IsPrimary);

            if (primaryField is null)
            {
                throw new PrimaryFieldNotFoundException($"The is no primary field from the table '{tableName}' that can be used as qualifier.");
            }
            else
            {
                throw new InvalidQualifiersException($"There are no defined qualifier fields.");
            }
        }

        var builder = new QueryBuilder();

        // Remove the qualifiers from the fields
        var insertableFields = fields;
        var updatableFields = EnumerableExtension.AsList(fields.Where(f => qualifiers.GetByFieldName(f.FieldName) is null && noUpdateFields?.GetByFieldName(f.FieldName) is null && keyFields.GetByFieldName(f.FieldName) is not { IsIdentity: true }));

        bool insertingIdentity = qualifiers.Any(x => keyFields.GetByFieldName(x.FieldName) is { IsIdentity: true }) && fields.Any(f => keyFields.GetByFieldName(f.FieldName) is { IsIdentity: true });

        if (!insertingIdentity)
        {
            insertableFields = fields.Where(f => keyFields.GetByFieldName(f.FieldName) is not { IsIdentity: true }).AsFieldSet();
        }

        var identityField = keyFields.FirstOrDefault(x => x.IsIdentity);
        // Iterate the indexes
        for (var index = 0; index < batchSize; index++)
        {
            // Build the query
            builder
                .Insert()
                .Into()
                .TableNameFrom(tableName, DbSetting)
                .OpenParen()
                .FieldsFrom(insertableFields, DbSetting)
                .CloseParen()
                .Values()
                .OpenParen()
                .ParametersFrom(insertableFields, index, DbSetting)
                .CloseParen()
                .WriteText("ON DUPLICATE KEY")
                .Update();

            IdentityFieldsAndParametersFrom(builder, updatableFields, index, identityField);

            builder
                .End(DbSetting);

            if (keyFields.Any())
            {
                builder
                    .Select();
                bool first = true;

                foreach (var kf in keyFields)
                {
                    if (first)
                        first = false;
                    else
                        builder.Comma();

                    if (kf.IsIdentity)
                    {
                        if (insertingIdentity)
                            builder.Case().When().WriteText(identityField!.FieldName.AsParameter(index, DbSetting)).WriteText("IS NULL").Then();

                        builder
                            .WriteText("LAST_INSERT_ID()");
                        if (insertingIdentity)
                            builder.Else().WriteText(identityField!.FieldName.AsParameter(index, DbSetting)).WriteText("END");
                    }
                    else
                    {
                        builder.WriteText(kf.FieldName.AsParameter(index, DbSetting));
                    }
                    builder.As(kf.FieldName, DbSetting);
                }

                builder.End(DbSetting);
            }
        }

        // Return the query
        return builder.ToString();
    }

    private void IdentityFieldsAndParametersFrom(QueryBuilder builder, IEnumerable<Field> updateFields, int index, DbField? identityField)
    {
        if (identityField is null)
        {
            builder.FieldsAndParametersFrom(updateFields, index, DbSetting);
        }
        else
        {
            // We want to have the LAST_INSERT_ID, and we have to set it ourselves here

            builder.FieldFrom(identityField, DbSetting);
            builder.WriteText("= LAST_INSERT_ID(");
            builder.WriteText(identityField.FieldName.AsField(DbSetting));
            builder.CloseParen();

            var filteredFields = updateFields.Where(x => !string.Equals(x.FieldName, identityField.FieldName, StringComparison.OrdinalIgnoreCase));
            if (filteredFields.Any())
            {
                builder.Comma();
                builder.FieldsAndParametersFrom(filteredFields, index, DbSetting);
            }
        }
    }

    #endregion

    #region CreateQuery

    /// <summary>
    /// Creates a SQL Statement for query operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="orderBy">The list of fields for ordering.</param>
    /// <param name="offset"></param>
    /// <param name="top">The number of rows to be returned.</param>
    /// <returns>A sql statement for query operation.</returns>
    /// <param name="hints">The table hints to be used.</param>
    public override string CreateQuery(string tableName,
        IEnumerable<Field> fields,
        QueryGroup? where = null,
        IEnumerable<OrderField>? orderBy = null,
        int offset = 0,
        int take = 0,
        string? hints = null)
    {
        ArgumentNullException.ThrowIfNull(fields);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentOutOfRangeException.ThrowIfLessThan(offset, 0);
        ArgumentOutOfRangeException.ThrowIfLessThan(take, 0);

        // Validate the hints
        GuardHints(hints);

        // Initialize the builder
        var builder = new QueryBuilder();

        // Build the query
        builder
            .Select()
            .FieldsFrom(fields, DbSetting)
            .From()
            .TableNameFrom(tableName, DbSetting)
            .HintsFrom(hints)
            .WhereFrom(where, DbSetting)
            .OrderByFrom(orderBy, DbSetting)
            .LimitTake(take, offset)
            .End(DbSetting);

        // Return the query
        return builder.ToString();
    }

    #endregion

    public override string? JsonColumnType => base.JsonColumnType;
    public override string? VectorColumnType => "VECTOR";
    public override string IdentityDefinition => "AUTO_INCREMENT";
}
